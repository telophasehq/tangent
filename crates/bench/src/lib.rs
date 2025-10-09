use anyhow::{Context, Result};
use std::{
    fs,
    path::{Path, PathBuf},
};
use tangent_shared::{source::SourceConfig, Config};

pub mod metrics;
pub mod msk;
pub mod socket;
pub mod sqs;

/// Options for running the benchmark.
#[derive(Debug, Clone)]
pub struct BenchOptions {
    /// Path to tangent.yaml (already parsed in `run_with_config`, here for symmetry)
    pub config_path: Option<PathBuf>,
    /// Duration (seconds)
    pub seconds: u64,
    /// Concurrent connections
    pub connections: u16,
    /// Payload filepath. If None, runs all files in `test_data/`.
    pub payload: Option<PathBuf>,
    /// Batch-bytes cap per write (0 = disabled)
    pub max_bytes: usize,
    /// Prometheus metrics endpoint
    pub metrics_url: String,
    /// For S3 + SQS bench
    pub bucket: Option<String>,
    pub object_prefix: Option<String>,
}

impl Default for BenchOptions {
    fn default() -> Self {
        Self {
            config_path: None,
            seconds: 15,
            connections: 2,
            payload: None,
            max_bytes: 65_536,
            metrics_url: "http://127.0.0.1:9184/metrics".to_string(),
            bucket: None,
            object_prefix: None,
        }
    }
}

pub async fn run(config_path: &PathBuf, opts: BenchOptions) -> Result<()> {
    let cfg = Config::from_file(config_path)?;
    run_with_config(&cfg, opts).await
}

pub async fn run_with_config(cfg: &Config, opts: BenchOptions) -> Result<()> {
    let mut payloads: Vec<PathBuf> = Vec::new();
    if let Some(payload) = &opts.payload {
        payloads.push(payload.clone());
    } else {
        for entry in fs::read_dir(test_data_dir()).context(format!(
            "reading test data dir: {}",
            test_data_dir().display()
        ))? {
            payloads.push(entry?.path());
        }
    }

    for payload in payloads {
        run_one_payload(
            cfg,
            &opts.metrics_url,
            opts.connections,
            opts.max_bytes,
            opts.seconds,
            payload,
            opts.bucket.clone(),
            opts.object_prefix.clone(),
        )
        .await?;
    }

    Ok(())
}

fn crate_root() -> &'static str {
    env!("CARGO_MANIFEST_DIR")
}

fn test_data_dir() -> PathBuf {
    Path::new(crate_root()).join("test_data")
}

pub async fn run_one_payload(
    cfg: &Config,
    metrics_url: &str,
    connections: u16,
    max_bytes: usize,
    seconds: u64,
    payload: PathBuf,
    bucket: Option<String>,
    obj_prefix: Option<String>,
) -> Result<()> {
    for src in &cfg.sources {
        let pd = payload.clone();
        let before = metrics::scrape_stats(metrics_url).await?;
        let t0 = std::time::Instant::now();

        let name = src.0;
        match &src.1 {
            SourceConfig::Socket(sc) => {
                socket::run_bench(
                    name,
                    sc.socket_path.clone(),
                    connections,
                    pd,
                    max_bytes,
                    seconds,
                )
                .await?;
            }
            SourceConfig::MSK(mc) => {
                msk::run_bench(name, &mc, connections, pd, max_bytes, seconds).await?;
            }
            SourceConfig::SQS(sq) => {
                if let Some(ref b) = bucket {
                    sqs::run_bench(
                        name,
                        &sq,
                        b.clone(),
                        obj_prefix.clone(),
                        max_bytes,
                        connections,
                        pd,
                        seconds,
                    )
                    .await?;
                } else {
                    anyhow::bail!("--bucket is required for SQS bench");
                }
            }
        }

        let drained = metrics::wait_for_drain(metrics_url).await?;
        let elapsed = t0.elapsed().as_secs_f64();

        let in_bytes = (drained.consumer_bytes - before.consumer_bytes) as f64;
        let out_bytes = (drained.sink_bytes - before.sink_bytes) as f64;
        let out_bytes_uncompressed =
            (drained.sink_bytes_uncompressed - before.sink_bytes_uncompressed) as f64;
        let amp = if in_bytes > 0.0 {
            out_bytes / in_bytes
        } else {
            0.0
        };

        let in_mibs = in_bytes / (1024.0 * 1024.0);
        let out_mibs = out_bytes / (1024.0 * 1024.0);
        let out_mibs_uncompressed = out_bytes_uncompressed / (1024.0 * 1024.0);
        let in_mibs_s = in_mibs / elapsed;
        let out_mibs_s = out_mibs / elapsed;
        let out_mibs_uncompressed_s = out_mibs_uncompressed / elapsed;

        println!(
            "end-to-end: uploaded={:.2} MiB ({:.2} MiB uncompressed) over {:.2}s → {:.2} MiB/s ({:.2} MiB/s uncompressed) (amplification x{:.5})",
            out_mibs, out_mibs_uncompressed, elapsed, out_mibs_s, out_mibs_uncompressed_s, amp
        );
        println!(
            "producer bytes (consumed): {:.2} MiB → {:.2} MiB/s",
            in_mibs, in_mibs_s
        );

        let guest_bytes_delta = drained.guest_bytes - before.guest_bytes;
        let guest_sum_delta = drained.guest_seconds_sum - before.guest_seconds_sum;
        let guest_cnt_delta = drained.guest_seconds_count - before.guest_seconds_count;

        let guest_avg_ms = if guest_cnt_delta > 0.0 {
            (guest_sum_delta / guest_cnt_delta) * 1_000.0
        } else {
            0.0
        };

        println!(
            "guest: bytes_in={:.2} MiB, avg_latency={:.3} ms (over {:.0} calls)",
            guest_bytes_delta / (1024.0 * 1024.0),
            guest_avg_ms,
            guest_cnt_delta
        );
    }

    Ok(())
}
