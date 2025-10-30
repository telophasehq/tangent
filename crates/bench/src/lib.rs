use anyhow::{Context, Result};
use serde_json::Value;
use std::{fs, path::PathBuf};
use tangent_shared::{sources::common::SourceConfig, Config};

use crate::metrics::Stats;

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
    /// Payload filepath.
    pub payload: PathBuf,
    /// Batch-bytes cap per write (0 = disabled)
    pub max_bytes: usize,
    /// Prometheus metrics endpoint
    pub metrics_url: String,
    /// For S3 + SQS bench
    pub bucket: Option<String>,
    pub object_prefix: Option<String>,

    // disable metrics. Only use if benchmarking outside of tangent run.
    pub disable_metrics: bool,
}

impl Default for BenchOptions {
    fn default() -> Self {
        Self {
            config_path: None,
            seconds: 15,
            connections: 2,
            payload: "tests/input.json".into(),
            max_bytes: 65_536,
            metrics_url: "http://127.0.0.1:9184/metrics".to_string(),
            bucket: None,
            object_prefix: None,
            disable_metrics: false,
        }
    }
}

pub async fn run(config_path: &PathBuf, opts: BenchOptions) -> Result<()> {
    let cfg = Config::from_file(config_path)?;
    run_with_config(&cfg, opts).await
}

pub async fn run_with_config(cfg: &Config, opts: BenchOptions) -> Result<()> {
    let payload = fs::read_to_string(&opts.payload)
        .with_context(|| format!("failed to read payload file {}", &opts.payload.display()))?;

    let json_payload = serde_json::from_str::<Value>(&payload)?;

    let mut payload_buf: Vec<u8> = Vec::new();
    match json_payload {
        Value::Array(arr) => {
            for v in arr {
                let line = serde_json::to_string(&v)?;
                payload_buf.extend_from_slice(line.as_bytes());
                payload_buf.push(b'\n');
            }
        }
        _ => {
            let line = serde_json::to_string(&json_payload)?;
            payload_buf.extend_from_slice(line.as_bytes());
            payload_buf.push(b'\n');
        }
    }

    run_one_payload(
        cfg,
        &opts.metrics_url,
        opts.connections,
        opts.max_bytes,
        opts.seconds,
        payload_buf,
        opts.bucket.clone(),
        opts.object_prefix.clone(),
        opts.disable_metrics,
    )
    .await?;

    Ok(())
}

pub async fn run_one_payload(
    cfg: &Config,
    metrics_url: &str,
    connections: u16,
    max_bytes: usize,
    seconds: u64,
    payload: Vec<u8>,
    bucket: Option<String>,
    obj_prefix: Option<String>,
    disable_metrics: bool,
) -> Result<()> {
    for src in &cfg.sources {
        let pd = payload.clone();
        let mut before: Option<Stats> = None;
        if !disable_metrics {
            before = Some(metrics::scrape_stats(metrics_url).await?);
        }
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
                        connections,
                        pd,
                        seconds,
                    )
                    .await?;
                } else {
                    anyhow::bail!("--bucket is required for SQS bench");
                }
            }
            SourceConfig::File(_) => unimplemented!("not implemented"),
        }

        if !disable_metrics {
            let elapsed = t0.elapsed().as_secs_f64();
            let before = before.unwrap();
            let drained = metrics::scrape_stats(metrics_url).await?;

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
    }

    Ok(())
}
