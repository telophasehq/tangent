use anyhow::{Context, Result};
use serde_json::Value;
use std::{fs, path::PathBuf, time::Instant};
use tangent_shared::{sources::common::SourceConfig, Config};

use crate::metrics::Stats;

pub mod metrics;
pub mod msk;
pub mod socket;
pub mod sqs;
pub mod synthesize;
pub mod tcp;

const WARMUP_SECS: u64 = 5;

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
    // Whether to use the payload as-is or synthesize new logs from the payload.
    pub synthesize: bool,
}

impl Default for BenchOptions {
    fn default() -> Self {
        Self {
            config_path: None,
            seconds: 15,
            connections: 2,
            payload: "tests/input.json".into(),
            max_bytes: 1 << 20,
            metrics_url: "http://127.0.0.1:9184/metrics".to_string(),
            bucket: None,
            object_prefix: None,
            disable_metrics: false,
            synthesize: false,
        }
    }
}

pub async fn run(config_path: &PathBuf, opts: BenchOptions) -> Result<()> {
    let mut cfg = Config::from_file(config_path)?;

    // disable remote calls in bench.
    cfg.runtime.disable_remote_calls = true;
    run_with_config(cfg, opts).await
}

pub async fn run_with_config(cfg: Config, opts: BenchOptions) -> Result<()> {
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
        opts.synthesize,
    )
    .await?;

    Ok(())
}

pub async fn run_one_payload(
    cfg: Config,
    metrics_url: &str,
    connections: u16,
    max_bytes: usize,
    seconds: u64,
    payload: Vec<u8>,
    bucket: Option<String>,
    obj_prefix: Option<String>,
    disable_metrics: bool,
    synthesize_payload: bool,
) -> Result<()> {
    for (name, src) in &cfg.sources {
        let pd = payload.clone();

        // Single-run with warmup: run bench for warmup_seconds + seconds.
        // Capture baseline metrics exactly at warmup boundary (or immediately if warmup is 0).
        let total_seconds = seconds.saturating_add(WARMUP_SECS);

        let (before_pair_res, bench_res) = {
            tracing::info!(
                "warmup: {}s, then measuring {}s for source {}",
                WARMUP_SECS,
                seconds,
                name
            );

            tokio::join!(
                async {
                    if disable_metrics {
                        return Ok::<Option<(Stats, Instant)>, anyhow::Error>(None);
                    }
                    tokio::time::sleep(std::time::Duration::from_secs(WARMUP_SECS)).await;
                    let stats = metrics::scrape_stats(metrics_url).await?;
                    Ok::<Option<(Stats, Instant)>, anyhow::Error>(Some((stats, Instant::now())))
                },
                async {
                    match src {
                        SourceConfig::Socket(sc) => {
                            socket::run_bench(
                                name.clone(),
                                sc.socket_path.clone(),
                                connections,
                                pd,
                                max_bytes,
                                total_seconds,
                                synthesize_payload,
                            )
                            .await
                        }
                        SourceConfig::MSK(mc) => {
                            msk::run_bench(
                                name.clone(),
                                mc,
                                connections,
                                pd,
                                max_bytes,
                                total_seconds,
                            )
                            .await
                        }
                        SourceConfig::SQS(sq) => {
                            if let Some(ref b) = bucket {
                                sqs::run_bench(
                                    name.clone(),
                                    sq,
                                    b.clone(),
                                    obj_prefix.clone(),
                                    connections,
                                    pd,
                                    total_seconds,
                                )
                                .await
                            } else {
                                anyhow::bail!("--bucket is required for SQS bench");
                            }
                        }
                        SourceConfig::Tcp(tc) => {
                            tcp::run_bench(
                                name.clone(),
                                tc.bind_address,
                                connections,
                                pd,
                                max_bytes,
                                seconds,
                                synthesize_payload,
                            )
                            .await
                        }
                        SourceConfig::File(_) => unimplemented!("not implemented"),
                    }
                }
            )
        };

        bench_res?;
        let t1 = Instant::now();

        if !disable_metrics {
            let (before, t0) = before_pair_res?.expect("metrics baseline missing");
            let drained = metrics::scrape_stats(metrics_url).await?;
            let elapsed = t1.duration_since(t0).as_secs_f64();

            let in_bytes = (drained.consumer_bytes - before.consumer_bytes) as f64;
            let out_bytes = (drained.sink_bytes - before.sink_bytes) as f64;
            let out_bytes_uncompressed =
                (drained.sink_bytes_uncompressed - before.sink_bytes_uncompressed) as f64;
            let amp = if in_bytes > 0.0 {
                out_bytes / in_bytes
            } else {
                0.0
            };

            let in_mbs = in_bytes / 1_000_000.0;
            let out_mbs = out_bytes / 1_000_000.0;
            let out_mbs_uncompressed = out_bytes_uncompressed / 1_000_000.0;
            let in_mbs_s = in_mbs / elapsed;
            let out_mbs_s = out_mbs / elapsed;
            let out_mbs_uncompressed_s = out_mbs_uncompressed / elapsed;

            println!(
                "end-to-end: uploaded={:.2} MB ({:.2} MB uncompressed) over {:.2}s → {:.2} MB/s ({:.2} MB/s uncompressed) (amplification x{:.5})",
                out_mbs, out_mbs_uncompressed, elapsed, out_mbs_s, out_mbs_uncompressed_s, amp
            );
            println!(
                "producer bytes (consumed): {:.2} MB → {:.2} MB/s",
                in_mbs, in_mbs_s
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
                "guest: bytes_in={:.2} MB, avg_latency={:.3} ms (over {:.0} calls)",
                guest_bytes_delta / 1_000_000.0,
                guest_avg_ms,
                guest_cnt_delta
            );
        }
    }

    Ok(())
}
