use anyhow::{Context, Result};
use clap::Parser;
use std::{
    fs,
    path::{Path, PathBuf},
};
use tangent_shared::{Config, SourceConfig};

mod metrics;
mod msk;
mod socket;
mod sqs;
/// NDJSON UDS load generator
#[derive(Parser, Debug)]
struct Args {
    /// Path to tangent.yaml
    #[arg(long)]
    config: PathBuf,
    /// Duration (seconds)
    #[arg(long, default_value_t = 15)]
    seconds: u64,
    /// Concurrent connections
    #[arg(long, default_value_t = 2)]
    connections: u16,
    /// Payload filepath. Leave empty to run all test payloads.
    #[arg(long)]
    payload: Option<PathBuf>,
    /// Batch-bytes cap per write (0 = disabled)
    #[arg(long, default_value_t = 65536)]
    max_bytes: usize,

    #[arg(long, default_value = "http://127.0.0.1:9184/metrics")]
    metrics_url: String,
    /// For S3 + SQS Bench.
    #[arg(long)]
    bucket: Option<String>,
    #[arg(long)]
    object_prefix: Option<String>,
}

fn crate_root() -> &'static str {
    env!("CARGO_MANIFEST_DIR")
}

fn test_data_dir() -> PathBuf {
    Path::new(crate_root()).join("test_data")
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().init();

    let args = Args::parse();
    let cfg = Config::from_file(&args.config)?;

    let mut payloads: Vec<PathBuf> = Vec::new();
    if let Some(payload) = args.payload {
        payloads.push(payload);
    } else {
        for entry in fs::read_dir(test_data_dir()).context(format!(
            "reading test data dir: {}",
            test_data_dir().display()
        ))? {
            payloads.push(entry?.path());
        }
    }

    for payload in payloads {
        run_bench(
            &cfg,
            &args.metrics_url,
            args.connections,
            args.max_bytes,
            args.seconds,
            payload,
            args.bucket.clone(),
            args.object_prefix.clone(),
        )
        .await?
    }

    Ok(())
}

async fn run_bench(
    cfg: &Config,
    metrics_url: &String,
    connections: u16,
    max_bytes: usize,
    seconds: u64,
    payload: PathBuf,
    bucket: Option<String>,
    obj_prefix: Option<String>,
) -> Result<()> {
    for src in &cfg.sources {
        let pd = payload.clone();
        let before = metrics::scrape_stats(&metrics_url).await?;

        match src {
            (name, SourceConfig::Socket(sc)) => {
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
            (name, SourceConfig::MSK(mc)) => {
                msk::run_bench(name, mc, connections, pd, max_bytes, seconds).await?;
            }
            (name, SourceConfig::SQS(sq)) => {
                if let Some(ref b) = bucket {
                    sqs::run_bench(
                        name,
                        sq,
                        b.clone(),
                        obj_prefix.clone(),
                        max_bytes,
                        connections,
                        pd,
                        seconds,
                    )
                    .await?;
                } else {
                    anyhow::bail!("--bucket is a required arg for sqs bench");
                }
            }
        }

        let after = metrics::scrape_stats(&metrics_url).await?;
        println!(
            "processed: events={}, bytes={:.2} MiB, bytes/s={:.2} MB",
            after.batch_objects as u64,
            after.sink_bytes as u64 / 1024 / 1024,
            after.sink_bytes as u64 / 1_000_000 / seconds
        );

        println!("waiting for inflight messages to finish to verify correctness...");

        let drained = metrics::wait_for_drain(&metrics_url).await?;
        let final_delta_vs_before = metrics::Stats {
            batch_objects: drained.batch_objects - before.batch_objects,
            sink_bytes: drained.sink_bytes - before.sink_bytes,
            sink_objects: drained.sink_objects - before.sink_objects,
            consumer_bytes: drained.consumer_bytes - before.consumer_bytes,
            consumer_objects: drained.consumer_objects - before.consumer_objects,
            inflight: drained.inflight,
            wal_pending: drained.wal_pending,
        };

        println!(
            "objects delta (consumer - uploaded): {}",
            (final_delta_vs_before.consumer_objects - final_delta_vs_before.sink_objects)
        );
    }

    Ok(())
}
