use anyhow::{Context, Result};
use clap::Parser;
use std::{
    fs,
    path::{Path, PathBuf},
};
use tangent_shared::{Config, Consumer};

mod msk;
mod socket;
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
            args.connections,
            args.max_bytes,
            args.seconds,
            payload,
        )
        .await?
    }

    Ok(())
}

async fn run_bench(
    cfg: &Config,
    connections: u16,
    max_bytes: usize,
    seconds: u64,
    payload: PathBuf,
) -> Result<()> {
    for consumer in &cfg.consumers {
        let pd = payload.clone();
        match consumer {
            (name, Consumer::Socket(sc)) => {
                socket::run_bench(sc.socket_path.clone(), connections, pd, max_bytes, seconds)
                    .await?;
            }
            (name, Consumer::MSK(mc)) => {
                msk::run_bench(mc, connections, pd, max_bytes, seconds).await?;
            }
        }
    }

    Ok(())
}
