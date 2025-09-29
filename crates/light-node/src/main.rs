use anyhow::{bail, Result};
use clap::Parser;
use std::{path::PathBuf, sync::Arc};
use tangent_shared::{Config, Source};
use tokio::signal;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::EnvFilter;

use prometheus::{register_histogram, register_int_counter, Histogram, IntCounter};
use prometheus_exporter;

mod sources;
mod wasm;
mod worker;

lazy_static::lazy_static! {
    static ref BATCH_LATENCY: Histogram = register_histogram!(
        "tangent_batch_seconds",
        "Batch call latency (sec)",
        vec![5e-5,1e-4,2e-4,4e-4,8e-4,1.6e-3,3.2e-3,6.4e-3,1.28e-2,2.56e-2,5.12e-2,0.102,0.204,0.409,0.819,1.638],
    ).unwrap();

    static ref BATCH_EVENTS: IntCounter = register_int_counter!(
        "tangent_batch_events_total",
        "Events processed in batches",
    ).unwrap();
}

#[derive(Parser, Debug)]
#[command(version, about = "Run log processor")]

struct Args {
    /// Path to YAML config
    #[arg(long)]
    config: PathBuf,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
    let _exporter =
        prometheus_exporter::start("0.0.0.0:9184".parse().expect("failed to parse binding"))
            .expect("failed to start prometheus exporter");

    let _ = &*BATCH_LATENCY;
    let _ = &*BATCH_EVENTS;

    let args = &Args::parse();
    let cfg = Config::from_file(&args.config)?;

    if cfg.sources.is_empty() {
        bail!("At least one source is required.");
    }

    let engine = wasm::WasmEngine::new().expect("engine");

    info!(
        "Batch size: {} KiB, max age: {:?}",
        cfg.batch_size,
        cfg.batch_age_ms()
    );

    let shutdown = CancellationToken::new();

    let pool = Arc::new(
        worker::WorkerPool::new(cfg.workers, engine, cfg.batch_size_kb(), cfg.batch_age_ms())
            .await?,
    );

    run_all_consumers(cfg, pool.clone(), shutdown.clone()).await;
    signal::ctrl_c().await?;
    shutdown.cancel();

    Ok(())
}

async fn run_all_consumers(
    cfg: Config,
    pool: Arc<worker::WorkerPool>,
    shutdown: CancellationToken,
) {
    for src in cfg.sources {
        let pool = pool.clone();
        let shutdown = shutdown.clone();
        match src {
            (_, Source::MSK(kc)) => {
                tokio::spawn(async move {
                    if let Err(e) = sources::msk::run_consumer(kc, pool, shutdown.clone()).await {
                        tracing::error!("msk consumer error: {e}");
                    }
                });
            }
            (_, Source::Socket(sc)) => {
                tokio::spawn(async move {
                    if let Err(e) = sources::socket::run_consumer(sc, pool, shutdown.clone()).await
                    {
                        tracing::error!("socket listener error: {e}");
                    }
                });
            }
            (_, Source::SQS(sq)) => {
                tokio::spawn(async move {
                    if let Err(e) =
                        sources::sqs::run_consumer(sq, cfg.batch_size, pool, shutdown.clone()).await
                    {
                        tracing::error!("SQS consumer error: {e}");
                    }
                });
            }
        }
    }
}
