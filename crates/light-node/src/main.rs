use anyhow::{bail, Result};
use clap::Parser;
use std::{path::PathBuf, sync::Arc};
use tangent_shared::{Config, SinkConfig, SourceConfig};
use tokio::signal;
use tokio::time::{timeout, Duration};
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_subscriber::EnvFilter;

use prometheus::{
    register_histogram, register_int_counter, register_int_gauge, Histogram, IntCounter, IntGauge,
};
use prometheus_exporter;

use crate::sinks::manager::SinkManager;

mod sinks;
mod sources;
mod wasm;
mod worker;

lazy_static::lazy_static! {
    // Existing
    static ref BATCH_LATENCY: Histogram = register_histogram!(
        "tangent_batch_seconds", "Batch call latency (sec)",
        vec![5e-5,1e-4,2e-4,4e-4,8e-4,1.6e-3,3.2e-3,6.4e-3,1.28e-2,2.56e-2,5.12e-2,0.102,0.204,0.409,0.819,1.638]
    ).unwrap();

    static ref BATCH_EVENTS: IntCounter = register_int_counter!(
        "tangent_batch_objects_total", "Objects processed in batches"
    ).unwrap();

    static ref CONSUMER_BYTES_TOTAL: IntCounter = register_int_counter!(
        "tangent_consumer_bytes_total", "Bytes consumed (raw input)"
    ).unwrap();

    static ref CONSUMER_OBJECTS_TOTAL: IntCounter = register_int_counter!(
        "tangent_consumer_objects_total", "Objects consumed (raw input)"
    ).unwrap();

    // counts when a WAL file is sealed (ready for upload)
    static ref WAL_SEALED_BYTES_TOTAL: IntCounter = register_int_counter!(
        "tangent_wal_sealed_bytes_total", "Bytes sealed to WAL files"
    ).unwrap();
    static ref WAL_SEALED_FILES_TOTAL: IntCounter = register_int_counter!(
        "tangent_wal_sealed_files_total", "WAL files sealed"
    ).unwrap();

    static ref SINK_BYTES_TOTAL: IntCounter = register_int_counter!(
        "tangent_sink_bytes_total", "Bytes uploaded to sink"
    ).unwrap();
    static ref SINK_OBJECTS_TOTAL: IntCounter = register_int_counter!(
        "tangent_sink_objects_total", "Objects sent to sink"
    ).unwrap();

    // Gauges to see draining state
    static ref INFLIGHT: IntGauge = register_int_gauge!(
        "tangent_inflight", "Batches enqueued but not yet persisted"
    ).unwrap();
    static ref WAL_PENDING_FILES: IntGauge = register_int_gauge!(
        "tangent_wal_pending_files", "Sealed WAL files pending upload"
    ).unwrap();
    static ref WAL_PENDING_BYTES: IntGauge = register_int_gauge!(
        "tangent_wal_pending_bytes", "Approx bytes pending in sealed WAL files"
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

    if cfg.sinks.is_empty() {
        bail!("At least one sink is required.");
    }

    if cfg.sinks.len() > 1 {
        bail!("You must configure exactly one sink.");
    }

    let name: &String;
    let sink_cfg: &SinkConfig;
    if let Some((n, s)) = cfg.sinks.first_key_value() {
        name = n;
        sink_cfg = s;
    } else {
        bail!("Sink name or config was missing from config.");
    }

    // Allow queueing 10 batches for now. Should use a WAL in the future.
    let sink_manager = Arc::new(SinkManager::new(name, sink_cfg, cfg.batch_size_kb() * 10).await?);

    let engine = wasm::WasmEngine::new().expect("engine");

    info!(
        "Batch size: {} KiB, max age: {:?}",
        cfg.batch_size,
        cfg.batch_age_ms()
    );

    let shutdown = CancellationToken::new();

    let pool = Arc::new(
        worker::WorkerPool::new(
            cfg.workers,
            engine,
            Arc::clone(&sink_manager),
            cfg.batch_size_kb(),
            cfg.batch_age_ms(),
        )
        .await?,
    );

    let consumer_handles = run_all_consumers(cfg, pool.clone(), shutdown.clone()).await;

    signal::ctrl_c().await?;
    shutdown.cancel();

    info!("received shutdown signal...");

    info!("waiting on consumers to shutdown...");
    for h in consumer_handles {
        let _ = tokio::time::timeout(Duration::from_secs(10), h).await;
    }

    let mut pool_owned = Arc::try_unwrap(pool)
        .map_err(|_| anyhow::anyhow!("pool still has refs; drop all clones before shutdown"))?;

    pool_owned.close();

    info!("waiting on workers to shutdown...");
    let _ = timeout(Duration::from_secs(15), pool_owned.join()).await;

    let sink_manager_owned = Arc::try_unwrap(sink_manager).map_err(|_| {
        anyhow::anyhow!("sink_manager still has refs; drop all clones before shutdown")
    })?;

    info!("waiting on sinks to flush and shutdown...");
    let _ = timeout(Duration::from_secs(10), sink_manager_owned.join()).await;

    Ok(())
}

async fn run_all_consumers(
    cfg: Config,
    pool: Arc<worker::WorkerPool>,
    shutdown: CancellationToken,
) -> Vec<tokio::task::JoinHandle<()>> {
    let mut handles = Vec::new();
    for src in cfg.sources {
        let pool = pool.clone();
        let shutdown = shutdown.clone();
        match src {
            (_, SourceConfig::MSK(kc)) => {
                handles.push(tokio::spawn(async move {
                    if let Err(e) = sources::msk::run_consumer(kc, pool, shutdown.clone()).await {
                        tracing::error!("msk consumer error: {e}");
                    }
                }));
            }
            (_, SourceConfig::Socket(sc)) => {
                handles.push(tokio::spawn(async move {
                    if let Err(e) = sources::socket::run_consumer(sc, pool, shutdown.clone()).await
                    {
                        tracing::error!("socket listener error: {e}");
                    }
                }));
            }
            (_, SourceConfig::SQS(sq)) => {
                handles.push(tokio::spawn(async move {
                    if let Err(e) =
                        sources::sqs::run_consumer(sq, cfg.batch_size, pool, shutdown.clone()).await
                    {
                        tracing::error!("SQS consumer error: {e}");
                    }
                }));
            }
        }
    }

    handles
}
