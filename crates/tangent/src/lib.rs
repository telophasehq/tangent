use anyhow::{anyhow, bail, Result};
use std::{net::SocketAddr, path::PathBuf, sync::Arc};
use tokio::time::{timeout, Duration};
use tokio_util::sync::CancellationToken;
use tracing::info;

use prometheus::{
    register_histogram_vec, register_int_counter, register_int_gauge, HistogramVec, IntCounter,
    IntGauge,
};

use tangent_shared::{source::SourceKind, Config, SinkConfig};

pub mod sinks;
pub mod sources;
pub mod wasm;
pub mod worker;

/// Options for running the runtime.
#[derive(Debug, Clone)]
pub struct RuntimeOptions {
    pub prometheus_bind: Option<SocketAddr>,
    pub once: bool,
}

impl Default for RuntimeOptions {
    fn default() -> Self {
        Self {
            prometheus_bind: Some("0.0.0.0:9184".parse().unwrap()),
            once: false,
        }
    }
}

lazy_static::lazy_static! {
    pub static ref GUEST_LATENCY: HistogramVec = register_histogram_vec!(
        "tangent_guest_seconds",
        "WASM guest call latency (sec)",
        &["worker"],
        vec![5e-5,1e-4,2e-4,4e-4,8e-4,1.6e-3,3.2e-3,6.4e-3,1.28e-2,2.56e-2,5.12e-2,0.102,0.204,0.409,0.819,1.638]
    ).unwrap();

    pub static ref GUEST_BYTES_TOTAL: IntCounter =
        register_int_counter!("tangent_guest_bytes_total", "Bytes fed to WASM guest").unwrap();

    pub static ref CONSUMER_BYTES_TOTAL: IntCounter =
        register_int_counter!("tangent_consumer_bytes_total", "Bytes consumed (raw input)").unwrap();

    pub static ref CONSUMER_OBJECTS_TOTAL: IntCounter =
        register_int_counter!("tangent_consumer_objects_total", "Objects consumed (raw input)").unwrap();

    pub static ref WAL_SEALED_BYTES_TOTAL: IntCounter =
        register_int_counter!("tangent_wal_sealed_bytes_total", "Bytes sealed to WAL files").unwrap();

    pub static ref WAL_SEALED_FILES_TOTAL: IntCounter =
        register_int_counter!("tangent_wal_sealed_files_total", "WAL files sealed").unwrap();

    pub static ref SINK_BYTES_TOTAL: IntCounter =
        register_int_counter!("tangent_sink_bytes_total", "Bytes uploaded to sink").unwrap();

    pub static ref SINK_BYTES_UNCOMPRESSED_TOTAL: IntCounter =
        register_int_counter!("tangent_sink_bytes_uncompressed_total", "Bytes (uncompressed) uploaded to sink").unwrap();

    pub static ref SINK_OBJECTS_TOTAL: IntCounter =
        register_int_counter!("tangent_sink_objects_total", "Objects sent to sink").unwrap();

    pub static ref INFLIGHT: IntGauge =
        register_int_gauge!("tangent_inflight", "Batches enqueued but not yet persisted").unwrap();

    pub static ref WAL_PENDING_FILES: IntGauge =
        register_int_gauge!("tangent_wal_pending_files", "Sealed WAL files pending upload").unwrap();

    pub static ref WAL_PENDING_BYTES: IntGauge =
        register_int_gauge!("tangent_wal_pending_bytes", "Approx bytes pending in sealed WAL files").unwrap();
}

pub async fn run(config_path: &PathBuf, opts: RuntimeOptions) -> Result<()> {
    let cfg = Config::from_file(config_path)?;
    run_with_config(cfg, opts).await
}

pub async fn run_with_config(cfg: Config, opts: RuntimeOptions) -> Result<()> {
    let _exporter_guard = if let Some(addr) = opts.prometheus_bind {
        Some(prometheus_exporter::start(addr).expect("failed to start prometheus exporter"))
    } else {
        None
    };

    if cfg.sources.is_empty() {
        bail!("At least one source is required.");
    }
    if cfg.sinks.is_empty() {
        bail!("At least one sink is required.");
    }
    if cfg.sinks.len() > 1 {
        bail!("You must configure exactly one sink.");
    }

    tracing::info!(target = "startup", config = ?cfg);

    let (name, sink_cfg): (&String, &SinkConfig) = cfg
        .sinks
        .first_key_value()
        .ok_or_else(|| anyhow!("Sink name or config was missing from config."))?;

    let shutdown = CancellationToken::new();

    let sink_manager = Arc::new(
        sinks::manager::SinkManager::new(
            name,
            sink_cfg,
            cfg.batch_size_kb() * 10,
            shutdown.clone(),
        )
        .await?,
    );

    let engine = wasm::WasmEngine::new().expect("engine");
    info!(
        "Batch size: {} KiB, max age: {:?}",
        cfg.batch_size,
        cfg.batch_age_ms()
    );

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

    let consumer_handles = spawn_consumers(cfg, pool.clone(), shutdown.clone()).await;

    if opts.once {
        shutdown.cancel();
    } else {
        wait_for_shutdown_signal().await?;
        shutdown.cancel();
    }

    sink_manager.cancel();
    info!("received shutdown signal...");

    info!("waiting on consumers to shutdown...");
    for h in consumer_handles {
        let _ = tokio::time::timeout(Duration::from_secs(10), h).await;
    }

    let mut pool_owned = Arc::try_unwrap(pool)
        .map_err(|_| anyhow!("pool still has refs; drop all clones before shutdown"))?;
    pool_owned.close();

    info!("waiting on workers to shutdown...");
    let _ = timeout(Duration::from_secs(15), pool_owned.join()).await;

    let sink_manager_owned = Arc::try_unwrap(sink_manager)
        .map_err(|_| anyhow!("sink_manager still has refs; drop all clones before shutdown"))?;
    info!("waiting on sinks to flush and shutdown...");
    if let Err(e) = sink_manager_owned.join().await {
        tracing::warn!("sink manager join failed: {e}");
    }

    Ok(())
}

async fn spawn_consumers(
    cfg: Config,
    pool: Arc<worker::WorkerPool>,
    shutdown: CancellationToken,
) -> Vec<tokio::task::JoinHandle<()>> {
    let mut handles = Vec::new();
    for src in cfg.sources {
        let pool = pool.clone();
        let shutdown = shutdown.clone();
        match src.1.kind {
            SourceKind::MSK(kc) => {
                handles.push(tokio::spawn(async move {
                    if let Err(e) = sources::msk::run_consumer(
                        kc,
                        src.1.common.decoding,
                        pool,
                        shutdown.clone(),
                    )
                    .await
                    {
                        tracing::error!("msk consumer error: {e}");
                    }
                }));
            }
            SourceKind::Socket(sc) => {
                handles.push(tokio::spawn(async move {
                    if let Err(e) = sources::socket::run_consumer(
                        sc,
                        src.1.common.decoding,
                        pool,
                        shutdown.clone(),
                    )
                    .await
                    {
                        tracing::error!("socket listener error: {e}");
                    }
                }));
            }
            SourceKind::SQS(sq) => {
                handles.push(tokio::spawn(async move {
                    if let Err(e) = sources::sqs::run_consumer(
                        sq,
                        src.1.common.decoding,
                        cfg.batch_size,
                        pool,
                        shutdown.clone(),
                    )
                    .await
                    {
                        tracing::error!("SQS consumer error: {e}");
                    }
                }));
            }
        }
    }

    handles
}

/// If a caller wants the lib to handle Ctrl-C on its own.
pub async fn wait_for_shutdown_signal() -> Result<()> {
    #[cfg(unix)]
    {
        use tokio::signal;
        use tokio::signal::unix::{signal as unix_signal, SignalKind};
        let mut term = unix_signal(SignalKind::terminate())?;
        tokio::select! {
            _ = signal::ctrl_c() => {},
            _ = term.recv() => {},
        }
        Ok(())
    }

    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await?;
        Ok(())
    }
}
