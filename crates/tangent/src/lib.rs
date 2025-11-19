use anyhow::{bail, Result};
use std::{net::SocketAddr, path::PathBuf};
use tokio::time::Duration;
use tokio_util::sync::CancellationToken;
use tracing::info;

use prometheus::{
    register_histogram_vec, register_int_counter, register_int_gauge, HistogramVec, IntCounter,
    IntGauge,
};

use tangent_shared::Config;

use crate::dag::DagRuntime;

pub mod dag;
pub mod router;
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

    let _exporter_guard = opts
        .prometheus_bind
        .map(|addr| prometheus_exporter::start(addr).expect("failed to start prometheus exporter"));

    if cfg.sources.is_empty() {
        bail!("At least one source is required.");
    }
    if cfg.sinks.is_empty() {
        bail!("At least one sink is required.");
    }
    if cfg.dag.is_empty() {
        bail!("Must configure dag.");
    }

    if std::env::var("DEBUG").is_ok_and(|x| x == "1") {
        console_subscriber::init();
    }
    tracing::info!(target = "startup", config = ?cfg);

    let ingest_shutdown = CancellationToken::new();

    info!(
        "Batch size: {} KiB, max age: {:?}",
        cfg.runtime.batch_size,
        cfg.batch_age_ms()
    );

    let dag_runtime = DagRuntime::build(cfg, &config_path, ingest_shutdown.clone()).await?;

    #[cfg(feature = "alloc-prof")]
    jemalloc_dump("warm");

    if !opts.once {
        wait_for_shutdown_signal().await?;
    }

    #[cfg(feature = "alloc-prof")]
    jemalloc_dump("pre_teardown");

    info!("received shutdown signal...");
    ingest_shutdown.cancel();

    dag_runtime
        .shutdown(Duration::from_secs(120), Duration::from_secs(120))
        .await?;

    Ok(())
}

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

#[cfg(feature = "alloc-prof")]
fn jemalloc_dump(tag: &str) {
    use tikv_jemalloc_ctl;
    unsafe {
        let _: tikv_jemalloc_ctl::Result<()> =
            tikv_jemalloc_ctl::raw::write::<*const libc::c_char>(b"prof.dump\0", std::ptr::null());
    }
    tracing::warn!("jemalloc: forced heap dump ({tag})");
}
