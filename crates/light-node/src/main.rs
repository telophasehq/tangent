use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
use tangent_shared::Config;
use tokio::net::UnixListener;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

use prometheus::{register_histogram, register_int_counter, Histogram, IntCounter};
use prometheus_exporter;

mod wasm;

lazy_static::lazy_static! {    static ref BATCH_LATENCY: Histogram = register_histogram!(
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

#[tokio::main]
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

    info!("Starting light node socket listener...");
    let _ = std::fs::remove_file(&cfg.socket_path);
    let listener = UnixListener::bind(&cfg.socket_path)?;
    info!("Listening on {}", cfg.socket_path.display());

    let engine = wasm::WasmEngine::new().expect("engine");
    info!(
        "Batch size: {} KiB, max age: {:?}",
        cfg.batch_size >> 10,
        cfg.batch_age_ms()
    );

    let workers = engine
        .spawn_workers(cfg.workers, cfg.batch_size_kb(), cfg.batch_age_ms())
        .await;

    let mut rr = 0usize;
    loop {
        let (stream, _) = listener.accept().await?;
        let tx = &workers[rr % workers.len()];
        rr = rr.wrapping_add(1);

        // TODO: we should probably use try_send here.
        if tx.send(stream).await.is_err() {
            error!("worker channel closed");
        }
    }
}
