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

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();
    let _exporter =
        prometheus_exporter::start("0.0.0.0:9184".parse().expect("failed to parse binding"))
            .expect("failed to start prometheus exporter");

    let _ = &*BATCH_LATENCY;
    let _ = &*BATCH_EVENTS;

    info!("Starting light node socket listener...");
    let path = std::env::var("SOCKET_PATH").unwrap_or("/tmp/sidecar.sock".into());
    let _ = std::fs::remove_file(&path);
    let listener = UnixListener::bind(&path)?;
    info!("Listening on {}", path);

    let engine = wasm::WasmEngine::new().expect("engine");
    let n = std::env::var("WASM_WORKERS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(num_cpus::get);
    let workers = engine.spawn_workers(n).await;

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
