use anyhow::{Context, Result};
use clap::Parser;
use serde_json::Value;
use std::{
    fs,
    path::PathBuf,
    time::{Duration, Instant},
};
use tokio::{self, io::AsyncWriteExt, net::UnixStream};
use tracing::info;

/// NDJSON UDS load generator
#[derive(Parser, Debug)]
struct Args {
    /// UDS path (must match sidecar SOCKET_PATH)
    #[arg(long, default_value = "/tmp/sidecar.sock")]
    socket: PathBuf,
    /// Duration (seconds)
    #[arg(long, default_value_t = 15)]
    seconds: u64,
    /// Duration (seconds)
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

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    tracing_subscriber::fmt().init();

    let args = Args::parse();

    if let Some(payload) = args.payload {
        run_bench(
            args.socket,
            args.connections,
            payload,
            args.max_bytes,
            args.seconds,
        )
        .await?;
    } else {
        for entry in fs::read_dir("bench/test_data")? {
            let entry = entry?;
            run_bench(
                args.socket.clone(),
                args.connections,
                entry.path(),
                args.max_bytes,
                args.seconds,
            )
            .await?;
        }
    }

    Ok(())
}

async fn run_bench(
    socket: PathBuf,
    connections: u16,
    payload: PathBuf,
    max_bytes: usize,
    seconds: u64,
) -> Result<()> {
    let payload = fs::read_to_string(&payload)
        .with_context(|| format!("failed to read payload file {}", payload.display()))?;

    let one_line = serde_json::to_string(&serde_json::from_str::<Value>(&payload)?)?;

    info!("===Starting benchmark===");
    info!(
        "uds={:?} payload={:?} bytes/line={} connections={}",
        socket,
        payload,
        payload.len(),
        connections,
    );

    let mut line = Vec::with_capacity(one_line.len() + 1);
    line.extend_from_slice(one_line.as_bytes());
    line.push(b'\n');

    let max_bytes = max_bytes;
    let seconds = seconds;
    let bytes_per_event = line.len() as u64;
    let mut handles = Vec::with_capacity(connections as usize);

    for _ in 0..connections {
        let line_cl = line.clone();
        let uds = socket.clone();

        handles.push(tokio::spawn(async move {
            let mut s = UnixStream::connect(&uds)
                .await
                .with_context(|| format!("socket does not exist: {}", uds.display()))?;
            let deadline = Instant::now() + Duration::from_secs(seconds);
            let mut buf = Vec::with_capacity(max_bytes.max(line_cl.len()));
            let mut i: u64 = 0;

            while Instant::now() < deadline {
                buf.clear();
                while buf.len() + line_cl.len() <= max_bytes {
                    buf.extend_from_slice(&line_cl);
                    i += 1;
                }

                s.write_all(&buf).await?;
            }
            anyhow::Ok(i)
        }));
    }

    let appends: u64 = futures::future::try_join_all(handles)
        .await?
        .into_iter()
        .try_fold(0u64, |acc, res| res.map(|v| acc + v))?;

    let total_bytes = appends * bytes_per_event;
    let mb_per_sec = (total_bytes as f64 / (1024.0 * 1024.0)) / seconds as f64;

    info!(
        "events per second = {}, MB per second = {:.2}",
        appends / seconds,
        mb_per_sec
    );

    Ok(())
}
