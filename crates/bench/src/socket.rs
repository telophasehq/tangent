use anyhow::{Context, Result};
use serde_json::Value;
use std::{
    fs,
    path::PathBuf,
    time::{Duration, Instant},
};
use tokio::{self, io::AsyncWriteExt, net::UnixStream};
use tracing::info;

pub async fn run_bench(
    name: &String,
    socket: PathBuf,
    connections: u16,
    payload_path: PathBuf,
    max_bytes: usize,
    seconds: u64,
) -> Result<()> {
    let payload = fs::read_to_string(&payload_path)
        .with_context(|| format!("failed to read payload file {}", payload_path.display()))?;

    let one_line = serde_json::to_string(&serde_json::from_str::<Value>(&payload)?)?;

    let mut line = Vec::with_capacity(one_line.len() + 1);
    line.extend_from_slice(one_line.as_bytes());
    line.push(b'\n');

    info!("===Starting {name} benchmark===");
    info!(
        "uds={:?} payload={:?} bytes/line={} connections={}",
        socket,
        payload_path,
        line.len(),
        connections,
    );

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

            let mut events_per_buff: u64 = 0;
            while max_bytes < line_cl.len() || buf.len() + line_cl.len() <= max_bytes {
                buf.extend_from_slice(&line_cl);
                events_per_buff += 1;
            }

            let mut total_events: u64 = 0;
            while Instant::now() < deadline {
                match s.write_all(&buf).await {
                    Ok(()) => total_events += events_per_buff,
                    Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => break,
                    Err(e) => return Err(e.into()),
                }
            }
            anyhow::Ok(total_events)
        }));
    }

    futures::future::try_join_all(handles)
        .await?
        .into_iter()
        .try_fold(0u64, |acc, res| res.map(|v| acc + v))?;

    Ok(())
}
