use anyhow::{Context, Result};
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{self, io::AsyncWriteExt, net::UnixStream};
use tracing::info;

pub async fn run_bench(
    name: Arc<str>,
    socket: PathBuf,
    connections: u16,
    payload: Vec<u8>,
    max_bytes: usize,
    seconds: u64,
) -> Result<()> {
    info!("===Starting benchmark===");
    info!(
        "source={} uds={:?} connections={}",
        name, socket, connections,
    );

    let max_bytes = max_bytes;
    let seconds = seconds;
    let mut handles = Vec::with_capacity(connections as usize);

    for _ in 0..connections {
        let line_cl = payload.clone();
        let uds = socket.clone();

        handles.push(tokio::spawn(async move {
            let mut s = UnixStream::connect(&uds)
                .await
                .with_context(|| format!("socket does not exist: {}", uds.display()))?;
            let deadline = Instant::now() + Duration::from_secs(seconds);
            let mut buf = Vec::with_capacity(max_bytes.max(line_cl.len()));

            let mut events_per_buff: u64 = 0;
            while buf.len() + line_cl.len() <= max_bytes {
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
