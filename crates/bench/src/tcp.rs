use anyhow::{Context, Result};
use std::{
    net::SocketAddr,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{self, io::AsyncWriteExt, net::TcpStream};
use tracing::info;

pub async fn run_bench(
    name: Arc<str>,
    addr: SocketAddr,
    connections: u16,
    payload: Vec<u8>,
    max_bytes: usize,
    seconds: u64,
) -> Result<()> {
    info!("===Starting benchmark===");
    info!("source={} tcp={} connections={}", name, addr, connections);

    let mut handles = Vec::with_capacity(connections as usize);

    for _ in 0..connections {
        let payload = payload.clone();
        let addr = addr;

        handles.push(tokio::spawn(async move {
            let mut stream = TcpStream::connect(addr)
                .await
                .with_context(|| format!("tcp address unreachable: {addr}"))?;
            stream
                .set_nodelay(true)
                .with_context(|| format!("failed to enable TCP_NODELAY for {addr}"))?;

            let deadline = Instant::now() + Duration::from_secs(seconds);
            let mut buf = Vec::with_capacity(max_bytes.max(payload.len()));

            let mut events_per_buff: u64 = 0;
            while buf.len() + payload.len() <= max_bytes {
                buf.extend_from_slice(&payload);
                events_per_buff += 1;
            }

            let mut total_events: u64 = 0;
            while Instant::now() < deadline {
                match stream.write_all(&buf).await {
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
