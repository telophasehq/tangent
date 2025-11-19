use anyhow::{Context, Result};
use serde_json::Value;
use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{self, io::AsyncWriteExt, net::TcpStream};
use tracing::info;

use crate::synthesize::{Scope, Synth};

pub async fn run_bench(
    name: Arc<str>,
    addr: SocketAddr,
    connections: u16,
    payload: Vec<u8>,
    max_bytes: usize,
    seconds: u64,
    synthesize_payload: bool,
) -> Result<()> {
    info!("===Starting benchmark===");
    info!("source={} tcp={} connections={}", name, addr, connections);

    let mut handles = Vec::with_capacity(connections as usize);

    static BYTES_SENT: AtomicU64 = AtomicU64::new(0);
    static RECS_SENT: AtomicU64 = AtomicU64::new(0);
    let start = Instant::now();

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

            let mut synth = Synth::new(rand::random::<u64>());
            let templates: Vec<Value> = payload
                .split(|b| *b == b'\n')
                .filter(|line| !line.is_empty())
                .map(|line| serde_json::from_slice::<Value>(line))
                .collect::<Result<_, _>>()?;

            let deadline = Instant::now() + Duration::from_secs(seconds);
            let mut buf = Vec::with_capacity(max_bytes.max(payload.len()));

            let mut total_events: u64 = 0;
            while Instant::now() < deadline {
                buf.clear();
                let mut events_per_buff: u64 = 0;
                if synthesize_payload && templates.len() > 0 {
                    'fill: loop {
                        for template in templates.iter() {
                            let mut scope = Scope::new(&template);

                            let v = synth.gen(&template, &mut scope)?;

                            let mut line = serde_json::to_vec(&v)?;
                            line.push(b'\n');

                            if max_bytes > 0 && buf.len() + line.len() > max_bytes {
                                break 'fill;
                            }

                            buf.extend_from_slice(&line);
                            events_per_buff += 1;

                            if max_bytes == 0 {
                                break 'fill;
                            }
                        }
                    }
                } else {
                    while max_bytes == 0 || buf.len() + payload.len() <= max_bytes {
                        buf.extend_from_slice(&payload);
                        events_per_buff += 1;
                        if max_bytes == 0 {
                            break;
                        }
                    }
                }

                if buf.is_empty() {
                    buf.extend_from_slice(&payload);
                    events_per_buff = 1;
                }

                let buf_len = buf.len();
                match stream.write_all(&buf).await {
                    Ok(()) => {
                        total_events += events_per_buff;
                        BYTES_SENT.fetch_add(buf_len as u64, Ordering::Relaxed);
                        RECS_SENT.fetch_add(events_per_buff, Ordering::Relaxed);
                    }
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

    let bytes = BYTES_SENT.load(Ordering::Relaxed);
    let records = RECS_SENT.load(Ordering::Relaxed);
    let secs = start.elapsed().as_secs_f64();
    println!(
        "sent_bytes={} sent_records={} MB/s={:.2} MiB/s={:.2} recs/s={:.0}",
        bytes,
        records,
        (bytes as f64) / (1_000_000.0 * secs),
        (bytes as f64) / (1_048_576.0 * secs),
        (records as f64) / secs,
    );

    Ok(())
}
