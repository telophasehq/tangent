use anyhow::{Context, Result};
use serde_json::Value;
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{self, io::AsyncWriteExt, net::UnixStream};
use tracing::info;

use crate::synthesize::{Scope, Synth};

pub async fn run_bench(
    name: Arc<str>,
    socket: PathBuf,
    connections: u16,
    payload: Vec<u8>,
    max_bytes: usize,
    seconds: u64,
    synthesize_payload: bool,
) -> Result<()> {
    info!("===Starting benchmark===");
    info!(
        "source={} uds={:?} connections={}",
        name, socket, connections,
    );

    let mut handles = Vec::with_capacity(connections as usize);

    for _ in 0..connections {
        let payload = payload.clone();
        let uds = socket.clone();

        handles.push(tokio::spawn(async move {
            let mut s = UnixStream::connect(&uds)
                .await
                .with_context(|| format!("socket does not exist: {}", uds.display()))?;
            let deadline = Instant::now() + Duration::from_secs(seconds);

            let mut synth = Synth::new(rand::random::<u64>());
            let templates: Vec<Option<Value>> = payload
                .clone()
                .split(|b| *b == b'\n')
                .find(|line| !line.is_empty())
                .iter()
                .map(|line| serde_json::from_slice::<Value>(line).ok())
                .collect();

            let mut buf: Vec<u8> = Vec::with_capacity(max_bytes.max(payload.len()));
            let mut total_events: u64 = 0;

            while Instant::now() < deadline {
                buf.clear();
                let mut events_per_buff: u64 = 0;

                if synthesize_payload && templates.len() > 0 {
                    'fill: loop {
                        for template in templates.iter() {
                            if let Some(tmpl) = template {
                                let mut scope = Scope::new(&tmpl);

                                let v = synth.gen(&tmpl, &mut scope)?;

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
                            } else {
                                anyhow::bail!("failed to synthesize log")
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
