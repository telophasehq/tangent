use anyhow::Result;
use bytes::BytesMut;
use memchr::memchr;
use std::io;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::dag::DagRuntime;
use tangent_shared::sources::tcp::TcpConfig;

fn drain_ndjson_lines(buf: &mut BytesMut) -> Vec<BytesMut> {
    let mut out = Vec::with_capacity(500);

    while let Some(nl) = memchr(b'\n', &buf[..]) {
        let line = buf.split_to(nl + 1);
        out.push(line);
    }

    out
}

pub async fn run_consumer(
    name: Arc<str>,
    cfg: TcpConfig,
    dag_runtime: DagRuntime,
    shutdown: CancellationToken,
) -> Result<()> {
    let listener = TcpListener::bind(cfg.bind_address).await?;

    let read_buf_cap = cfg.read_buffer_size.max(8 * 1024);

    let (err_tx, mut err_rx) = mpsc::channel::<anyhow::Error>(64);

    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,

            accept_res = listener.accept() => {
                let (mut stream, remote_addr) = match accept_res {
                    Ok(pair) => pair,
                    Err(e) => {
                        tracing::warn!("tcp accept error: {e}");
                        continue;
                    }
                };

                if let Err(e) = stream.set_nodelay(true) {
                    tracing::debug!("failed to enable TCP_NODELAY: {e}");
                }

                let err_tx = err_tx.clone();
                let dag = dag_runtime.clone();
                let source_name = Arc::clone(&name);
                let addr = remote_addr;

                tokio::spawn(async move {
                    let mut buf = BytesMut::with_capacity(read_buf_cap);

                    loop {
                        match stream.read_buf(&mut buf).await {
                            Ok(0) => {
                                if !buf.is_empty() {
                                    if !buf.ends_with(b"\n") {
                                        buf.extend_from_slice(b"\n");
                                    }
                                    let frames = drain_ndjson_lines(&mut buf);
                                    if let Err(e) = dag
                                        .push_from_source(source_name.clone(), frames, Vec::new())
                                        .await
                                    {
                                        let _ = err_tx.send(e).await;
                                    }
                                }
                                break;
                            }
                            Ok(_) => {
                                let frames = drain_ndjson_lines(&mut buf);
                                if !frames.is_empty() {
                                    if let Err(e) = dag
                                        .push_from_source(source_name.clone(), frames, Vec::new())
                                        .await
                                    {
                                        let _ = err_tx.send(e).await;
                                        break;
                                    }
                                }

                                if buf.capacity() > read_buf_cap * 8 && buf.len() < read_buf_cap {
                                    let mut new_buf = BytesMut::with_capacity(read_buf_cap);
                                    new_buf.extend_from_slice(&buf[..]);
                                    buf = new_buf;
                                }
                            }
                            Err(e) if e.kind() == io::ErrorKind::Interrupted => continue,
                            Err(e) => {
                                tracing::warn!(remote = ?addr, "tcp read error: {e}");
                                break;
                            }
                        }
                    }
                });
            }

            Some(err) = err_rx.recv() => {
                return Err(err);
            }
        }
    }

    Ok(())
}
