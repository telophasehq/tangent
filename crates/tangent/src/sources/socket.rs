use anyhow::Result;
use bytes::BytesMut;
use std::io;
use tangent_shared::sources::common::DecodeFormat;
use tokio::io::AsyncReadExt;
use tokio::net::UnixListener;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::dag::DagRuntime;
use crate::sources::decoding;
use tangent_shared::sources::socket::SocketConfig;

pub async fn run_consumer(
    name: String,
    cfg: SocketConfig,
    chunks: usize,
    dag_runtime: DagRuntime,
    shutdown: CancellationToken,
) -> Result<()> {
    let _ = std::fs::remove_file(&cfg.socket_path);
    let listener = UnixListener::bind(&cfg.socket_path)?;

    let read_buf_cap: usize = 512 * 1024;

    let (err_tx, mut err_rx) = mpsc::channel::<anyhow::Error>(64);

    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,

            Ok((mut us, _addr)) = listener.accept() => {
                let err_tx = err_tx.clone();
                let dag = dag_runtime.clone();
                let source_name = name.clone();

                tokio::spawn(async move {
                    let mut buf = BytesMut::with_capacity(read_buf_cap);

                    loop {
                        match us.read_buf(&mut buf).await {
                            Ok(0) => {
                                if !buf.is_empty() {
                                    if !buf.ends_with(b"\n") { buf.extend_from_slice(b"\n"); }
                                    let drained = buf.split();
                                    let mut ndjson: BytesMut;
                                    match decoding::normalize_to_ndjson(&DecodeFormat::Ndjson, drained) {
                                        Ok(v) => {
                                            ndjson = v;
                                        }
                                        Err(e) => {
                                            let _ = err_tx.send(e).await;
                                            break;
                                        }
                                    }
                                    let frames = decoding::chunk_ndjson(&mut ndjson, chunks);
                                    let _ = dag.push_from_source(&source_name, frames, Vec::new()).await;
                                }
                                break;
                            }
                            Ok(_n) => {
                                if buf.len() > 0  {
                                    let drained = buf.split();
                                    let mut ndjson: BytesMut;
                                    match decoding::normalize_to_ndjson(&DecodeFormat::Ndjson, drained) {
                                        Ok(v) => {
                                            ndjson = v;
                                        }
                                        Err(e) => {
                                            let _ = err_tx.send(e).await;
                                            break;
                                        }
                                    }
                                    let frames = decoding::chunk_ndjson(&mut ndjson, chunks);
                                    if let Err(e) = dag.push_from_source(&source_name, frames, Vec::new()).await {
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
                                tracing::warn!("socket read error: {e}");
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
