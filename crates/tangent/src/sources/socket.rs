use anyhow::Result;
use bytes::BytesMut;
use memchr::memchr;
use std::io;
use std::sync::Arc;
use tokio::io::AsyncReadExt;
use tokio::net::UnixListener;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::dag::DagRuntime;
use tangent_shared::sources::socket::SocketConfig;

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
    cfg: SocketConfig,
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
                let source_name = Arc::clone(&name);

                tokio::spawn(async move {
                    let mut buf = BytesMut::with_capacity(read_buf_cap);

                    loop {
                        match us.read_buf(&mut buf).await {
                            Ok(0) => {
                                if !buf.is_empty() {
                                    if !buf.ends_with(b"\n") { buf.extend_from_slice(b"\n"); }
                                    let frames = drain_ndjson_lines(&mut buf);
                                    let _ = dag.push_from_source(source_name.clone(), frames, Vec::new()).await;
                                }
                                break;
                            }
                            Ok(_n) => {
                                let frames = drain_ndjson_lines(&mut buf);
                                if !frames.is_empty() {
                                    if let Err(e) = dag.push_from_source(source_name.clone(), frames, Vec::new()).await {
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
