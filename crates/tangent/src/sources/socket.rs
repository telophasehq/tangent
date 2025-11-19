use anyhow::Result;
use bytes::BytesMut;
use memchr::memchr;
use std::io;
use std::sync::Arc;
use tangent_shared::dag::NodeRef;
use tokio::io::AsyncReadExt;
use tokio::net::UnixListener;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::router::Router;
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
    router: Arc<Router>,
    shutdown: CancellationToken,
) -> Result<()> {
    let _ = std::fs::remove_file(&cfg.socket_path);
    let listener = UnixListener::bind(&cfg.socket_path)?;

    let read_buf_cap: usize = 512 * 1024;

    let (err_tx, mut err_rx) = mpsc::channel::<anyhow::Error>(64);

    let from = NodeRef::Source { name: name };
    let mut js = JoinSet::new();

    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,

            Ok((mut us, _addr)) = listener.accept() => {
                let err_tx = err_tx.clone();
                let from = from.clone();
                let router = router.clone();
                let shutdown2 = shutdown.clone();

                js.spawn(async move {
                    let mut buf = BytesMut::with_capacity(read_buf_cap);

                    loop {
                        tokio::select!{
                            _ = shutdown2.cancelled() => break,
                            r = us.read_buf(&mut buf) => {
                                match r {
                                Ok(0) => {
                                    if !buf.is_empty() {
                                        if !buf.ends_with(b"\n") { buf.extend_from_slice(b"\n"); }
                                        let frames = drain_ndjson_lines(&mut buf);
                                        let _ = router.forward(&from, frames, Vec::new()).await;
                                    }
                                    break;
                                }
                                Ok(_n) => {
                                    let frames = drain_ndjson_lines(&mut buf);
                                    if !frames.is_empty() {
                                        if let Err(e) = router.forward(&from, frames, Vec::new()).await {
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
                    }
                }
            });
        }

        Some(err) = err_rx.recv() => {
            return Err(err);
        }
        }
    }

    while let Some(res) = js.join_next().await {
        if let Err(e) = res {
            tracing::warn!("connection task failed: {e}");
        }
    }

    Ok(())
}
