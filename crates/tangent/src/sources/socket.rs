use anyhow::Result;
use bytes::BytesMut;
use std::sync::Arc;
use tangent_shared::sources::socket::SocketConfig;
use tokio::net::UnixListener;
use tokio_stream::StreamExt;
use tokio_util::codec::{BytesCodec, FramedRead};
use tokio_util::sync::CancellationToken;

use crate::worker::{Record, WorkerPool};

pub async fn run_consumer(
    cfg: SocketConfig,
    pool: Arc<WorkerPool>,
    shutdown: CancellationToken,
) -> Result<()> {
    let _ = std::fs::remove_file(&cfg.socket_path);
    let listener = UnixListener::bind(&cfg.socket_path)?;

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            Ok((us, _)) = listener.accept() => {
                let pool = pool.clone();
                tokio::spawn(async move {
                    let mut framed = FramedRead::new(us, BytesCodec::new());
                    while let Some(line_res) = framed.next().await {
                        match line_res {
                            Ok(line) => {
                                let mut b = BytesMut::with_capacity(line.len() + 1);
                                b.extend_from_slice(&line);
                                b.extend_from_slice(b"\n");
                                let _ = pool.dispatch(Record{
                                    payload: b.freeze(),
                                    ack: None,
                            }).await;
                            }
                            Err(e) => {
                                tracing::warn!("socket read error: {e}");
                                break;
                            }
                        }
                    }
                });
            }
        }
    }
    Ok(())
}
