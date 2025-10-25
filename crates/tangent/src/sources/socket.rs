use anyhow::Result;
use bytes::{Bytes, BytesMut};
use memchr::memchr;
use std::sync::Arc;
use tangent_shared::sources::socket::SocketConfig;
use tokio::net::UnixListener;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tokio_util::codec::{Decoder, FramedRead};
use tokio_util::sync::CancellationToken;

use crate::worker::{Record, WorkerPool};

struct BytesLines;

impl Decoder for BytesLines {
    type Item = Bytes;
    type Error = std::io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Bytes>, std::io::Error> {
        memchr(b'\n', buf).map_or_else(
            || Ok(None),
            |nl| {
                let line = buf.split_to(nl + 1);
                Ok(Some(line.freeze()))
            },
        )
    }
}

pub async fn run_consumer(
    cfg: SocketConfig,
    pool: Arc<WorkerPool>,
    shutdown: CancellationToken,
) -> Result<()> {
    let _ = std::fs::remove_file(&cfg.socket_path);
    let listener = UnixListener::bind(&cfg.socket_path)?;
    let (err_tx, mut err_rx) = mpsc::channel::<anyhow::Error>(64);

    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            Ok((us, _)) = listener.accept() => {
                let pool = pool.clone();
                let err_tx = err_tx.clone();

                tokio::spawn(async move {
                    let mut framed = FramedRead::new(us, BytesLines);
                    while let Some(line_res) = framed.next().await {
                        match line_res {
                            Ok(line) => {
                                if let Err(e) = pool.dispatch(Record { payload: line, ack: None }).await {
                                    let _ = err_tx.send(e).await;
                                    break;
                                }
                            }
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
