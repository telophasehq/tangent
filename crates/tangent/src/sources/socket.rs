// consumers/socket.rs
use anyhow::Result;
use bytes::BufMut;
use bytes::{Bytes, BytesMut};
use std::sync::Arc;
use tangent_shared::socket::SocketConfig;
use tangent_shared::source::{DecodeCompression, DecodeFormat, Decoding};
use tokio::io::AsyncReadExt;
use tokio::net::UnixListener;
use tokio_util::sync::CancellationToken;

use crate::sources::decoding;
use crate::worker::{Record, WorkerPool};

pub async fn run_consumer(
    cfg: SocketConfig,
    dc: Decoding,
    pool: Arc<WorkerPool>,
    shutdown: CancellationToken,
) -> Result<()> {
    let _ = std::fs::remove_file(&cfg.socket_path);
    let listener = UnixListener::bind(&cfg.socket_path)?;

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            accept_res = listener.accept() => {
                match accept_res {
                    Ok((mut us, _addr)) => {
                        let pool = pool.clone();
                        let dc = dc.clone();
                        tokio::spawn(async move {
                            let mut head = vec![0u8; 4096.min(64 * 1024)];
                            match us.read(&mut head).await {
                                Ok(0) => return,
                                Ok(n) => head.truncate(n),
                                Err(e) => { tracing::warn!("socket read head error: {e}"); return; }
                            }

                            let comp = dc.resolve_compression(None, None, &head[..head.len().min(8)]);
                            let looks_compressed = matches!(comp, DecodeCompression::Gzip | DecodeCompression::Zstd);

                            if looks_compressed {
                                let mut buf = head;
                                if let Err(e) = us.read_to_end(&mut buf).await {
                                    tracing::warn!("socket read body error: {e}");
                                    return;
                                }
                                let raw = match decoding::decompress_bytes(comp, &buf) {
                                    Ok(v) => v,
                                    Err(e) => { tracing::warn!("decompress failed: {e}"); return; }
                                };
                                let fmt = dc.resolve_format(&raw);
                                let ndjson = decoding::normalize_to_ndjson(fmt, &raw);
                                let _ = pool.dispatch(Record{
                                    payload: Bytes::from(ndjson),
                                    ack: None,
                                }).await;
                                return;
                            }

                            let fmt = dc.resolve_format(&head);
                            match fmt {
                                DecodeFormat::Ndjson | DecodeFormat::Text => {
                                    let mut carry = BytesMut::new();
                                    stream_lines_to_pool(head, &mut us, &pool, &mut carry).await;
                                    if !carry.is_empty() {
                                        carry.put_u8(b'\n');
                                        let _ = pool.dispatch(Record{
                                            payload: carry.freeze(),
                                            ack: None,
                                        }).await;
                                    }
                                }
                                DecodeFormat::Json | DecodeFormat::JsonArray | DecodeFormat::Msgpack => {
                                    let mut buf = head;
                                    if let Err(e) = us.read_to_end(&mut buf).await {
                                        tracing::warn!("socket read body error: {e}");
                                        return;
                                    }
                                    let ndjson = decoding::normalize_to_ndjson(fmt, &buf);
                                    let _ = pool.dispatch(Record{
                                        payload: Bytes::from(ndjson),
                                        ack: None,
                                    }).await;
                                }
                                DecodeFormat::Auto => unreachable!("auto should be resolved in resolve_format"),
                            }
                        });
                    }
                    Err(e) => {
                        tracing::warn!("socket accept error: {e}");
                    }
                }
            }
        }
    }
    Ok(())
}

async fn stream_lines_to_pool(
    head: Vec<u8>,
    us: &mut tokio::net::UnixStream,
    pool: &Arc<WorkerPool>,
    carry: &mut BytesMut,
) {
    carry.extend_from_slice(&head);
    emit_complete_lines(pool, carry).await;

    let mut buf = [0u8; 64 * 1024];
    loop {
        match us.read(&mut buf).await {
            Ok(0) => break,
            Ok(n) => {
                carry.extend_from_slice(&buf[..n]);
                emit_complete_lines(pool, carry).await;
            }
            Err(e) => {
                tracing::warn!("socket read error: {e}");
                break;
            }
        }
    }
}

async fn emit_complete_lines(pool: &Arc<WorkerPool>, carry: &mut BytesMut) {
    if let Some(last_nl) = carry.iter().rposition(|&b| b == b'\n') {
        let mut start = 0usize;
        while start <= last_nl {
            if let Some(pos) = memchr::memchr(b'\n', &carry[start..=last_nl]) {
                let end = start + pos + 1;
                let line = carry[start..end].to_vec();
                let _ = pool
                    .dispatch(Record {
                        payload: Bytes::from(line),
                        ack: None,
                    })
                    .await;
                start = end;
            } else {
                break;
            }
        }
        let tail = carry.split_off(last_nl + 1);
        *carry = tail;
    }
}
