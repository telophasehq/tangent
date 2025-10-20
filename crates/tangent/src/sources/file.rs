use anyhow::Result;
use bytes::BytesMut;
use std::path::PathBuf;
use std::sync::Arc;
use tangent_shared::sources::file::FileConfig;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio_util::sync::CancellationToken;

use crate::sources::decoding;
use crate::worker::{Record, WorkerPool};

pub async fn run_consumer(
    cfg: FileConfig,
    pool: Arc<WorkerPool>,
    shutdown: CancellationToken,
) -> Result<()> {
    let path: PathBuf = cfg.path;
    let dc = cfg.decoding.clone();

    let mut f = File::open(&path).await?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf).await?;

    let sniff = &buf[..buf.len().min(8)];
    let comp = dc.resolve_compression(None, path.file_name().and_then(|s| s.to_str()), sniff);
    let raw = decoding::decompress_bytes(comp, &buf)?;
    let fmt = dc.resolve_format(&raw);
    let ndjson = decoding::normalize_to_ndjson(fmt, &raw);

    let mut cur = BytesMut::with_capacity(ndjson.len());
    for line in ndjson.split(|&b| b == b'\n') {
        if line.is_empty() {
            continue;
        }
        cur.extend_from_slice(line);
        cur.extend_from_slice(b"\n");
        pool.dispatch(Record {
            payload: cur.split().freeze(),
            ack: None,
        })
        .await;
    }

    let _ = shutdown.cancelled().await;
    Ok(())
}
