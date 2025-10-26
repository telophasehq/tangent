use anyhow::Result;
use bytes::BufMut;
use bytes::BytesMut;
use memchr::memchr_iter;
use std::path::PathBuf;
use tangent_shared::sources::file::FileConfig;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio_util::sync::CancellationToken;

use crate::dag::DagRuntime;
use crate::sources::decoding;

const FRAME_TARGET: usize = 256 * 1024;
pub async fn run_consumer(
    name: String,
    cfg: FileConfig,
    dag_runtime: DagRuntime,
    shutdown: CancellationToken,
) -> Result<()> {
    let path: PathBuf = cfg.path;
    let dc = cfg.decoding.clone();

    let mut f = File::open(&path).await?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf).await?;
    let buf = BytesMut::from(&buf[..]);

    let sniff = &buf[..buf.len().min(8)];
    let comp = dc.resolve_compression(None, path.file_name().and_then(|s| s.to_str()), sniff);
    let raw = decoding::decompress_bytes(&comp, buf)?;
    let fmt = dc.resolve_format(&raw);
    let ndjson = decoding::normalize_to_ndjson(&fmt, raw);

    let mut frames = Vec::<BytesMut>::new();
    let mut cur = BytesMut::with_capacity(FRAME_TARGET);

    let mut start = 0usize;
    for nl in memchr_iter(b'\n', &ndjson) {
        if nl > start {
            cur.extend_from_slice(&ndjson[start..nl]);
            cur.put_u8(b'\n');
        }
        start = nl + 1;

        if cur.len() >= FRAME_TARGET {
            frames.push(cur.split());
            if cur.capacity() < FRAME_TARGET {
                cur.reserve(FRAME_TARGET - cur.capacity());
            }
        }
    }

    if start < ndjson.len() {
        cur.extend_from_slice(&ndjson[start..]);
        cur.put_u8(b'\n');
    }

    if !cur.is_empty() {
        frames.push(cur.split());
    }

    dag_runtime
        .push_from_source(&name, frames, Vec::new())
        .await?;

    let () = shutdown.cancelled().await;
    Ok(())
}
