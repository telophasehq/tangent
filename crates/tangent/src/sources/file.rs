use anyhow::Result;
use bytes::BytesMut;
use std::path::PathBuf;
use tangent_shared::sources::file::FileConfig;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use tokio_util::sync::CancellationToken;

use crate::dag::DagRuntime;
use crate::sources::decoding;
use crate::sources::decoding::normalize_to_ndjson;

pub async fn run_consumer(
    name: String,
    cfg: FileConfig,
    chunks: usize,
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

    let mut ndjson = normalize_to_ndjson(&cfg.decoding.format, raw)?;
    let frames = decoding::chunk_ndjson(&mut ndjson, chunks);

    dag_runtime
        .push_from_source(&name, frames, Vec::new())
        .await?;

    let () = shutdown.cancelled().await;
    Ok(())
}
