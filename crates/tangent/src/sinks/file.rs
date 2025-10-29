use anyhow::Result;
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tangent_shared::sinks::common::{CommonSinkOptions, Compression, Encoding};
use tangent_shared::sinks::file::FileConfig;
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use crate::sinks::encoding;
use crate::sinks::manager::{Sink, SinkWrite};
use crate::{SINK_BYTES_TOTAL, SINK_BYTES_UNCOMPRESSED_TOTAL, SINK_OBJECTS_TOTAL};

pub struct FileSinkItem {
    pub path: String,
}

pub struct FileSink {
    path: PathBuf,
    encoding: Encoding,
    compression: Compression,
    file: Mutex<tokio::fs::File>,
}

impl FileSink {
    pub async fn new(cfg: &FileConfig, common: &CommonSinkOptions) -> Result<Arc<Self>> {
        let path: PathBuf = cfg.path.to_path_buf();
        if let Some(dir) = path.parent() {
            fs::create_dir_all(dir).await?;
        }

        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&path)
            .await?;

        Ok(Arc::new(Self {
            path,
            encoding: common.encoding.clone(),
            compression: common.compression.clone(),
            file: Mutex::new(file),
        }))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[async_trait]
impl Sink for FileSink {
    async fn write(&self, req: SinkWrite) -> Result<()> {
        let uncompressed_bytes = req.payload.len();
        let normalized_payload =
            encoding::normalize_from_ndjson(&self.encoding, &self.compression, req.payload)?;

        self.file
            .lock()
            .await
            .write_all(&normalized_payload)
            .await?;

        SINK_OBJECTS_TOTAL.inc();
        SINK_BYTES_TOTAL.inc_by(normalized_payload.len() as u64);
        SINK_BYTES_UNCOMPRESSED_TOTAL.inc_by(uncompressed_bytes as u64);
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        self.file.lock().await.sync_data().await?;
        Ok(())
    }
}
