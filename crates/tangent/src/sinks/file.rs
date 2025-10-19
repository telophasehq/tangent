use anyhow::Result;
use async_trait::async_trait;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs::{self, OpenOptions};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use crate::sinks::manager::{Sink, SinkWrite};

pub struct FileSinkItem {
    pub path: String,
}

pub struct FileSink {
    path: PathBuf,
    file: Mutex<tokio::fs::File>,
}

impl FileSink {
    pub async fn new(path: impl AsRef<Path>) -> Result<Arc<Self>> {
        let path: PathBuf = path.as_ref().to_path_buf();
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
            file: Mutex::new(file),
        }))
    }

    pub async fn from_uri(uri: &str) -> Result<Arc<Self>> {
        let path = uri
            .strip_prefix("file://")
            .ok_or_else(|| anyhow::anyhow!("invalid file URI: {uri}"))?;
        Self::new(PathBuf::from(path)).await
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[async_trait]
impl Sink for FileSink {
    async fn write(&self, req: SinkWrite) -> Result<()> {
        let mut f = self.file.lock().await;
        f.write_all(&req.payload).await?;
        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        let f = self.file.lock().await;
        f.sync_data().await?;
        Ok(())
    }
}
