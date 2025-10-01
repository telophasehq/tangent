use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicUsize, Ordering, Ordering::Acquire},
    Arc,
};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, Semaphore};
use ulid::Ulid;

use crate::sinks::manager::Sink;
use crate::{
    SINK_BYTES_TOTAL, SINK_OBJECTS_TOTAL, WAL_PENDING_BYTES, WAL_PENDING_FILES,
    WAL_SEALED_BYTES_TOTAL, WAL_SEALED_FILES_TOTAL,
};

pub struct DurableFileSink {
    inner: Arc<dyn WALSink>,
    dir: PathBuf,
    inflight: Arc<AtomicUsize>,
    max_inflight: Arc<Semaphore>,
    max_file_size: usize,
    cur: Mutex<Current>,
}

struct Current {
    path: PathBuf,
    file: Option<File>,
    bytes: usize,
}

#[async_trait]
pub trait WALSink: Send + Sync {
    async fn write_path(&self, path: &Path) -> Result<()>;
}

impl DurableFileSink {
    pub async fn new(
        inner: Arc<dyn WALSink>,
        dir: impl AsRef<Path>,
        max_inflight: usize,
        max_file_size: usize,
    ) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        tokio::fs::create_dir_all(&dir).await?;

        let path = make_path(&dir);
        let file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .await?;

        let s = Self {
            inner,
            dir,
            inflight: Default::default(),
            max_inflight: Arc::new(tokio::sync::Semaphore::new(max_inflight)),
            max_file_size: max_file_size.into(),
            cur: Mutex::new(Current {
                path,
                file: Some(file),
                bytes: 0,
            }),
        };
        s.retry_leftovers().await;
        Ok(s)
    }

    async fn retry_leftovers(&self) {
        let mut rd = match tokio::fs::read_dir(&self.dir).await {
            Ok(r) => r,
            Err(_) => return,
        };

        while let Some(ent) = rd.next_entry().await.ok().flatten() {
            let path = ent.path();
            let is_current = {
                let cur = self.cur.lock().await;
                path == cur.path
            };
            if is_current {
                continue;
            }

            match tokio::fs::metadata(&path).await {
                Ok(md) => {
                    let size = md.len();

                    WAL_PENDING_FILES.inc();
                    WAL_PENDING_BYTES.add(size as i64);
                    self.spawn_upload(path, size).await;
                }
                Err(e) => {
                    tracing::warn!("leftover stat failed for {:?}: {e}", path);
                }
            }
        }
    }

    async fn rotate_locked(&self, cur: &mut Current) -> Result<()> {
        tracing::warn!("rotating cur");
        if let Some(f) = cur.file.take() {
            f.sync_data().await?;
            drop(f);
        }
        let sealed = cur.path.clone();
        let sealed_bytes = cur.bytes as u64;

        WAL_SEALED_FILES_TOTAL.inc();
        WAL_SEALED_BYTES_TOTAL.inc_by(sealed_bytes);
        WAL_PENDING_FILES.inc();
        WAL_PENDING_BYTES.add(sealed_bytes as i64);

        self.spawn_upload(sealed, sealed_bytes).await;

        let new_path = make_path(&self.dir);
        let newf = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&new_path)
            .await?;
        cur.path = new_path;
        cur.file = Some(newf);
        cur.bytes = 0;
        Ok(())
    }

    async fn spawn_upload(&self, path: PathBuf, size: u64) {
        let permit = self.max_inflight.clone().acquire_owned().await.unwrap();
        self.inflight.fetch_add(1, Ordering::AcqRel);
        let inner = self.inner.clone();
        let inflight = self.inflight.clone();
        tokio::spawn(async move {
            let res = async {
                tracing::warn!("calling s3 write");

                // TODO: use streaming where possible.
                inner.write_path(&path).await?;
                tracing::warn!("called s3 write");
                tokio::fs::remove_file(&path).await?;
                anyhow::Ok(())
            }
            .await;

            drop(permit);
            match res {
                Ok(()) => {
                    SINK_OBJECTS_TOTAL.inc();
                    SINK_BYTES_TOTAL.inc_by(size);
                    WAL_PENDING_FILES.dec();
                    WAL_PENDING_BYTES.sub(size as i64);
                    tracing::info!(uploaded=%path.display(), bytes=size, "WAL uploaded & removed");
                }
                Err(e) => {
                    tracing::warn!("upload retryable error for {:?}: {e}", path);
                }
            }
            inflight.fetch_sub(1, Ordering::AcqRel);
        });
    }
}

#[async_trait::async_trait]
impl Sink for DurableFileSink {
    async fn write(&self, payload: Bytes) -> Result<()> {
        if payload.is_empty() {
            tracing::error!(target = "wal", "WAL got EMPTY payload");
        }
        let mut cur = self.cur.lock().await;

        if cur.bytes + payload.len() > self.max_file_size {
            self.rotate_locked(&mut cur).await?;
        }

        let f = cur.file.as_mut().expect("current file missing");
        f.write_all(payload.as_ref()).await?;
        f.sync_data().await?;
        cur.bytes += payload.len();

        if cur.bytes >= self.max_file_size {
            self.rotate_locked(&mut cur).await?;
        }

        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        {
            let mut cur = self.cur.lock().await;
            if cur.bytes > 0 {
                self.rotate_locked(&mut cur).await?;
            } else if let Some(f) = cur.file.as_mut() {
                f.sync_data().await?;
            }
        }

        loop {
            let mut rd = tokio::fs::read_dir(&self.dir).await?;
            let mut any_sealed = false;
            while let Some(ent) = rd.next_entry().await? {
                let p = ent.path();
                let is_current = {
                    let cur = self.cur.lock().await;
                    p == cur.path
                };
                if !is_current {
                    any_sealed = true;
                    break;
                }
            }
            let inflight = self.inflight.load(Acquire);
            if !any_sealed && inflight == 0 {
                break;
            }
            if any_sealed {
                tracing::warn!("retrying leftovers");
                self.retry_leftovers().await;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        Ok(())
    }
}

fn make_path(dir: &PathBuf) -> std::path::PathBuf {
    let ulid = Ulid::new();
    let name = format!("{}.bin", ulid.to_string());
    dir.join(name)
}
