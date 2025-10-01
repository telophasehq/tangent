use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use std::cmp::max;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{sleep, Duration, Instant};
use tokio_util::sync::CancellationToken;
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
    max_file_age: Duration,
    cur: Mutex<Current>,
    rotator: Mutex<Option<JoinHandle<()>>>,
    uploads: tokio::sync::Mutex<JoinSet<()>>,
}

struct Current {
    path: PathBuf,
    file: Option<File>,
    bytes: usize,
    created_at: Instant,
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
        max_file_age: Duration,
        cancel: CancellationToken,
    ) -> Result<Arc<Self>> {
        let dir = dir.as_ref().to_path_buf();
        tokio::fs::create_dir_all(&dir).await?;

        let path = make_path(&dir);
        let file = tokio::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .await?;

        let s = Arc::new(Self {
            inner,
            dir,
            inflight: Default::default(),
            max_inflight: Arc::new(Semaphore::new(max_inflight)),
            max_file_size: max_file_size.into(),
            max_file_age: max_file_age,
            cur: Mutex::new(Current {
                path,
                file: Some(file),
                bytes: 0,
                created_at: Instant::now(),
            }),
            rotator: Mutex::new(None),
            uploads: Mutex::new(JoinSet::new()),
        });
        s.retry_leftovers().await;

        let s_cloned = s.clone();
        let handle = tokio::spawn(async move {
            let tick = max(Duration::from_millis(250), max_file_age / 4);
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = sleep(tick) => {
                        let should_rotate = { let cur = s_cloned.cur.lock().await; cur.bytes > 0 && cur.created_at.elapsed() >= s_cloned.max_file_age };
                        if should_rotate { s_cloned.rotate().await.ok(); }
                    }
                }
            }
        });

        *s.rotator.lock().await = Some(handle);
        Ok(s)
    }

    async fn retry_leftovers(&self) {
        let mut rd = match tokio::fs::read_dir(&self.dir).await {
            Ok(r) => r,
            Err(_) => return,
        };
        while let Some(ent) = rd.next_entry().await.ok().flatten() {
            if !ent.file_type().await.map(|t| t.is_file()).unwrap_or(false) {
                continue;
            }
            let path = ent.path();

            let is_current = {
                let cur = self.cur.lock().await;
                path == cur.path
            };
            if is_current {
                continue;
            }

            if path.extension().and_then(|e| e.to_str()) != Some("sealed") {
                continue;
            }

            if let Ok(md) = tokio::fs::metadata(&path).await {
                let size = md.len();
                self.spawn_upload(path, size).await;
            }
        }
    }

    async fn rotate(&self) -> Result<()> {
        // Do not hold the cur lock while uploading
        let (sealed_ready, sealed_bytes) = {
            let mut cur = self.cur.lock().await;
            if cur.bytes == 0 {
                return Ok(());
            }

            if let Some(f) = cur.file.take() {
                f.sync_data().await?;
            }

            let sealed = cur.path.clone();
            let sealed_bytes = cur.bytes as u64;

            let sealed_ready = sealed.with_extension("sealed");
            tokio::fs::rename(&sealed, &sealed_ready).await?;

            let new_path = make_path(&self.dir);
            let newf = tokio::fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(&new_path)
                .await?;
            cur.path = new_path;
            cur.file = Some(newf);
            cur.bytes = 0;
            cur.created_at = Instant::now();

            WAL_SEALED_FILES_TOTAL.inc();
            WAL_SEALED_BYTES_TOTAL.inc_by(sealed_bytes);
            WAL_PENDING_FILES.inc();
            WAL_PENDING_BYTES.add(sealed_bytes as i64);

            (sealed_ready, sealed_bytes)
        };

        self.spawn_upload(sealed_ready, sealed_bytes).await;
        Ok(())
    }

    async fn spawn_upload(&self, path: PathBuf, size: u64) {
        let permit = self.max_inflight.clone().acquire_owned().await.unwrap();
        self.inflight.fetch_add(1, Ordering::AcqRel);
        let inner = self.inner.clone();
        let inflight = self.inflight.clone();
        let fut = async move {
            let res = async {
                inner.write_path(&path).await?;
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
                    tracing::debug!(uploaded=%path.display(), bytes=size, "WAL uploaded & removed");
                }
                Err(e) => {
                    tracing::warn!("upload retryable error for {:?}: {e}", path);
                }
            }
            inflight.fetch_sub(1, Ordering::AcqRel);
        };

        let mut js = self.uploads.lock().await;
        js.spawn(fut);
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
            drop(cur);
            self.rotate().await?;
            cur = self.cur.lock().await;
        }

        let f = cur.file.as_mut().expect("current file missing");
        f.write_all(payload.as_ref()).await?;
        cur.bytes += payload.len();

        if cur.bytes >= self.max_file_size {
            drop(cur);
            self.rotate().await?;
        }

        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        {
            let need_rotate = {
                let cur = self.cur.lock().await;
                cur.bytes > 0
            };
            if need_rotate {
                self.rotate().await?;
            }
        }

        loop {
            self.retry_leftovers().await;
            let mut js = {
                let mut g = self.uploads.lock().await;
                std::mem::take(&mut *g)
            };
            while js.join_next().await.is_some() {}
            *self.uploads.lock().await = js;

            let current_path = { self.cur.lock().await.path.clone() };
            let any_ready = {
                let mut rd = tokio::fs::read_dir(&self.dir).await?;
                let mut found = false;
                while let Some(ent) = rd.next_entry().await? {
                    if !ent.file_type().await.map(|t| t.is_file()).unwrap_or(false) {
                        continue;
                    }
                    let p = ent.path();
                    if p != current_path && p.extension().and_then(|e| e.to_str()) == Some("sealed")
                    {
                        found = true;
                        break;
                    }
                }
                found
            };
            if !any_ready && self.inflight.load(Ordering::Acquire) == 0 {
                break;
            }
        }

        Ok(())
    }
}

fn make_path(dir: &Path) -> std::path::PathBuf {
    let ulid = Ulid::new();
    let name = format!("{}.bin", ulid.to_string());
    dir.join(name)
}
