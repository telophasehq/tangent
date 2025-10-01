use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use flate2::write::GzEncoder;
use flate2::Compression as f2Compression;
use std::cmp::max;
use std::fs::metadata;
use std::fs::File as stdFile;
use std::io::copy;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tangent_shared::Compression;
use tokio::fs;
use tokio::fs::File;
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::{spawn_blocking, JoinHandle, JoinSet};
use tokio::time::{sleep, Duration, Instant};
use tokio_util::sync::CancellationToken;
use ulid::Ulid;

use crate::sinks::manager::Sink;
use crate::SINK_BYTES_UNCOMPRESSED_TOTAL;
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
    compression: Compression,
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
        compression: Compression,
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
            compression: compression,
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
        let current_path = { self.cur.lock().await.path.clone() };

        let mut rd = match tokio::fs::read_dir(&self.dir).await {
            Ok(r) => r,
            Err(_) => return,
        };

        while let Some(ent) = rd.next_entry().await.ok().flatten() {
            let p = ent.path();
            if !is_ready_wal_file(&p, &current_path).await {
                continue;
            }

            if let Ok(md) = tokio::fs::metadata(&p).await {
                let size = md.len();
                self.spawn_upload(p, size).await;
            }
        }
    }

    async fn rotate(&self) -> Result<()> {
        // Important: Do not hold the cur lock while uploading
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

    async fn spawn_upload(&self, path: PathBuf, orig_size: u64) {
        let permit = self.max_inflight.clone().acquire_owned().await.unwrap();
        self.inflight.fetch_add(1, Ordering::AcqRel);
        let inner = self.inner.clone();
        let inflight = self.inflight.clone();
        let compression = self.compression.clone();
        let fut = async move {
            let path_cl = path.clone();
            let res = async move {
                let (upload_path, upload_size) = match compression {
                    Compression::None => (path_cl.clone(), orig_size),
                    Compression::Gzip { level } => compress_gzip_to_file(&path_cl, level).await?,
                    Compression::Zstd { level } => compress_zstd_to_file(&path_cl, level).await?,
                };

                inner.write_path(&path_cl).await?;

                let _ = tokio::fs::remove_file(&upload_path).await;
                let _ = tokio::fs::remove_file(&path_cl).await;

                anyhow::Ok(upload_size)
            }
            .await;

            drop(permit);
            match res {
                Ok(uploaded) => {
                    SINK_OBJECTS_TOTAL.inc();
                    SINK_BYTES_TOTAL.inc_by(uploaded);
                    SINK_BYTES_UNCOMPRESSED_TOTAL.inc_by(orig_size);
                    WAL_PENDING_FILES.dec();
                    WAL_PENDING_BYTES.sub(orig_size as i64);
                    tracing::debug!(bytes = uploaded, "WAL uploaded & removed");
                }
                Err(e) => {
                    tracing::warn!("upload error for {:?}: {e}", path);
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
                    let p = ent.path();
                    if is_ready_wal_file(&p, &current_path).await {
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

async fn compress_zstd_to_file(src: &Path, level: i32) -> Result<(PathBuf, u64)> {
    let dst = src.with_extension("sealed.zst");
    let src = src.to_path_buf();
    let dst2 = dst.clone();
    let size = spawn_blocking(move || -> Result<u64> {
        let mut fin = stdFile::open(&src)?;
        let mut fout = stdFile::create(&dst2)?;
        let mut enc = zstd::stream::Encoder::new(&mut fout, level)?;
        copy(&mut fin, &mut enc)?;
        enc.finish()?;
        Ok(metadata(&dst2)?.len())
    })
    .await??;
    Ok((dst, size))
}

async fn compress_gzip_to_file(src: &Path, level: u32) -> Result<(PathBuf, u64)> {
    let dst = src.with_extension("sealed.gz");
    let src = src.to_path_buf();
    let dst2 = dst.clone();
    let size = spawn_blocking(move || -> Result<u64> {
        let mut fin = stdFile::open(&src)?;
        let mut fout = stdFile::create(&dst2)?;
        let mut enc = GzEncoder::new(&mut fout, f2Compression::new(level));
        copy(&mut fin, &mut enc)?;
        enc.finish()?;
        Ok(metadata(&dst2)?.len())
    })
    .await??;
    Ok((dst, size))
}

#[inline]
async fn is_ready_wal_file(path: &Path, current_path: &Path) -> bool {
    if path == current_path {
        return false;
    }
    if !fs::metadata(path)
        .await
        .map(|m| m.is_file())
        .unwrap_or(false)
    {
        return false;
    }

    let ext = path.extension().and_then(|e| e.to_str());
    let stem = path.file_stem().and_then(|s| s.to_str());

    match (ext, stem) {
        (Some("zst") | Some("gz"), Some(st)) if st.ends_with(".sealed") => true,

        (Some("sealed"), _) => {
            let fname = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            let zst = path.with_file_name(format!("{fname}.zst"));
            let gz = path.with_file_name(format!("{fname}.gz"));
            let has_comp = fs::try_exists(&zst).await.unwrap_or(false)
                || fs::try_exists(&gz).await.unwrap_or(false);
            !has_comp
        }

        _ => false,
    }
}
