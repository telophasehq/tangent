use anyhow::Result;
use async_trait::async_trait;
use flate2::write::GzEncoder;
use flate2::Compression as f2Compression;
use std::cmp::max;
use std::collections::HashMap;
use std::fs::File as stdFile;
use std::io::copy;
use std::path::{Path, PathBuf};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use tangent_shared::sinks::common::{Compression, Encoding};
use tokio::fs::{self, File};
use tokio::io::AsyncWriteExt;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::{spawn_blocking, JoinHandle, JoinSet};
use tokio::time::{sleep, Duration, Instant};

use crate::sinks::manager::{Sink, SinkWrite};
use crate::sinks::s3;
use crate::SINK_BYTES_UNCOMPRESSED_TOTAL;
use crate::{
    SINK_BYTES_TOTAL, SINK_OBJECTS_TOTAL, WAL_PENDING_BYTES, WAL_PENDING_FILES,
    WAL_SEALED_BYTES_TOTAL, WAL_SEALED_FILES_TOTAL,
};

pub struct DurableFileSink {
    inner: Arc<dyn WALSink>,
    dir: PathBuf,
    routes: Mutex<HashMap<RouteKey, RouteState>>,
    inflight: Arc<AtomicUsize>,
    max_inflight: Arc<Semaphore>,
    max_file_size: usize,
    max_file_age: Duration,
    compression: Compression,
    encoding: Encoding,
    rotator: Mutex<Option<JoinHandle<()>>>,
    uploads: tokio::sync::Mutex<JoinSet<()>>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct WalMeta {
    bucket_name: String,
    key_prefix: Option<String>,

    encoding: Encoding,
    compression: Compression,
}

#[derive(Hash, Eq, PartialEq, Clone)]
pub struct RouteKey {
    pub sink_name: String,
    pub prefix: Option<String>,
}

struct RouteState {
    cur: Current,
    meta: s3::S3SinkItem,
    last_used: Instant,
}

struct Current {
    path: PathBuf,
    file: Option<File>,
    bytes: usize,
    created_at: Instant,
}

#[async_trait]
pub trait WALSink: Send + Sync {
    async fn write_path_with(
        &self,
        path: &Path,
        encoding: &Encoding,
        compression: &Compression,
        meta: &s3::S3SinkItem,
    ) -> Result<()>;
}

impl DurableFileSink {
    pub async fn new(
        inner: Arc<dyn WALSink>,
        dir: impl AsRef<Path>,
        max_inflight: usize,
        max_file_size: usize,
        max_file_age: Duration,
        compression: Compression,
        encoding: Encoding,
    ) -> Result<Arc<Self>> {
        let dir = dir.as_ref().to_path_buf();
        tokio::fs::create_dir_all(&dir).await?;

        let s = Arc::new(Self {
            inner,
            dir,
            inflight: Arc::default(),
            routes: Mutex::new(HashMap::new()),
            max_inflight: Arc::new(Semaphore::new(max_inflight)),
            max_file_size,
            max_file_age,
            compression,
            encoding,
            rotator: Mutex::new(None),
            uploads: Mutex::new(JoinSet::new()),
        });
        s.retry_leftovers(false).await;

        let s_cloned = s.clone();
        let handle = tokio::spawn(async move {
            let tick = max(Duration::from_millis(250), max_file_age / 4);
            loop {
                tokio::select! {
                    () = sleep(tick) => {
                        let to_rotate: Vec<RouteKey> = {
                            let routes = s_cloned.routes.lock().await;
                            routes.iter()
                                .filter(|(_, rs)| rs.cur.bytes > 0 && rs.cur.created_at.elapsed() >= s_cloned.max_file_age)
                                .map(|(k, _)| k.clone())
                                .collect()
                        };
                        for k in to_rotate {
                            let _ = s_cloned.rotate_route(k).await;
                        }
                    }
                }
            }
        });

        *s.rotator.lock().await = Some(handle);
        Ok(s)
    }

    async fn rotate_route(&self, rkey: RouteKey) -> anyhow::Result<()> {
        let (sealed_ready, sealed_bytes, meta) = {
            let mut routes = self.routes.lock().await;
            let rs = routes
                .get_mut(&rkey)
                .ok_or_else(|| anyhow::anyhow!("route missing"))?;
            if rs.cur.bytes == 0 {
                return Ok(());
            }

            if let Some(f) = rs.cur.file.take() {
                f.sync_data().await?;
            }

            let mut sealed = rs.cur.path.clone();
            sealed.set_extension("bin.sealed");
            fs::rename(&rs.cur.path, &sealed).await?;
            let sealed_bytes = rs.cur.bytes as u64;

            rs.cur = open_route_current(
                &self.dir,
                &WalMeta {
                    bucket_name: rs.meta.bucket_name.clone(),
                    key_prefix: rs.meta.key_prefix.clone(),
                    encoding: self.encoding.clone(),
                    compression: self.compression.clone(),
                },
            )
            .await?;

            (sealed, sealed_bytes, rs.meta.clone())
        };

        WAL_SEALED_FILES_TOTAL.inc();
        WAL_SEALED_BYTES_TOTAL.inc_by(sealed_bytes);
        WAL_PENDING_FILES.inc();
        WAL_PENDING_BYTES.add(sealed_bytes as i64);

        self.spawn_upload_with_meta(sealed_ready, sealed_bytes, meta, true)
            .await;
        Ok(())
    }

    async fn retry_leftovers(&self, incr_counters: bool) {
        let Ok(mut rd) = fs::read_dir(&self.dir).await else {
            return;
        };
        while let Ok(Some(ent)) = rd.next_entry().await {
            let p = ent.path();
            let name = match ent.file_name().into_string() {
                Ok(s) => s,
                Err(_) => continue,
            };
            if !is_sealed_file_name(&name) {
                continue;
            }

            let meta_path = meta_path_for(&p);
            let meta = match read_meta(&meta_path).await {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!("missing/corrupt meta for {:?}: {e}", p);
                    let _ = fs::remove_file(&p).await;
                    continue;
                }
            };

            if let Ok(md) = fs::metadata(&p).await {
                self.spawn_upload_with_meta(
                    p.clone(),
                    md.len(),
                    s3::S3SinkItem {
                        bucket_name: meta.bucket_name,
                        key_prefix: meta.key_prefix,
                    },
                    incr_counters,
                )
                .await;
            }
        }
    }

    async fn spawn_upload_with_meta(
        &self,
        sealed_path: PathBuf,
        orig_size: u64,
        route_meta: s3::S3SinkItem,
        incr_metrics: bool,
    ) {
        let permit = self.max_inflight.clone().acquire_owned().await.unwrap();
        self.inflight.fetch_add(1, Ordering::AcqRel);

        let inner = self.inner.clone();
        let inflight = self.inflight.clone();
        let compression = self.compression.clone();
        let encoding = self.encoding.clone();
        let sealed_path_clone = sealed_path.clone();

        let fut = async move {
            let _permit = permit;

            let meta_path = meta_path_for(&sealed_path_clone);
            let wal_meta = read_meta(&meta_path).await.unwrap_or_else(|_| WalMeta {
                bucket_name: route_meta.bucket_name.clone(),
                key_prefix: route_meta.key_prefix.clone(),
                encoding: encoding.clone(),
                compression: compression.clone(),
            });

            let (upload_path, upload_size) = match compression {
                Compression::None => (sealed_path_clone.clone(), orig_size),
                Compression::Gzip { level } => {
                    compress_gzip_to_file(&sealed_path_clone, level).await?
                }
                Compression::Zstd { level } => {
                    compress_zstd_to_file(&sealed_path_clone, level).await?
                }
            };

            inner
                .write_path_with(
                    &upload_path,
                    &wal_meta.encoding,
                    &wal_meta.compression,
                    &s3::S3SinkItem {
                        bucket_name: wal_meta.bucket_name,
                        key_prefix: wal_meta.key_prefix,
                    },
                )
                .await?;

            let _ = fs::remove_file(&upload_path).await;
            let _ = fs::remove_file(&sealed_path_clone).await;
            let _ = fs::remove_file(&meta_path).await;

            Ok::<u64, anyhow::Error>(upload_size)
        };

        let mut js = self.uploads.lock().await;

        js.spawn(async move {
            match fut.await {
                Ok(uploaded) => {
                    // Don't incr metrics on restart.
                    if incr_metrics {
                        SINK_OBJECTS_TOTAL.inc();
                        SINK_BYTES_TOTAL.inc_by(uploaded);
                        SINK_BYTES_UNCOMPRESSED_TOTAL.inc_by(orig_size);
                        WAL_PENDING_FILES.dec();
                        WAL_PENDING_BYTES.sub(orig_size as i64);
                    }
                    tracing::debug!(bytes = uploaded, "WAL uploaded & removed");
                }
                Err(e) => {
                    tracing::warn!("upload error for {:?}: {e}", sealed_path);
                }
            }
            inflight.fetch_sub(1, Ordering::AcqRel);
        });
    }
}

#[async_trait::async_trait]
impl Sink for DurableFileSink {
    async fn write(&self, req: SinkWrite) -> Result<()> {
        let Some(meta) = req.s3 else {
            anyhow::bail!("DurableFileSink requires s3 meta (bucket/prefix)")
        };
        let rkey = RouteKey {
            sink_name: req.sink_name,
            prefix: meta.key_prefix.clone(),
        };

        let mut need_create = false;
        {
            let routes = self.routes.lock().await;
            if !routes.contains_key(&rkey) {
                need_create = true;
            }
        }

        if need_create {
            let cur = open_route_current(
                &self.dir,
                &WalMeta {
                    bucket_name: meta.bucket_name.clone(),
                    key_prefix: meta.key_prefix.clone(),
                    encoding: self.encoding.clone(),
                    compression: self.compression.clone(),
                },
            )
            .await?;
            {
                let mut routes = self.routes.lock().await;
                if !routes.contains_key(&rkey) {
                    routes.insert(
                        rkey.clone(),
                        RouteState {
                            cur,
                            meta: meta.clone(),
                            last_used: Instant::now(),
                        },
                    );
                    // TODO: enforce self.max_open_routes
                }
            }
        }

        loop {
            let mut routes = self.routes.lock().await;
            let rs = routes.get_mut(&rkey).expect("route exists after create");
            if rs.cur.bytes + req.payload.len() <= self.max_file_size {
                let f = rs.cur.file.as_mut().expect("current file missing");
                f.write_all(&req.payload).await?;
                rs.cur.bytes += req.payload.len();
                rs.last_used = Instant::now();
                break;
            }
            drop(routes);
            self.rotate_route(rkey.clone()).await?;
        }

        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        let value = self.rotator.lock().await.take();
        if let Some(h) = value {
            let _ = h.await;
        }

        let keys: Vec<RouteKey> = {
            let routes = self.routes.lock().await;
            routes
                .iter()
                .filter(|(_, rs)| rs.cur.bytes > 0)
                .map(|(k, _)| k.clone())
                .collect()
        };
        for k in keys {
            let _ = self.rotate_route(k).await;
        }

        loop {
            self.retry_leftovers(true).await;
            let mut js = {
                let mut g = self.uploads.lock().await;
                std::mem::take(&mut *g)
            };

            while let Some(res) = js.join_next().await {
                if let Err(e) = res {
                    tracing::warn!("upload task join error: {e}");
                }
            }
            *self.uploads.lock().await = js;

            if self.inflight.load(Ordering::Acquire) == 0 {
                break;
            }
        }
        Ok(())
    }
}

async fn compress_zstd_to_file(src: &Path, level: i32) -> Result<(PathBuf, u64)> {
    let dst = src.with_extension("sealed.zst");
    let dst_tmp = dst.with_extension("sealed.zst.tmp");
    let src = src.to_path_buf();
    let dst_clone = dst.clone();
    let size = spawn_blocking(move || -> Result<u64> {
        let mut fin = stdFile::open(&src)?;
        let mut fout = stdFile::create(&dst_tmp)?;
        let mut enc = zstd::stream::Encoder::new(&mut fout, level)?;
        copy(&mut fin, &mut enc)?;
        enc.finish()?;

        std::fs::rename(&dst_tmp, &dst_clone)?;
        Ok(std::fs::metadata(&dst_clone)?.len())
    })
    .await??;
    Ok((dst, size))
}

async fn compress_gzip_to_file(src: &Path, level: u32) -> Result<(PathBuf, u64)> {
    let dst = src.with_extension("sealed.gz");
    let dst_tmp = dst.with_extension("sealed.gz.tmp");
    let src = src.to_path_buf();
    let dst_clone = dst.clone();
    let size = spawn_blocking(move || -> Result<u64> {
        let mut fin = stdFile::open(&src)?;
        let mut fout = stdFile::create(&dst_tmp)?;
        let mut enc = GzEncoder::new(&mut fout, f2Compression::new(level));
        copy(&mut fin, &mut enc)?;
        enc.finish()?;

        std::fs::rename(&dst_tmp, &dst_clone)?;
        Ok(std::fs::metadata(&dst_clone)?.len())
    })
    .await??;
    Ok((dst, size))
}

#[must_use]
pub fn base_for(path: &Path) -> PathBuf {
    let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
        return path.to_path_buf();
    };
    let mut out = name.to_owned();

    if out.ends_with(".gz") || out.ends_with(".zst") {
        if let Some(idx) = out.rfind('.') {
            out.truncate(idx);
        }
    }

    if out.ends_with(".sealed") {
        let new_len = out.len() - ".sealed".len();
        out.truncate(new_len);
    }

    if out.ends_with(".bin") {
        let new_len = out.len() - ".bin".len();
        out.truncate(new_len);
    }

    path.with_file_name(out)
}

fn meta_path_for(any_stage_path: &Path) -> PathBuf {
    let mut b = base_for(any_stage_path);
    b.set_extension("meta");
    b
}

async fn write_meta_atomic(meta_path: &Path, meta: &WalMeta) -> anyhow::Result<()> {
    let tmp = meta_path.with_extension("meta.tmp");
    let mut f = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&tmp)
        .await?;
    let json = serde_json::to_vec(meta)?;
    f.write_all(&json).await?;
    f.sync_data().await?;
    drop(f);
    fs::rename(&tmp, meta_path).await?;
    Ok(())
}

async fn read_meta(meta_path: &Path) -> anyhow::Result<WalMeta> {
    let bytes = fs::read(meta_path).await?;
    Ok(serde_json::from_slice(&bytes)?)
}

fn is_sealed_file_name(name: &str) -> bool {
    name.ends_with(".bin.sealed")
        || name.ends_with(".bin.sealed.gz")
        || name.ends_with(".bin.sealed.zst")
}

fn make_base_ulid(dir: &Path) -> PathBuf {
    dir.join(format!("{}.bin", ulid::Ulid::new()))
        .with_extension("")
}

fn bin_path_from_base(base: &Path) -> PathBuf {
    let mut p = base.to_path_buf();
    p.set_extension("bin");
    p
}

async fn open_route_current(dir: &Path, meta: &WalMeta) -> anyhow::Result<Current> {
    let base = make_base_ulid(dir);
    let meta_path = {
        let mut p = base.clone();
        p.set_extension("meta");
        p
    };
    write_meta_atomic(&meta_path, meta).await?;

    let bin_path = bin_path_from_base(&base);
    let file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&bin_path)
        .await?;

    Ok(Current {
        path: bin_path,
        file: Some(file),
        bytes: 0,
        created_at: Instant::now(),
    })
}
