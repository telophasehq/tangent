use anyhow::Result;
use async_trait::async_trait;
use bytes::BytesMut;
use flate2::write::GzEncoder;
use flate2::Compression as f2Compression;
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::fs::File as stdFile;
use std::io::{copy, BufReader, BufWriter, Write};
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

use crate::sinks::encoding;
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
    /// Sealed files being sent now: a retry passes them by.
    sending: Arc<parking_lot::Mutex<HashSet<PathBuf>>>,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
struct WalMeta {
    bucket_name: Arc<str>,
    key_prefix: Option<Arc<str>>,

    encoding: Encoding,
    compression: Compression,
}

#[derive(Hash, Eq, PartialEq, Clone)]
pub struct RouteKey {
    pub sink_name: Arc<str>,
    pub prefix: Option<Arc<str>>,
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

/// Where a sealed file goes once it is in the sink's encoding: `path` holds
/// the object's bytes, `name` is what to call it (see [`object_name`]).
#[async_trait]
pub trait WALSink: Send + Sync {
    async fn write_path_with(
        &self,
        path: &Path,
        name: &str,
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
            sending: Arc::default(),
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
        // flush retries what is sealed, which includes what it just sealed
        if !self.sending.lock().insert(sealed_path.clone()) {
            return;
        }
        let permit = self.max_inflight.clone().acquire_owned().await.unwrap();
        self.inflight.fetch_add(1, Ordering::AcqRel);

        let inner = self.inner.clone();
        let inflight = self.inflight.clone();
        let sending = self.sending.clone();
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

            let name = object_name(
                &sealed_path_clone,
                &wal_meta.encoding,
                &wal_meta.compression,
            );
            let (upload_path, upload_size) = encode_sealed(
                &sealed_path_clone,
                orig_size,
                &name,
                &wal_meta.encoding,
                &wal_meta.compression,
            )
            .await?;

            inner
                .write_path_with(
                    &upload_path,
                    &name,
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
            sending.lock().remove(&sealed_path);
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
        // The rotator loops for as long as the sink lives: stop it, then seal
        // what is left below.
        let value = self.rotator.lock().await.take();
        if let Some(h) = value {
            h.abort();
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

/// The sealed NDJSON as the object it goes out as, beside it under the
/// object's name: the sealed file itself for uncompressed NDJSON, a
/// compressed copy for NDJSON with gzip or zstd, and otherwise the records
/// encoded, Parquet a batch at a time, JSON and Avro all at once.
async fn encode_sealed(
    sealed: &Path,
    size: u64,
    name: &str,
    enc: &Encoding,
    comp: &Compression,
) -> Result<(PathBuf, u64)> {
    let dst = sealed.with_file_name(name);
    match (enc, comp) {
        (Encoding::NDJSON, Compression::Gzip { level }) => {
            compress_gzip_to_file(sealed, &dst, *level).await
        }
        (Encoding::NDJSON, Compression::Zstd { level }) => {
            compress_zstd_to_file(sealed, &dst, *level).await
        }
        (Encoding::NDJSON, _) => Ok((sealed.to_path_buf(), size)),
        _ => encode_to_file(sealed, &dst, enc, comp).await,
    }
}

async fn encode_to_file(
    src: &Path,
    dst: &Path,
    enc: &Encoding,
    comp: &Compression,
) -> Result<(PathBuf, u64)> {
    let (src, dst) = (src.to_path_buf(), dst.to_path_buf());
    let (enc, comp) = (enc.clone(), comp.clone());
    spawn_blocking(move || -> Result<(PathBuf, u64)> {
        let tmp = tmp_path(&dst);
        let mut out = BufWriter::new(stdFile::create(&tmp)?);
        if let Encoding::Parquet { schema } = &enc {
            let fin = BufReader::new(stdFile::open(&src)?);
            out = encoding::write_parquet(fin, out, schema, &comp)?;
        } else {
            let raw = BytesMut::from(std::fs::read(&src)?.as_slice());
            let bytes = encoding::normalize_from_ndjson(&enc, &comp, raw)?;
            // JSON is compressed whole, as NDJSON is; Avro inside the file
            match (&enc, &comp) {
                (Encoding::JSON, Compression::Gzip { level }) => {
                    let mut gz = GzEncoder::new(&mut out, f2Compression::new(*level));
                    gz.write_all(&bytes)?;
                    gz.finish()?;
                }
                (Encoding::JSON, Compression::Zstd { level }) => {
                    zstd::stream::copy_encode(&bytes[..], &mut out, *level)?;
                }
                _ => out.write_all(&bytes)?,
            }
        }
        out.into_inner().map_err(|e| e.into_error())?.sync_data()?;
        std::fs::rename(&tmp, &dst)?;
        let size = std::fs::metadata(&dst)?.len();
        Ok((dst, size))
    })
    .await?
}

/// Where a stage is written before it is renamed into place.
fn tmp_path(dst: &Path) -> PathBuf {
    let mut tmp = dst.as_os_str().to_owned();
    tmp.push(".tmp");
    tmp.into()
}

/// Whether the encoding is compressed whole, by a compressor around the file
/// (NDJSON, JSON), rather than inside it (Parquet, Avro).
pub fn compressed_whole(enc: &Encoding) -> bool {
    matches!(enc, Encoding::NDJSON | Encoding::JSON)
}

/// What a sealed file is called once it is out: its ULID, which sorts by the
/// time the file was opened and is unique, the encoding's extension, and
/// for an encoding compressed whole, the compression's.
pub fn object_name(sealed: &Path, enc: &Encoding, comp: &Compression) -> String {
    let base = base_for(sealed);
    let stem = base.file_name().unwrap_or_default().to_string_lossy();
    let mut name = format!("{stem}.{}", enc.extension());
    if compressed_whole(enc) {
        name.push_str(comp.extension());
    }
    name
}

async fn compress_zstd_to_file(src: &Path, dst: &Path, level: i32) -> Result<(PathBuf, u64)> {
    let dst = dst.to_path_buf();
    let dst_tmp = tmp_path(&dst);
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

async fn compress_gzip_to_file(src: &Path, dst: &Path, level: u32) -> Result<(PathBuf, u64)> {
    let dst = dst.to_path_buf();
    let dst_tmp = tmp_path(&dst);
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

/// A sealed WAL file. The compressed copy made from one on its way out is
/// not: it is made again from the sealed file, which stays until it is sent.
fn is_sealed_file_name(name: &str) -> bool {
    name.ends_with(".bin.sealed")
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

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    /// What reached the inner sink: each file's name and bytes.
    #[derive(Default)]
    struct Recorder {
        got: Mutex<Vec<(String, Vec<u8>)>>,
    }

    #[async_trait]
    impl WALSink for Recorder {
        async fn write_path_with(
            &self,
            path: &Path,
            name: &str,
            _encoding: &Encoding,
            _compression: &Compression,
            _meta: &s3::S3SinkItem,
        ) -> Result<()> {
            let bytes = fs::read(path).await?;
            self.got.lock().await.push((name.to_string(), bytes));
            Ok(())
        }
    }

    fn wal_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("tangent-wal-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn write(payload: &str) -> SinkWrite {
        SinkWrite {
            sink_name: Arc::from("logs"),
            payload: BytesMut::from(payload),
            s3: Some(s3::S3SinkItem {
                bucket_name: Arc::from(""),
                key_prefix: None,
            }),
        }
    }

    #[tokio::test]
    async fn flush_seals_and_sends_what_is_open_and_returns() {
        let dir = wal_dir("flush");
        let rec = Arc::new(Recorder::default());
        let wal = DurableFileSink::new(
            rec.clone(),
            &dir,
            4,
            1 << 20,
            Duration::from_secs(3600),
            Compression::None,
            Encoding::NDJSON,
        )
        .await
        .unwrap();
        wal.write(write("{\"n\":1}\n")).await.unwrap();
        wal.write(write("{\"n\":2}\n")).await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), wal.flush())
            .await
            .expect("flush returns")
            .unwrap();
        let got = rec.got.lock().await;
        assert_eq!(got.len(), 1, "sent once");
        assert_eq!(got[0].1, b"{\"n\":1}\n{\"n\":2}\n");
        // what is left is the next file, opened by the rotation and empty
        for f in std::fs::read_dir(&dir).unwrap() {
            let f = f.unwrap();
            let name = f.file_name().into_string().unwrap();
            assert!(!is_sealed_file_name(&name), "{name} was sent and removed");
            if name.ends_with(".bin") {
                assert_eq!(f.metadata().unwrap().len(), 0, "{name} holds nothing");
            }
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn a_sealed_file_left_from_before_is_sent_once() {
        let dir = wal_dir("leftover");
        std::fs::create_dir_all(&dir).unwrap();
        let meta = WalMeta {
            bucket_name: Arc::from(""),
            key_prefix: None,
            encoding: Encoding::NDJSON,
            compression: Compression::None,
        };
        write_meta_atomic(&dir.join("01LEFT.meta"), &meta)
            .await
            .unwrap();
        std::fs::write(dir.join("01LEFT.bin.sealed"), "{\"n\":1}\n").unwrap();
        // a compressed copy, made before the process stopped
        std::fs::write(dir.join("01LEFT.bin.sealed.zst"), "not this").unwrap();
        let rec = Arc::new(Recorder::default());
        let wal = DurableFileSink::new(
            rec.clone(),
            &dir,
            4,
            1 << 20,
            Duration::from_secs(3600),
            Compression::None,
            Encoding::NDJSON,
        )
        .await
        .unwrap();
        wal.flush().await.unwrap();
        let got = rec.got.lock().await;
        assert_eq!(got.len(), 1, "sent once");
        assert_eq!(got[0].1, b"{\"n\":1}\n");
        let _ = std::fs::remove_dir_all(dir);
    }

    async fn sent(encoding: Encoding, compression: Compression, tag: &str) -> (String, Vec<u8>) {
        let dir = wal_dir(tag);
        let rec = Arc::new(Recorder::default());
        let wal = DurableFileSink::new(
            rec.clone(),
            &dir,
            4,
            1 << 20,
            Duration::from_secs(3600),
            compression,
            encoding,
        )
        .await
        .unwrap();
        wal.write(write("{\"n\":1,\"m\":\"x\"}\n{\"n\":2}\n"))
            .await
            .unwrap();
        wal.write(write("{\"n\":3}\n")).await.unwrap();
        wal.flush().await.unwrap();
        let mut got = rec.got.lock().await;
        assert_eq!(got.len(), 1);
        for f in std::fs::read_dir(&dir).unwrap() {
            let f = f.unwrap();
            let name = f.file_name().into_string().unwrap();
            assert!(
                name.ends_with(".bin") || name.ends_with(".meta"),
                "{name} is gone once sent"
            );
        }
        let _ = std::fs::remove_dir_all(dir);
        got.pop().unwrap()
    }

    #[tokio::test]
    async fn a_sealed_file_goes_out_in_the_sinks_encoding() {
        let lines = "{\"n\":1,\"m\":\"x\"}\n{\"n\":2}\n{\"n\":3}\n";

        let (name, bytes) = sent(Encoding::NDJSON, Compression::Zstd { level: 3 }, "zst").await;
        assert!(name.ends_with(".ndjson.zst"), "{name}");
        assert_eq!(zstd::decode_all(&bytes[..]).unwrap(), lines.as_bytes());

        let (name, bytes) = sent(Encoding::NDJSON, Compression::None, "plain").await;
        assert!(name.ends_with(".ndjson"), "{name}");
        assert_eq!(bytes, lines.as_bytes());

        let schema = arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "n",
            arrow_schema::DataType::Int64,
            true,
        )]);
        let parquet = Encoding::Parquet {
            schema: serde_json::to_string(&schema).unwrap(),
        };
        let (name, bytes) = sent(parquet, Compression::Zstd { level: 3 }, "parquet").await;
        assert!(name.ends_with(".parquet"), "compressed inside: {name}");
        let rows = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
            .unwrap()
            .build()
            .unwrap()
            .map(|b| b.unwrap().num_rows())
            .sum::<usize>();
        assert_eq!(rows, 3);
    }

    #[test]
    fn object_name_is_the_ulid_then_extensions_the_bytes_have() {
        let sealed = Path::new("/wal/01JBX4N3V2.bin.sealed");
        let zstd = Compression::Zstd { level: 3 };
        assert_eq!(
            object_name(sealed, &Encoding::NDJSON, &zstd),
            "01JBX4N3V2.ndjson.zst"
        );
        assert_eq!(
            object_name(sealed, &Encoding::JSON, &Compression::None),
            "01JBX4N3V2.json"
        );
        let parquet = Encoding::Parquet {
            schema: String::new(),
        };
        assert_eq!(object_name(sealed, &parquet, &zstd), "01JBX4N3V2.parquet");
    }
}
