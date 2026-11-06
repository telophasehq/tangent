use anyhow::Result;
use async_trait::async_trait;
use flate2::write::GzEncoder;
use flate2::Compression as f2Compression;
use std::cmp::max;
use std::collections::{HashMap, HashSet};
use std::fs::File as stdFile;
use std::io::{copy, BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
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
use tokio_util::sync::CancellationToken;

use crate::sinks::encoding::{self, Unfit};
use crate::sinks::manager::{Sink, SinkWrite};
use crate::sinks::s3;
use crate::SINK_BYTES_UNCOMPRESSED_TOTAL;
use crate::{
    SINK_BYTES_TOTAL, SINK_OBJECTS_TOTAL, SINK_RECORDS_UNFIT_TOTAL, WAL_PENDING_BYTES,
    WAL_PENDING_FILES, WAL_SEALED_BYTES_TOTAL, WAL_SEALED_FILES_TOTAL,
};

/// How often a sealed file whose send failed is tried again.
const RETRY_EVERY: Duration = if cfg!(test) {
    Duration::from_millis(200)
} else {
    Duration::from_secs(30)
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
    /// Stops the rotator between its rounds.
    stop: CancellationToken,
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
/// the object's bytes, `name` is what to call it (see [`object_name`]). The
/// sink may move `path` away; the WAL removes it if it is still there.
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
            stop: CancellationToken::new(),
            uploads: Mutex::new(JoinSet::new()),
            sending: Arc::default(),
        });
        let d = s.dir.clone();
        let recovered = spawn_blocking(move || recover(&d)).await?;
        if recovered > 0 {
            tracing::info!(files = recovered, dir = ?s.dir, "WAL sealed files left open by a stop without flush");
        }
        s.retry_leftovers(true).await;

        let s_cloned = s.clone();
        let stop = s.stop.clone();
        let handle = tokio::spawn(async move {
            let tick = max(Duration::from_millis(250), max_file_age / 4).min(RETRY_EVERY);
            let mut retried = Instant::now();
            loop {
                tokio::select! {
                    () = stop.cancelled() => break,
                    () = sleep(tick) => {
                        // a file whose send failed is in the WAL still
                        if retried.elapsed() >= RETRY_EVERY {
                            s_cloned.retry_leftovers(false).await;
                            retried = Instant::now();
                        }
                        // each with the file found aged: a writer may have
                        // sealed it since
                        let to_rotate: Vec<(RouteKey, PathBuf)> = {
                            let routes = s_cloned.routes.lock().await;
                            routes.iter()
                                .filter(|(_, rs)| rs.cur.bytes > 0 && rs.cur.created_at.elapsed() >= s_cloned.max_file_age)
                                .map(|(k, rs)| (k.clone(), rs.cur.path.clone()))
                                .collect()
                        };
                        for (k, path) in to_rotate {
                            let _ = s_cloned.rotate_route(k, Some(&path)).await;
                        }
                    }
                }
            }
        });

        *s.rotator.lock().await = Some(handle);
        Ok(s)
    }

    /// Seals the route's open file and closes the route: its next file is
    /// opened by the write that needs it. With `only`, only if the open file
    /// is still that one, so writers that each found it full seal it once
    /// between them.
    async fn rotate_route(&self, rkey: RouteKey, only: Option<&Path>) -> anyhow::Result<()> {
        let (sealed_ready, sealed_bytes, meta) = {
            let mut routes = self.routes.lock().await;
            let Some(rs) = routes.get_mut(&rkey) else {
                return Ok(());
            };
            if rs.cur.bytes == 0 || only.is_some_and(|p| p != rs.cur.path) {
                return Ok(());
            }

            if let Some(f) = rs.cur.file.take() {
                f.sync_data().await?;
            }

            let mut sealed = rs.cur.path.clone();
            sealed.set_extension("bin.sealed");
            fs::rename(&rs.cur.path, &sealed).await?;
            let sealed_bytes = rs.cur.bytes as u64;
            let meta = rs.meta.clone();
            routes.remove(&rkey);

            (sealed, sealed_bytes, meta)
        };

        WAL_SEALED_FILES_TOTAL.inc();
        WAL_SEALED_BYTES_TOTAL.inc_by(sealed_bytes);
        WAL_PENDING_FILES.inc();
        WAL_PENDING_BYTES.add(sealed_bytes as i64);

        self.spawn_upload_with_meta(sealed_ready, sealed_bytes, meta)
            .await;
        Ok(())
    }

    /// Sends each sealed file in the WAL not being sent now: at the start,
    /// what an earlier process left, counted as pending as a file sealed now
    /// is; later, a file whose send failed.
    async fn retry_leftovers(&self, at_start: bool) {
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
                if at_start {
                    WAL_PENDING_FILES.inc();
                    WAL_PENDING_BYTES.add(md.len() as i64);
                }
                self.spawn_upload_with_meta(
                    p.clone(),
                    md.len(),
                    s3::S3SinkItem {
                        bucket_name: meta.bucket_name,
                        key_prefix: meta.key_prefix,
                    },
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
            let (upload_path, upload_size, unfit) = encode_sealed(
                &sealed_path_clone,
                orig_size,
                &name,
                &wal_meta.encoding,
                &wal_meta.compression,
            )
            .await?;

            let sent = inner
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
                .await;
            // the encoded copy goes whether or not it went out: a retry makes
            // it again from the sealed file
            if upload_path != sealed_path_clone {
                let _ = fs::remove_file(&upload_path).await;
            }
            sent?;

            let _ = fs::remove_file(&sealed_path_clone).await;
            let _ = fs::remove_file(&meta_path).await;

            Ok::<_, anyhow::Error>((upload_size, unfit))
        };

        let mut js = self.uploads.lock().await;
        // a finished send is let go here, not kept until the next flush
        while let Some(done) = js.try_join_next() {
            if let Err(e) = done {
                tracing::warn!("upload task join error: {e}");
            }
        }

        js.spawn(async move {
            match fut.await {
                Ok((uploaded, unfit)) => {
                    SINK_OBJECTS_TOTAL.inc();
                    SINK_BYTES_TOTAL.inc_by(uploaded);
                    SINK_BYTES_UNCOMPRESSED_TOTAL.inc_by(orig_size);
                    WAL_PENDING_FILES.dec();
                    WAL_PENDING_BYTES.sub(orig_size as i64);
                    tracing::debug!(bytes = uploaded, "WAL uploaded & removed");
                    // counted once the file is out: each try encodes it again
                    if unfit != Unfit::default() {
                        SINK_RECORDS_UNFIT_TOTAL
                            .with_label_values(&["kept"])
                            .inc_by(unfit.kept as u64);
                        SINK_RECORDS_UNFIT_TOTAL
                            .with_label_values(&["dropped"])
                            .inc_by(unfit.dropped as u64);
                        tracing::warn!(
                            kept = unfit.kept,
                            dropped = unfit.dropped,
                            "{:?}: lines its encoding could not read",
                            sealed_path
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!(
                        "upload error for {:?}, retried in {RETRY_EVERY:?}: {e}",
                        sealed_path
                    );
                }
            }
            sending.lock().remove(&sealed_path);
            inflight.fetch_sub(1, Ordering::AcqRel);
        });
    }
}

#[async_trait::async_trait]
impl Sink for DurableFileSink {
    async fn write(&self, mut req: SinkWrite) -> Result<()> {
        let Some(meta) = req.s3 else {
            anyhow::bail!("DurableFileSink requires s3 meta (bucket/prefix)")
        };
        if req.payload.is_empty() {
            return Ok(());
        }
        // a write's last line must not run into the next write's first
        if !req.payload.ends_with(b"\n") {
            req.payload.extend_from_slice(b"\n");
        }
        let rkey = RouteKey {
            sink_name: req.sink_name,
            prefix: meta.key_prefix.clone(),
        };

        loop {
            let mut routes = self.routes.lock().await;
            // a route has a file only while a write needs one, opened under
            // the lock: concurrent writes open one file, not one each, the
            // losers' left empty in the WAL
            if !routes.contains_key(&rkey) {
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
            let rs = routes.get_mut(&rkey).expect("route opened above");
            // a write bigger than a file gets a file of its own: rotating
            // an empty file does nothing, and would go round for ever
            if rs.cur.bytes == 0 || rs.cur.bytes + req.payload.len() <= self.max_file_size {
                let f = rs.cur.file.as_mut().expect("current file missing");
                // acked once write(2) is done: tokio's write_all returns with
                // the bytes in its own buffer, and an error writing them out
                // would surface on the next write instead of this one
                let wrote = async {
                    f.write_all(&req.payload).await?;
                    f.flush().await
                }
                .await;
                if let Err(e) = wrote {
                    // what part of it reached the file goes, so the retry's
                    // lines do not follow a torn one
                    let _ = f.set_len(rs.cur.bytes as u64).await;
                    return Err(e.into());
                }
                rs.cur.bytes += req.payload.len();
                rs.last_used = Instant::now();
                break;
            }
            let full = rs.cur.path.clone();
            drop(routes);
            self.rotate_route(rkey.clone(), Some(&full)).await?;
        }

        Ok(())
    }

    async fn flush(&self) -> Result<()> {
        // The rotator loops for as long as the sink lives: stop it between
        // rounds, then seal what is left below. Aborted in a round, it could
        // leave a sealed file marked as being sent with nothing sending it,
        // or the in-flight count raised for good.
        self.stop.cancel();
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
            let _ = self.rotate_route(k, None).await;
        }
        // a route whose only write failed holds an empty file: it goes,
        // rather than wait in the WAL for the next start
        let empty: Vec<PathBuf> = {
            let mut routes = self.routes.lock().await;
            let mut empty = Vec::new();
            routes.retain(|_, rs| {
                if rs.cur.bytes > 0 {
                    return true;
                }
                rs.cur.file.take();
                empty.push(rs.cur.path.clone());
                false
            });
            empty
        };
        for p in empty {
            let _ = fs::remove_file(&p).await;
            let _ = fs::remove_file(meta_path_for(&p)).await;
        }

        loop {
            self.retry_leftovers(false).await;
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

/// The sealed NDJSON as the object it goes out as: the sealed file itself
/// for NDJSON left uncompressed, and otherwise an encoded copy beside it
/// under the object's name. With the records the encoding could not take.
async fn encode_sealed(
    sealed: &Path,
    size: u64,
    name: &str,
    enc: &Encoding,
    comp: &Compression,
) -> Result<(PathBuf, u64, Unfit)> {
    if matches!(enc, Encoding::NDJSON) && !compresses_whole(comp) {
        return Ok((sealed.to_path_buf(), size, Unfit::default()));
    }
    encode_to_file(sealed, &sealed.with_file_name(name), enc, comp).await
}

async fn encode_to_file(
    src: &Path,
    dst: &Path,
    enc: &Encoding,
    comp: &Compression,
) -> Result<(PathBuf, u64, Unfit)> {
    let (src, dst) = (src.to_path_buf(), dst.to_path_buf());
    let (enc, comp) = (enc.clone(), comp.clone());
    spawn_blocking(move || -> Result<(PathBuf, u64, Unfit)> {
        let tmp = tmp_path(&dst);
        let made = encode_file(&src, &tmp, &enc, &comp);
        if made.is_err() {
            let _ = std::fs::remove_file(&tmp);
        }
        let unfit = made?;
        std::fs::rename(&tmp, &dst)?;
        let size = std::fs::metadata(&dst)?.len();
        Ok((dst, size, unfit))
    })
    .await?
}

/// `src`'s records into `tmp` in the encoding, a batch, a line or a block in
/// memory at a time, not the file. NDJSON and JSON are compressed whole,
/// Parquet and Avro inside the file. Returns the records the encoding could
/// not take.
fn encode_file(src: &Path, tmp: &Path, enc: &Encoding, comp: &Compression) -> Result<Unfit> {
    let fin = BufReader::new(stdFile::open(src)?);
    let out = BufWriter::new(stdFile::create(tmp)?);
    let (mut out, unfit) = match (enc, comp) {
        (
            Encoding::Parquet {
                schema,
                record_column,
            },
            _,
        ) => encoding::write_parquet(fin, out, schema, record_column.as_deref(), comp)?,
        (Encoding::Avro { schema }, _) => {
            let (out, dropped) = encoding::write_avro(fin, out, schema, comp)?;
            (out, Unfit { kept: 0, dropped })
        }
        (_, Compression::Gzip { level }) => {
            let gz = GzEncoder::new(out, f2Compression::new(*level));
            let (gz, unfit) = write_whole(enc, fin, gz)?;
            (gz.finish()?, unfit)
        }
        (_, Compression::Zstd { level }) => {
            let zst = zstd::stream::Encoder::new(out, *level)?;
            let (zst, unfit) = write_whole(enc, fin, zst)?;
            (zst.finish()?, unfit)
        }
        _ => write_whole(enc, fin, out)?,
    };
    out.flush()?;
    Ok(unfit)
}

/// NDJSON as it is, or JSON as one array.
fn write_whole<W: Write>(enc: &Encoding, mut fin: impl BufRead, mut out: W) -> Result<(W, Unfit)> {
    if matches!(enc, Encoding::JSON) {
        let (out, dropped) = encoding::write_json_array(fin, out)?;
        return Ok((out, Unfit { kept: 0, dropped }));
    }
    copy(&mut fin, &mut out)?;
    Ok((out, Unfit::default()))
}

/// Whether the compression is applied around a whole NDJSON or JSON file
/// (gzip, zstd); the others are Parquet's and Avro's own.
fn compresses_whole(comp: &Compression) -> bool {
    matches!(comp, Compression::Gzip { .. } | Compression::Zstd { .. })
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

/// Makes ready to send what a process that stopped without its flush
/// (killed, crashed, out of time at shutdown) left in the WAL, and says how
/// many files it sealed: a file still open is cut back to its last whole
/// line (a write is acked only once it is whole in the file, so what follows
/// was never acked) and sealed; an empty one goes, with its meta; and so do
/// a meta whose file is gone, a half-made stage (`.tmp`), and the compressed
/// copy an earlier version made beside a sealed file. Only the names a WAL
/// makes, a ULID and its extensions, are touched.
fn recover(dir: &Path) -> usize {
    let Ok(rd) = std::fs::read_dir(dir) else {
        return 0;
    };
    let names: HashSet<String> = rd
        .filter_map(|e| e.ok()?.file_name().into_string().ok())
        .collect();
    let mut sealed = 0;
    for name in &names {
        let Some((base, ext)) = name.split_once('.') else {
            continue;
        };
        if ulid::Ulid::from_string(base).is_err() {
            continue;
        }
        let has = |e: &str| names.contains(&format!("{base}.{e}"));
        let path = dir.join(name);
        let done = match ext {
            "bin" => match whole_lines(&path) {
                Ok(0) => std::fs::remove_file(&path)
                    .and_then(|()| std::fs::remove_file(dir.join(format!("{base}.meta")))),
                Ok(keep) => {
                    let cut = std::fs::OpenOptions::new()
                        .write(true)
                        .open(&path)
                        .and_then(|f| {
                            f.set_len(keep)?;
                            f.sync_all()
                        });
                    cut.and_then(|()| {
                        std::fs::rename(&path, dir.join(format!("{base}.bin.sealed")))
                    })
                    .map(|()| sealed += 1)
                }
                Err(e) => Err(e),
            },
            "bin.sealed.zst" | "bin.sealed.gz" if has("bin.sealed") => std::fs::remove_file(&path),
            "meta" if !has("bin") && !has("bin.sealed") => std::fs::remove_file(&path),
            e if e.ends_with("tmp") => std::fs::remove_file(&path),
            _ => Ok(()),
        };
        if let Err(e) = done {
            tracing::warn!("WAL recovery of {path:?}: {e}");
        }
    }
    sealed
}

/// The length of the file up to and including its last newline.
fn whole_lines(path: &Path) -> std::io::Result<u64> {
    let mut f = stdFile::open(path)?;
    let mut end = f.metadata()?.len();
    let mut buf = vec![0u8; 64 << 10];
    while end > 0 {
        let start = end.saturating_sub(buf.len() as u64);
        let chunk = &mut buf[..(end - start) as usize];
        f.seek(SeekFrom::Start(start))?;
        f.read_exact(chunk)?;
        if let Some(i) = memchr::memrchr(b'\n', chunk) {
            return Ok(start + i as u64 + 1);
        }
        end = start;
    }
    Ok(0)
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
    // appending: a write cut back after a failure leaves the end where the
    // next one goes
    let file = fs::OpenOptions::new()
        .create_new(true)
        .append(true)
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
    use bytes::BytesMut;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::io::Write;

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

    fn names_in(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        names
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
        assert_eq!(
            names_in(&dir),
            Vec::<String>::new(),
            "sent, and nothing left"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn a_route_sealed_by_age_leaves_no_file_open() {
        let dir = wal_dir("idle");
        let rec = Arc::new(Recorder::default());
        let wal = DurableFileSink::new(
            rec.clone(),
            &dir,
            4,
            1 << 20,
            Duration::from_millis(300),
            Compression::None,
            Encoding::NDJSON,
        )
        .await
        .unwrap();
        // routes that stop getting writes, as an hour's directory does
        for hour in ["h1", "h2", "h3"] {
            let mut w = write("{\"n\":1}\n");
            w.s3.as_mut().unwrap().key_prefix = Some(Arc::from(hour));
            wal.write(w).await.unwrap();
        }
        // sent, and nothing left: no empty file opened for a next write
        tokio::time::timeout(Duration::from_secs(10), async {
            while rec.got.lock().await.len() < 3 || !names_in(&dir).is_empty() {
                sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("each route sealed, sent and gone by age");
        assert_eq!(wal.routes.lock().await.len(), 0, "no route kept open");
        let _ = std::fs::remove_dir_all(dir);
    }

    /// A finished send is let go when the next one starts, not kept until
    /// a flush.
    #[tokio::test]
    async fn a_finished_send_is_not_kept() {
        let dir = wal_dir("reap");
        let rec = Arc::new(Recorder::default());
        // a file holds one write: each write seals the one before
        let wal = DurableFileSink::new(
            rec.clone(),
            &dir,
            4,
            8,
            Duration::from_secs(3600),
            Compression::None,
            Encoding::NDJSON,
        )
        .await
        .unwrap();
        for n in 0..40 {
            wal.write(write(&format!("{{\"n\":{n}}}\n"))).await.unwrap();
        }
        tokio::time::timeout(Duration::from_secs(10), async {
            while rec.got.lock().await.len() < 39 || wal.inflight.load(Ordering::Acquire) > 0 {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("39 sealed and sent");
        sleep(Duration::from_millis(50)).await;
        wal.write(write("{\"n\":40}\n")).await.unwrap();
        assert_eq!(
            wal.uploads.lock().await.len(),
            1,
            "only the send just started"
        );
        wal.flush().await.unwrap();
        assert_eq!(rec.got.lock().await.len(), 41);
        assert_eq!(names_in(&dir), Vec::<String>::new());
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Fails its first send, then records as [`Recorder`] does.
    #[derive(Default)]
    struct FailsOnce {
        failed: std::sync::atomic::AtomicBool,
        rec: Recorder,
    }

    #[async_trait]
    impl WALSink for FailsOnce {
        async fn write_path_with(
            &self,
            path: &Path,
            name: &str,
            encoding: &Encoding,
            compression: &Compression,
            meta: &s3::S3SinkItem,
        ) -> Result<()> {
            if !self.failed.swap(true, Ordering::SeqCst) {
                anyhow::bail!("the disk is full");
            }
            self.rec
                .write_path_with(path, name, encoding, compression, meta)
                .await
        }
    }

    #[tokio::test]
    async fn a_failed_send_is_tried_again_without_a_restart() {
        let dir = wal_dir("retry");
        let sink = Arc::new(FailsOnce::default());
        let wal = DurableFileSink::new(
            sink.clone(),
            &dir,
            4,
            16,
            Duration::from_secs(3600),
            Compression::Zstd { level: 3 },
            Encoding::NDJSON,
        )
        .await
        .unwrap();
        // the second write seals the first's file, whose send fails
        wal.write(write("{\"n\":1,\"pad\":\"x\"}\n")).await.unwrap();
        wal.write(write("{\"n\":2}\n")).await.unwrap();
        let mut waited = Duration::ZERO;
        while sink.rec.got.lock().await.is_empty() && waited < Duration::from_secs(10) {
            tokio::time::sleep(Duration::from_millis(50)).await;
            waited += Duration::from_millis(50);
        }
        assert!(sink.failed.load(Ordering::SeqCst));
        {
            let got = sink.rec.got.lock().await;
            assert_eq!(got.len(), 1, "sent on a retry, before any flush");
            let sent = zstd::decode_all(&got[0].1[..]).unwrap();
            assert_eq!(sent, b"{\"n\":1,\"pad\":\"x\"}\n");
        }
        let left = names_in(&dir);
        assert!(
            !left
                .iter()
                .any(|n| n.ends_with(".zst") || n.ends_with(".tmp")),
            "no staged copy left: {left:?}"
        );
        wal.flush().await.unwrap();
        assert_eq!(sink.rec.got.lock().await.len(), 2);
        assert_eq!(names_in(&dir), Vec::<String>::new());
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn each_write_ends_its_last_line() {
        let dir = wal_dir("newline");
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
        for payload in ["{\"n\":1}", "", "{\"n\":2}\n{\"n\":3}"] {
            wal.write(write(payload)).await.unwrap();
        }
        wal.flush().await.unwrap();
        let got = rec.got.lock().await;
        assert_eq!(got[0].1, b"{\"n\":1}\n{\"n\":2}\n{\"n\":3}\n");
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

    #[tokio::test]
    async fn a_restart_sends_what_was_open_when_the_process_stopped() {
        let dir = wal_dir("restart");
        let ndjson = |rec: Arc<Recorder>| {
            DurableFileSink::new(
                rec,
                &dir,
                4,
                1 << 20,
                Duration::from_secs(3600),
                Compression::None,
                Encoding::NDJSON,
            )
        };
        let first = ndjson(Arc::new(Recorder::default())).await.unwrap();
        first.write(write("{\"n\":1}\n")).await.unwrap();
        first.write(write("{\"n\":2}\n")).await.unwrap();
        // it stops without a flush, in the middle of a write it never acked
        first.stop.cancel();
        let rotator = first.rotator.lock().await.take().unwrap();
        rotator.await.unwrap();
        let open = names_in(&dir)
            .into_iter()
            .find(|n| n.ends_with(".bin"))
            .unwrap();
        std::fs::OpenOptions::new()
            .append(true)
            .open(dir.join(&open))
            .unwrap()
            .write_all(b"{\"n\":")
            .unwrap();
        let meta = WalMeta {
            bucket_name: Arc::from(""),
            key_prefix: None,
            encoding: Encoding::NDJSON,
            compression: Compression::None,
        };
        let ulid = || ulid::Ulid::new().to_string();
        // a file sealed and compressed by an earlier version, not yet sent
        let old = ulid();
        write_meta_atomic(&dir.join(format!("{old}.meta")), &meta)
            .await
            .unwrap();
        std::fs::write(dir.join(format!("{old}.bin.sealed")), "{\"n\":0}\n").unwrap();
        std::fs::write(dir.join(format!("{old}.bin.sealed.zst")), "a copy").unwrap();
        // an empty pair, a meta whose file was sent, a half-written meta
        let empty = ulid();
        std::fs::write(dir.join(format!("{empty}.bin")), "").unwrap();
        write_meta_atomic(&dir.join(format!("{empty}.meta")), &meta)
            .await
            .unwrap();
        write_meta_atomic(&dir.join(format!("{}.meta", ulid())), &meta)
            .await
            .unwrap();
        std::fs::write(dir.join(format!("{}.meta.tmp", ulid())), "{").unwrap();

        let rec = Arc::new(Recorder::default());
        let second = ndjson(rec.clone()).await.unwrap();
        second.flush().await.unwrap();
        let mut got: Vec<Vec<u8>> = rec.got.lock().await.iter().map(|g| g.1.clone()).collect();
        got.sort();
        assert_eq!(
            got,
            [b"{\"n\":0}\n".to_vec(), b"{\"n\":1}\n{\"n\":2}\n".to_vec()]
        );
        assert_eq!(names_in(&dir), Vec::<String>::new(), "nothing left");
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
        assert_eq!(names_in(&dir), Vec::<String>::new(), "gone once sent");
        let _ = std::fs::remove_dir_all(dir);
        got.pop().unwrap()
    }

    #[tokio::test]
    async fn a_sealed_file_goes_out_in_the_sinks_encoding() {
        let lines = "{\"n\":1,\"m\":\"x\"}\n{\"n\":2}\n{\"n\":3}\n";

        let (name, bytes) = sent(Encoding::NDJSON, Compression::Zstd { level: 3 }, "zst").await;
        assert!(name.ends_with(".ndjson.zst"), "{name}");
        assert_eq!(zstd::decode_all(&bytes[..]).unwrap(), lines.as_bytes());

        let (name, bytes) = sent(Encoding::NDJSON, Compression::Gzip { level: 6 }, "gz").await;
        assert!(name.ends_with(".ndjson.gz"), "{name}");
        let mut ndjson = String::new();
        std::io::Read::read_to_string(&mut flate2::read::GzDecoder::new(&bytes[..]), &mut ndjson)
            .unwrap();
        assert_eq!(ndjson, lines);

        let (name, bytes) = sent(Encoding::NDJSON, Compression::None, "plain").await;
        assert!(name.ends_with(".ndjson"), "{name}");
        assert_eq!(bytes, lines.as_bytes());

        let (name, bytes) = sent(Encoding::JSON, Compression::Gzip { level: 6 }, "json").await;
        assert!(name.ends_with(".json.gz"), "{name}");
        let mut json = String::new();
        std::io::Read::read_to_string(&mut flate2::read::GzDecoder::new(&bytes[..]), &mut json)
            .unwrap();
        assert_eq!(json, r#"[{"n":1,"m":"x"},{"n":2},{"n":3}]"#);

        let parquet = Encoding::Parquet {
            schema: serde_yaml::from_str("{ n: int }").unwrap(),
            record_column: Some("record".into()),
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
            schema: Default::default(),
            record_column: None,
        };
        assert_eq!(object_name(sealed, &parquet, &zstd), "01JBX4N3V2.parquet");
    }

    #[tokio::test]
    async fn a_write_bigger_than_a_file_is_a_file_of_its_own() {
        let dir = wal_dir("big");
        let rec = Arc::new(Recorder::default());
        let wal = DurableFileSink::new(
            rec.clone(),
            &dir,
            4,
            16,
            Duration::from_secs(3600),
            Compression::None,
            Encoding::NDJSON,
        )
        .await
        .unwrap();
        let big = "{\"n\":1,\"m\":\"more than sixteen bytes\"}\n";
        for payload in ["{\"n\":0}\n", big, "{\"n\":2}\n"] {
            tokio::time::timeout(Duration::from_secs(10), wal.write(write(payload)))
                .await
                .expect("the write returns")
                .unwrap();
        }
        wal.flush().await.unwrap();
        let mut got: Vec<Vec<u8>> = rec.got.lock().await.iter().map(|g| g.1.clone()).collect();
        got.sort();
        assert_eq!(
            got,
            [
                b"{\"n\":0}\n".to_vec(),
                big.as_bytes().to_vec(),
                b"{\"n\":2}\n".to_vec()
            ]
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn concurrent_writers_fill_each_file_and_open_one_at_a_time() {
        let dir = wal_dir("concurrent");
        let rec = Arc::new(Recorder::default());
        let wal = DurableFileSink::new(
            rec.clone(),
            &dir,
            64,
            1000,
            Duration::from_secs(3600),
            Compression::None,
            Encoding::NDJSON,
        )
        .await
        .unwrap();
        // 100 bytes a write: a file is sealed with 901 to 1000 bytes in it
        let payload = format!("{{\"pad\":\"{}\"}}\n", "x".repeat(89));
        assert_eq!(payload.len(), 100);
        let writers: Vec<_> = (0..16)
            .map(|_| {
                let (wal, payload) = (wal.clone(), payload.clone());
                tokio::spawn(async move {
                    for _ in 0..50 {
                        wal.write(write(&payload)).await.unwrap();
                    }
                })
            })
            .collect();
        for w in writers {
            w.await.unwrap();
        }
        let open: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .filter(|n| n.ends_with(".bin"))
            .collect();
        assert_eq!(open.len(), 1, "one open file: {open:?}");
        wal.flush().await.unwrap();
        let got = rec.got.lock().await;
        let total: usize = got.iter().map(|g| g.1.len()).sum();
        assert_eq!(total, 16 * 50 * 100);
        let small = got.iter().filter(|g| g.1.len() <= 900).count();
        assert!(small <= 1, "{small} files sealed before they were full");
        let _ = std::fs::remove_dir_all(dir);
    }
}
