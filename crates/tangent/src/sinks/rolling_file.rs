use anyhow::Result;
use async_trait::async_trait;
use chrono::format::{Item, StrftimeItems};
use chrono::{DateTime, Utc};
use std::fs::File;
use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use tangent_shared::sinks::common::{Compression, Encoding};
use tangent_shared::sinks::rolling_file::subdir;
use tokio::task::spawn_blocking;

use crate::sinks::s3::S3SinkItem;
use crate::sinks::wal::WALSink;

/// A rolling_file sink's `path`: the part with no strftime specifier, where
/// the WAL puts files, and the rest, rendered in UTC when a write is routed,
/// as the directory below it each write goes to.
#[derive(Debug)]
pub struct PathTemplate {
    root: PathBuf,
    rest: Option<String>,
}

impl PathTemplate {
    pub fn parse(path: &str) -> Result<Self> {
        let mut root = PathBuf::new();
        let mut rest = Vec::new();
        for c in Path::new(path).components() {
            let s = c.as_os_str().to_string_lossy();
            if rest.is_empty() && !s.contains('%') {
                root.push(c);
            } else if matches!(c, Component::Normal(_)) {
                rest.push(s.into_owned());
            } else {
                anyhow::bail!("path {path:?}: no `.` or `..` after the first strftime specifier");
            }
        }
        let rest = (!rest.is_empty()).then(|| rest.join("/"));
        if let Some(r) = &rest {
            if StrftimeItems::new(r).any(|i| matches!(i, Item::Error)) {
                anyhow::bail!("path {path:?}: {r:?} is not a strftime template");
            }
        }
        Ok(Self { root, rest })
    }

    /// Where the files go: `path` up to its first strftime specifier.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// The directory below the root a write made at `now` goes to, with the
    /// edge's key prefix below that.
    pub fn route(&self, now: DateTime<Utc>, key_prefix: Option<&str>) -> Option<Arc<str>> {
        let time = self.rest.as_ref().map(|r| now.format(r).to_string());
        match (time, key_prefix) {
            (Some(t), Some(p)) => Some(Arc::from(format!("{t}/{p}"))),
            (Some(t), None) => Some(Arc::from(t)),
            (None, p) => p.map(Arc::from),
        }
    }
}

/// The far side of a WAL for a local directory: each sealed file, in the
/// sink's encoding, synced and renamed into place (by way of a copy under a
/// hidden temporary name when the WAL is on another filesystem), so a reader
/// listing the directory sees whole files only.
pub struct RollingFileSink {
    dir: PathBuf,
}

impl RollingFileSink {
    pub fn new(dir: impl AsRef<Path>) -> Self {
        Self {
            dir: dir.as_ref().to_path_buf(),
        }
    }
}

#[async_trait]
impl WALSink for RollingFileSink {
    async fn write_path_with(
        &self,
        path: &Path,
        name: &str,
        _encoding: &Encoding,
        _compression: &Compression,
        meta: &S3SinkItem,
    ) -> Result<()> {
        let dir = match meta.key_prefix.as_deref() {
            Some(prefix) if !prefix.is_empty() => self.dir.join(subdir(prefix)?),
            _ => self.dir.clone(),
        };
        let (src, name) = (path.to_path_buf(), name.to_string());
        spawn_blocking(move || -> Result<()> {
            // a directory made here lasts once its parent's entry for it does
            let mut made = Vec::new();
            let mut d = dir.as_path();
            while !d.exists() {
                made.push(d.to_path_buf());
                let Some(parent) = d.parent() else { break };
                d = parent;
            }
            std::fs::create_dir_all(&dir)?;
            for d in &made {
                if let Some(parent) = d.parent() {
                    File::open(parent)?.sync_all()?;
                }
            }
            let dst = dir.join(&name);
            File::open(&src)?.sync_all()?;
            // a rename costs nothing beside a second write of the file, which a
            // copy is on most filesystems (APFS clones it)
            match std::fs::rename(&src, &dst) {
                Ok(()) => {}
                Err(e) if e.kind() == ErrorKind::CrossesDevices => {
                    let tmp = dir.join(format!(".{name}.tmp"));
                    let copied = std::fs::copy(&src, &tmp)
                        .and_then(|_| File::open(&tmp)?.sync_all())
                        .and_then(|()| std::fs::rename(&tmp, &dst));
                    if copied.is_err() {
                        let _ = std::fs::remove_file(&tmp);
                    }
                    copied?;
                }
                Err(e) => return Err(e.into()),
            }
            // the rename lasts before the WAL lets the sealed file go
            File::open(&dir)?.sync_all()?;
            Ok(())
        })
        .await?
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sinks::manager::{Sink, SinkManager, SinkWrite};
    use crate::sinks::wal::DurableFileSink;
    use arrow_array::cast::AsArray;
    use bytes::BytesMut;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;
    use tangent_shared::sinks::common::SinkConfig;

    fn scratch(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("tangent-dir-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    fn parquet() -> Encoding {
        Encoding::Parquet {
            schema: serde_yaml::from_str("{ n: int, _norm: { actor: string } }").unwrap(),
            record_column: Some("record".into()),
        }
    }

    fn line(n: usize) -> String {
        format!(
            r#"{{"n":{n},"_norm":{{"actor":"a{}","org":"acme"}},"ip":"192.0.2.{}"}}"#,
            n % 5,
            n % 250
        )
    }

    fn write(payload: String, prefix: Option<&str>) -> SinkWrite {
        SinkWrite {
            sink_name: Arc::from("logs"),
            payload: BytesMut::from(payload.as_str()),
            s3: Some(S3SinkItem {
                bucket_name: Arc::from(""),
                key_prefix: prefix.map(Arc::from),
            }),
        }
    }

    /// The files in `dir`, sorted, and each one's record column.
    fn files(dir: &Path) -> Vec<(String, Vec<String>)> {
        let mut names: Vec<String> = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        names.sort();
        names
            .into_iter()
            .map(|name| {
                let bytes = std::fs::read(dir.join(&name)).unwrap();
                let reader = ParquetRecordBatchReaderBuilder::try_new(bytes::Bytes::from(bytes))
                    .unwrap()
                    .build()
                    .unwrap();
                let mut records = Vec::new();
                for batch in reader {
                    let batch = batch.unwrap();
                    let col = batch.column_by_name("record").unwrap().as_string::<i32>();
                    records.extend(col.iter().map(|r| r.unwrap().to_string()));
                }
                (name, records)
            })
            .collect()
    }

    async fn sink(out: &Path, wal: &Path, max_bytes: usize, age: Duration) -> Arc<DurableFileSink> {
        DurableFileSink::new(
            Arc::new(RollingFileSink::new(out)),
            wal,
            4,
            max_bytes,
            age,
            Compression::Zstd { level: 3 },
            parquet(),
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn sealed_files_become_whole_parquet_files_and_flush_sends_the_rest() {
        let root = scratch("rotate");
        let (out, wal_dir) = (root.join("out"), root.join("wal"));
        // about 70 bytes a line: a file holds some 28 lines
        let wal = sink(&out, &wal_dir, 2048, Duration::from_secs(3600)).await;
        let lines: Vec<String> = (0..500).map(line).collect();
        for chunk in lines.chunks(10) {
            wal.write(write(chunk.join("\n") + "\n", None))
                .await
                .unwrap();
        }
        wal.flush().await.unwrap();

        let got = files(&out);
        assert!(got.len() > 10, "rotated by size: {} files", got.len());
        for (name, _) in &got {
            assert!(
                name.ends_with(".parquet") && !name.starts_with('.'),
                "{name}"
            );
        }
        let names: std::collections::BTreeSet<_> = got.iter().map(|g| &g.0).collect();
        assert_eq!(names.len(), got.len(), "unique names");
        let mut records: Vec<String> = got.into_iter().flat_map(|g| g.1).collect();
        assert_eq!(records.len(), lines.len(), "every record once");
        records.sort();
        let mut want = lines.clone();
        want.sort();
        assert_eq!(records, want);
        for f in std::fs::read_dir(&wal_dir).unwrap() {
            let name = f.unwrap().file_name().into_string().unwrap();
            assert!(!name.ends_with(".sealed"), "{name} was sent");
        }
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn a_file_is_sealed_by_age_and_a_prefix_is_a_directory() {
        let root = scratch("age");
        let (out, wal_dir) = (root.join("out"), root.join("wal"));
        let wal = sink(&out, &wal_dir, 1 << 20, Duration::from_secs(1)).await;
        wal.write(write(line(1) + "\n", Some("acme/day")))
            .await
            .unwrap();
        let sub = out.join("acme/day");
        let mut waited = Duration::ZERO;
        while files_in(&sub) == 0 && waited < Duration::from_secs(10) {
            tokio::time::sleep(Duration::from_millis(100)).await;
            waited += Duration::from_millis(100);
        }
        assert_eq!(files(&sub).len(), 1, "sealed by age, without a flush");
        wal.write(write(line(2) + "\n", Some("acme/day")))
            .await
            .unwrap();
        wal.flush().await.unwrap();
        let got = files(&sub);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].1, [line(1)]);
        assert_eq!(got[1].1, [line(2)], "names sort by time");

        let bad = RollingFileSink::new(&out)
            .write_path_with(
                &out,
                "x.parquet",
                &parquet(),
                &Compression::None,
                &S3SinkItem {
                    bucket_name: Arc::from(""),
                    key_prefix: Some(Arc::from("../up")),
                },
            )
            .await;
        assert!(bad.is_err());
        let _ = std::fs::remove_dir_all(root);
    }

    fn files_in(dir: &Path) -> usize {
        std::fs::read_dir(dir).map_or(0, |d| {
            d.filter(|e| {
                let name = e.as_ref().unwrap().file_name();
                !name.to_string_lossy().starts_with('.')
            })
            .count()
        })
    }

    /// From YAML through the sink manager, as `tangent run` builds it: a
    /// write is in a file once the manager is joined, in the directory its
    /// time renders.
    #[tokio::test]
    async fn a_rolling_file_sink_from_config_has_its_files_by_shutdown() {
        let root = scratch("config");
        let yaml = format!(
            "type: rolling_file\npath: {}/%Y\nwal_path: {}\nmax_file_age_seconds: 3600\n\
             encoding: {{type: parquet, record_column: record, schema: {{n: int, _norm: {{actor: string}}}}}}\n",
            root.join("out").display(),
            root.join("wal").display(),
        );
        let cfg: SinkConfig = serde_yaml::from_str(&yaml).unwrap();
        let name: Arc<str> = Arc::from("logs");
        let manager = SinkManager::new(&BTreeMap::from([(name.clone(), cfg)]))
            .await
            .unwrap();
        let lines: Vec<String> = (0..3).map(line).collect();
        manager
            .enqueue(
                name,
                None,
                BytesMut::from((lines.join("\n") + "\n").as_str()),
                Vec::new(),
            )
            .await
            .unwrap();
        manager.join().await.unwrap();
        let year = Utc::now().format("%Y").to_string();
        let got = files(&root.join("out").join(year));
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].1, lines);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn a_path_is_a_root_and_a_strftime_rest() {
        let at = DateTime::parse_from_rfc3339("2026-11-02T13:05:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let t = PathTemplate::parse("lake/logs/%Y/%m/%d/%H").unwrap();
        assert_eq!(t.root(), Path::new("lake/logs"));
        assert_eq!(t.route(at, None).as_deref(), Some("2026/11/02/13"));
        assert_eq!(
            t.route(at, Some("aws")).as_deref(),
            Some("2026/11/02/13/aws")
        );
        let fixed = PathTemplate::parse("/var/lake").unwrap();
        assert_eq!(fixed.root(), Path::new("/var/lake"));
        assert_eq!(fixed.route(at, None), None);
        assert_eq!(fixed.route(at, Some("aws")).as_deref(), Some("aws"));
        assert!(PathTemplate::parse("lake/%Y/../x").is_err());
        assert!(PathTemplate::parse("lake/%Q").is_err());
    }
}
