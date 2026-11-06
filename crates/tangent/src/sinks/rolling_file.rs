use anyhow::Result;
use async_trait::async_trait;
use std::fs::File;
use std::io::ErrorKind;
use std::path::{Component, Path, PathBuf};
use tangent_shared::sinks::common::{Compression, Encoding};
use tangent_shared::sinks::rolling_file::subdir;
use tokio::task::spawn_blocking;

use crate::sinks::s3::S3SinkItem;
use crate::sinks::template::Template;
use crate::sinks::wal::WALSink;

/// A rolling_file sink's `path`: the part with no strftime specifier, where
/// the WAL puts files, and the rest, rendered in UTC for each record's own
/// time, as the directory below it the record goes to.
#[derive(Debug)]
pub struct PathTemplate {
    root: PathBuf,
    rest: Option<Template>,
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
        let rest = match rest.join("/") {
            r if r.is_empty() => None,
            r => match Template::parse(&r) {
                None => anyhow::bail!("path {path:?}: {r:?} is not a strftime template"),
                t => t,
            },
        };
        Ok(Self { root, rest })
    }

    /// Where the files go: `path` up to its first strftime specifier.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// What a record's directory below the root renders from; none for a
    /// `path` without a specifier.
    pub fn into_rest(self) -> Option<Template> {
        self.rest
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
    use chrono::{DateTime, Utc};
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

    /// A rolling_file sink `name` from YAML, as `tangent run` reads one: its
    /// `path` is `rest` below `root/out`, its WAL in `root/wal`.
    fn config(root: &Path, name: &Arc<str>, rest: &str) -> BTreeMap<Arc<str>, SinkConfig> {
        let yaml = format!(
            "type: rolling_file\npath: {}/{rest}\nwal_path: {}\nmax_file_age_seconds: 3600\n\
             encoding: {{type: parquet, record_column: record, schema: {{n: int, _norm: {{actor: string}}}}}}\n",
            root.join("out").display(),
            root.join("wal").display(),
        );
        BTreeMap::from([(name.clone(), serde_yaml::from_str(&yaml).unwrap())])
    }

    /// From YAML through the sink manager, as `tangent run` builds it: a
    /// write is in a file once the manager is joined, in the directory its
    /// time renders.
    #[tokio::test]
    async fn a_rolling_file_sink_from_config_has_its_files_by_shutdown() {
        let root = scratch("config");
        let name: Arc<str> = Arc::from("logs");
        let manager = SinkManager::new(&config(&root, &name, "%Y")).await.unwrap();
        let lines: Vec<String> = (0..3).map(line).collect();
        manager
            .enqueue(
                name,
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

    fn timed(ts: &str, n: usize) -> String {
        format!(r#"{{"ts":"{ts}","n":{n},"_norm":{{"actor":"a"}}}}"#)
    }

    /// Each directory below `out` that holds files, as a path from `out`,
    /// with its files' records, the files in name order; sorted.
    fn tree(out: &Path) -> Vec<(String, Vec<String>)> {
        let mut dirs = vec![out.to_path_buf()];
        let mut got = Vec::new();
        while let Some(dir) = dirs.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            let mut has_files = false;
            for e in entries {
                let p = e.unwrap().path();
                if p.is_dir() {
                    dirs.push(p);
                } else {
                    has_files = true;
                }
            }
            if has_files {
                let records = files(&dir).into_iter().flat_map(|f| f.1).collect();
                let rel = dir.strip_prefix(out).unwrap().display().to_string();
                got.push((rel, records));
            }
        }
        got.sort();
        got
    }

    #[derive(Default)]
    struct Acked(std::sync::atomic::AtomicUsize);

    impl Acked {
        fn count(&self) -> usize {
            self.0.load(std::sync::atomic::Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl crate::worker::Ack for Acked {
        async fn ack(&self) -> Result<()> {
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(())
        }
    }

    const H13: &str = "2026-11-02T13:59:59Z";
    const H14: &str = "2026-11-02T14:00:00Z";
    /// 13:30 in UTC
    const H13_WEST: &str = "2026-11-02T05:30:00-08:00";

    /// A frame of 13:00 and 14:00 records, in UTC.
    fn two_hours() -> String {
        [timed(H13, 0), timed(H14, 1), timed(H13_WEST, 2)].join("\n") + "\n"
    }

    /// Where [`two_hours`] goes: each record in its own hour, in the frame's
    /// order.
    fn in_their_hours() -> Vec<(String, Vec<String>)> {
        vec![
            (
                "2026/11/02/13".to_string(),
                vec![timed(H13, 0), timed(H13_WEST, 2)],
            ),
            ("2026/11/02/14".to_string(), vec![timed(H14, 1)]),
        ]
    }

    /// A rolling_file sink's `path` renders each record's own time, its
    /// `ts`, in UTC: a frame of two hours is files in two directories, and
    /// its ack follows both.
    #[tokio::test]
    async fn a_frame_of_two_hours_is_files_in_two_directories() {
        let root = scratch("hours");
        let name: Arc<str> = Arc::from("rolling-hours");
        let manager = SinkManager::new(&config(&root, &name, "%Y/%m/%d/%H"))
            .await
            .unwrap();
        let ack = Arc::new(Acked::default());
        manager
            .enqueue(
                name,
                BytesMut::from(two_hours().as_str()),
                vec![ack.clone()],
            )
            .await
            .unwrap();
        manager.join().await.unwrap();
        assert_eq!(tree(&root.join("out")), in_their_hours());
        assert_eq!(ack.count(), 1);
        let _ = std::fs::remove_dir_all(root);
    }

    /// A record without an RFC 3339 time in its `ts` goes in the directory
    /// of the hour it reaches the sink, and is counted for its sink: it is
    /// kept, not dropped.
    #[tokio::test]
    async fn a_record_without_a_time_goes_in_the_hour_it_reaches_the_sink() {
        let root = scratch("untimed");
        let name: Arc<str> = Arc::from("rolling-untimed");
        let manager = SinkManager::new(&config(&root, &name, "%Y/%m/%d/%H"))
            .await
            .unwrap();
        let untimed = vec![
            line(0),
            r#"{"ts":"noon","n":1,"_norm":{"actor":"a"}}"#.to_string(),
        ];
        let frame = [untimed.clone(), vec![timed(H13, 2)]].concat().join("\n") + "\n";
        let hour = || Utc::now().format("%Y/%m/%d/%H").to_string();
        let before = hour();
        manager
            .enqueue(name, BytesMut::from(frame.as_str()), Vec::new())
            .await
            .unwrap();
        manager.join().await.unwrap();
        let (timed_dir, now_dir): (Vec<_>, Vec<_>) = tree(&root.join("out"))
            .into_iter()
            .partition(|(dir, _)| dir == "2026/11/02/13");
        assert_eq!(
            timed_dir,
            [("2026/11/02/13".to_string(), vec![timed(H13, 2)])]
        );
        assert_eq!(now_dir.len(), 1, "{now_dir:?}");
        assert!([before, hour()].contains(&now_dir[0].0), "{now_dir:?}");
        assert_eq!(now_dir[0].1, untimed);
        let counted = crate::SINK_RECORDS_UNTIMED_TOTAL.with_label_values(&["rolling-untimed"]);
        assert_eq!(counted.get(), 2);
        let _ = std::fs::remove_dir_all(root);
    }

    /// Each hour's WAL file keeps its directory in its meta: a process
    /// killed with them open, and started again, puts each in its own hour.
    #[test]
    fn a_restart_puts_each_wal_file_in_its_own_hour() {
        let root = scratch("restart");
        let name: Arc<str> = Arc::from("rolling-restart");
        let ack = Arc::new(Acked::default());
        // killed: the runtime goes once the frame is acked, and every task
        // with it, with no join and no flush
        let killed = tokio::runtime::Runtime::new().unwrap();
        killed.block_on(async {
            let manager = SinkManager::new(&config(&root, &name, "%Y/%m/%d/%H"))
                .await
                .unwrap();
            manager
                .enqueue(
                    name.clone(),
                    BytesMut::from(two_hours().as_str()),
                    vec![ack.clone()],
                )
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(10), async {
                while ack.count() == 0 {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("acked once in the WAL");
        });
        drop(killed);
        assert!(
            tree(&root.join("out")).is_empty(),
            "sent before the restart"
        );

        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let manager = SinkManager::new(&config(&root, &name, "%Y/%m/%d/%H"))
                .await
                .unwrap();
            manager.join().await.unwrap();
        });
        assert_eq!(tree(&root.join("out")), in_their_hours());
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn a_path_is_a_root_and_a_strftime_rest() {
        let at = DateTime::parse_from_rfc3339("2026-11-02T13:05:00Z")
            .unwrap()
            .with_timezone(&Utc);
        let t = PathTemplate::parse("lake/logs/%Y/%m/%d/%H").unwrap();
        assert_eq!(t.root(), Path::new("lake/logs"));
        assert_eq!(&*t.into_rest().unwrap().render(at), "2026/11/02/13");
        let fixed = PathTemplate::parse("/var/lake").unwrap();
        assert_eq!(fixed.root(), Path::new("/var/lake"));
        assert!(fixed.into_rest().is_none());
        assert!(PathTemplate::parse("lake/%Y/../x").is_err());
        assert!(PathTemplate::parse("lake/%Q").is_err());
        // chrono parses `%#z` but cannot write it
        assert!(PathTemplate::parse("lake/%#z").is_err());
    }
}
