use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::BytesMut;
use rand::{rng, Rng};
use std::collections::{BTreeMap, HashMap};
use std::path::{Component, Path, PathBuf};
use std::{sync::Arc, time::Duration};
use tangent_shared::sinks::common::{SinkConfig, SinkKind};
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{sleep, Instant};

use crate::sinks::blackhole;
use crate::sinks::encoding;
use crate::sinks::file;
use crate::sinks::rolling_file;
use crate::sinks::s3::S3SinkItem;
use crate::sinks::slack;
use crate::sinks::sqlite;
use crate::INFLIGHT;
use crate::{
    sinks::{s3, wal},
    worker::Ack,
};

pub struct SinkWrite {
    pub sink_name: Arc<str>,
    pub payload: BytesMut,
    pub s3: Option<S3SinkItem>,
}

#[async_trait]
pub trait Sink: Send + Sync {
    async fn write(&self, req: SinkWrite) -> Result<()>;

    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}

pub struct SinkItem {
    pub acks: Vec<Arc<dyn Ack>>,
    pub req: SinkWrite,
}

struct Queue {
    tx: mpsc::Sender<SinkItem>,
    handle: JoinHandle<()>,
}

#[derive(Clone)]
enum SinkEntry {
    /// Through a WAL (`s3`, `rolling_file`): a write carries its route, the
    /// bucket (s3's; empty for rolling_file) and the key prefix rendered for
    /// the time it is routed: a rolling_file's directory below its `path`'s
    /// root, an s3 sink's `key_prefix`.
    Wal {
        sink: Arc<dyn Sink>,
        bucket: Arc<str>,
        path: Option<Arc<rolling_file::PathTemplate>>,
        key_prefix: Option<Arc<s3::KeyPrefix>>,
    },
    Other {
        sink: Arc<dyn Sink>,
    },
}

pub struct SinkManager {
    queues: HashMap<Arc<str>, Queue>,
    sinks: HashMap<Arc<str>, SinkEntry>,
}

impl SinkManager {
    pub async fn new(cfgs: &BTreeMap<Arc<str>, SinkConfig>) -> Result<Self> {
        check_wal_dirs(cfgs)?;
        s3::check_key_prefixes(cfgs)?;
        for (name, cfg) in cfgs {
            encoding::check(&cfg.common.encoding, &cfg.common.compression)
                .with_context(|| format!("sink '{name}'"))?;
        }
        let mut sinks: HashMap<Arc<str>, SinkEntry> = HashMap::with_capacity(cfgs.len());

        for (name, cfg) in cfgs {
            match &cfg.kind {
                SinkKind::S3(s3cfg) => {
                    let key_prefix = s3cfg
                        .key_prefix
                        .as_deref()
                        .map(s3::KeyPrefix::parse)
                        .transpose()
                        .with_context(|| format!("sink '{name}'"))?
                        .map(Arc::new);
                    let remote = Arc::new(s3::S3Sink::new(Arc::clone(name), s3cfg).await?);
                    let s3_sink = wal::DurableFileSink::new(
                        remote,
                        s3cfg.wal_path.clone(),
                        cfg.common.in_flight_limit,
                        cfg.common.object_max_bytes,
                        Duration::from_secs(s3cfg.max_file_age_seconds),
                        cfg.common.compression.clone(),
                        cfg.common.encoding.clone(),
                    )
                    .await?;
                    sinks.insert(
                        Arc::clone(&name),
                        SinkEntry::Wal {
                            sink: s3_sink as Arc<dyn Sink>,
                            bucket: Arc::<str>::from(s3cfg.bucket_name.clone()),
                            path: None,
                            key_prefix,
                        },
                    );
                }
                SinkKind::RollingFile(rcfg) => {
                    let path = Arc::new(
                        rolling_file::PathTemplate::parse(&rcfg.path)
                            .with_context(|| format!("sink '{name}'"))?,
                    );
                    let wal = wal::DurableFileSink::new(
                        Arc::new(rolling_file::RollingFileSink::new(path.root())),
                        rcfg.wal_path.clone(),
                        cfg.common.in_flight_limit,
                        cfg.common.object_max_bytes,
                        Duration::from_secs(rcfg.max_file_age_seconds),
                        cfg.common.compression.clone(),
                        cfg.common.encoding.clone(),
                    )
                    .await?;
                    sinks.insert(
                        Arc::clone(name),
                        SinkEntry::Wal {
                            sink: wal,
                            bucket: Arc::from(""),
                            path: Some(path),
                            key_prefix: None,
                        },
                    );
                }
                SinkKind::File(filecfg) => {
                    let file_sink = file::FileSink::new(filecfg, &cfg.common).await?;
                    sinks.insert(Arc::clone(&name), SinkEntry::Other { sink: file_sink });
                }
                SinkKind::Blackhole(_) => {
                    let bh = blackhole::BlackholeSink::new();
                    sinks.insert(Arc::clone(&name), SinkEntry::Other { sink: bh });
                }
                SinkKind::Sqlite(sq) => {
                    let sink = sqlite::SqliteSink::new(sq)?;
                    sinks.insert(Arc::clone(name), SinkEntry::Other { sink });
                }
                SinkKind::Slack(sl) => {
                    let sink = slack::SlackSink::new(sl)?;
                    sinks.insert(Arc::clone(name), SinkEntry::Other { sink });
                }
            }
        }

        Ok(Self::from_entries(sinks, |name| {
            cfgs[name].common.in_flight_limit
        }))
    }

    /// A queue for each sink, and its own `in_flight_limit` of writes at a
    /// time. A write holds its slot while it is retried, so a sink that is
    /// down holds only its own slots and queue, not another sink's.
    fn from_entries(
        sinks: HashMap<Arc<str>, SinkEntry>,
        in_flight_limit: impl Fn(&str) -> usize,
    ) -> Self {
        let mut queues = HashMap::with_capacity(sinks.len());

        for (name, entry) in &sinks {
            let (tx, mut rx) = mpsc::channel::<SinkItem>(4096);
            let entry = entry.clone();
            let sem = Arc::new(Semaphore::new(in_flight_limit(name).max(1)));

            let handle = tokio::spawn(async move {
                let mut js = JoinSet::new();
                loop {
                    tokio::select! {
                        maybe = rx.recv() => {
                            let Some(mut item) = maybe else { break };

                            let sink_name = item.req.sink_name.clone();

                            if let SinkEntry::Wal { bucket, path, key_prefix, .. } = &entry {
                                let now = chrono::Utc::now();
                                let prefix = path
                                    .as_ref()
                                    .and_then(|p| p.route(now))
                                    .or_else(|| key_prefix.as_ref().map(|p| p.render(now)));
                                item.req.s3 = Some(s3::S3SinkItem {
                                    bucket_name: bucket.clone(),
                                    key_prefix: prefix,
                                });
                            } else {
                                item.req.s3 = None;
                            }

                            let sink: Arc<dyn Sink> = match &entry {
                                SinkEntry::Wal { sink, .. } => sink.clone(),
                                SinkEntry::Other { sink } => sink.clone(),
                            };

                            let Ok(permit) = sem.clone().acquire_owned().await else { break };

                            js.spawn(async move {
                                let _permit: OwnedSemaphorePermit = permit;
                                let start = Instant::now();
                                let mut delay = Duration::from_millis(50);
                                // Move on first attempt; reconstruct from a frozen snapshot on retries
                                let frozen = item.req.payload.clone().freeze();
                                let mut first_attempt = true;
                                loop {
                                    let payload_to_send = if first_attempt {
                                        first_attempt = false;
                                        std::mem::take(&mut item.req.payload)
                                    } else {
                                        BytesMut::from(frozen.as_ref())
                                    };

                                    match sink.write(SinkWrite {
                                        sink_name: sink_name.clone(),
                                        payload: payload_to_send,
                                        s3: item.req.s3.clone(),
                                    }).await {
                                        Ok(()) => {
                                            for a in item.acks.drain(..) {
                                                if let Err(e) = a.ack().await {
                                                    tracing::warn!("ack failed: {e}");
                                                }
                                            }
                                            tracing::debug!(
                                                took_us = start.elapsed().as_micros(),
                                                "wrote sink item"
                                            );
                                            INFLIGHT.dec();
                                            break;
                                        }
                                        Err(e) => {
                                            tracing::warn!("sink write failed: {e}");
                                            let j = rng().random_range(0..=delay.as_millis() as u64 / 4);
                                            sleep(delay + Duration::from_millis(j)).await;
                                            delay = (delay * 2).min(Duration::from_secs(5));
                                        }
                                    }
                                }
                            });
                        }
                    }
                }
                while js.join_next().await.is_some() {}
            });

            queues.insert(Arc::clone(name), Queue { tx, handle });
        }

        Self { queues, sinks }
    }

    #[cfg(test)]
    pub(crate) fn for_test(sinks: Vec<(Arc<str>, Arc<dyn Sink>)>, in_flight_limit: usize) -> Self {
        let entries = sinks
            .into_iter()
            .map(|(name, sink)| (name, SinkEntry::Other { sink }))
            .collect();
        Self::from_entries(entries, |_| in_flight_limit)
    }

    pub async fn enqueue(
        &self,
        sink_name: Arc<str>,
        payload: BytesMut,
        acks: Vec<Arc<dyn Ack>>,
    ) -> Result<()> {
        let Some(queue) = self.queues.get(&sink_name) else {
            tracing::warn!("unknown sink '{}'; dropping item", sink_name);
            anyhow::bail!("unknown sink: {sink_name}");
        };

        let sink_item = SinkItem {
            acks,
            req: SinkWrite {
                sink_name: Arc::clone(&sink_name),
                payload: payload,
                s3: None, // filled in the sink's queue
            },
        };

        queue
            .tx
            .send(sink_item)
            .await
            .map(|()| {
                INFLIGHT.inc();
            })
            .map_err(|e| anyhow::anyhow!("send to sink '{sink_name}' failed: {e}"))
    }

    /// Drain and flush everything.
    pub async fn join(self) -> Result<()> {
        let Self { queues, sinks, .. } = self;

        for (nm, q) in queues {
            drop(q.tx);
            if let Err(e) = q.handle.await {
                tracing::warn!("sink '{nm}' queue join error: {e}");
            }
        }

        for (nm, entry) in sinks.iter() {
            let sink: &Arc<dyn Sink> = match entry {
                SinkEntry::Wal { sink, .. } => sink,
                SinkEntry::Other { sink } => sink,
            };
            if let Err(e) = sink.flush().await {
                tracing::warn!("sink '{nm}' flush failed during shutdown: {e}");
                return Err(e);
            }
        }
        Ok(())
    }
}

/// Each WAL directory is one sink's, and none is in a rolling_file sink's
/// directory. A WAL sends every sealed file it finds in its directory to its
/// own sink, so two sinks sharing one send each other's records; a reader of
/// an output directory would find the files a WAL in it stages, and a WAL in
/// the output itself removes each file it stages there, which is the file it
/// has just put in place.
fn check_wal_dirs(cfgs: &BTreeMap<Arc<str>, SinkConfig>) -> Result<()> {
    let mut wals: Vec<(&str, PathBuf)> = Vec::new();
    let mut outs: Vec<(&str, PathBuf)> = Vec::new();
    for (name, cfg) in cfgs {
        match &cfg.kind {
            SinkKind::S3(c) => wals.push((name, resolved(&c.wal_path)?)),
            SinkKind::RollingFile(c) => {
                wals.push((name, resolved(&c.wal_path)?));
                let path = rolling_file::PathTemplate::parse(&c.path)
                    .with_context(|| format!("sink '{name}'"))?;
                outs.push((name, resolved(path.root())?));
            }
            _ => {}
        }
    }
    for (i, (name, wal)) in wals.iter().enumerate() {
        if let Some((other, _)) = wals[..i].iter().find(|(_, w)| w == wal) {
            anyhow::bail!(
                "sinks '{other}' and '{name}' share the WAL directory {}: give each its own wal_path",
                wal.display()
            );
        }
        if let Some((other, out)) = outs.iter().find(|(_, out)| wal.starts_with(out)) {
            anyhow::bail!(
                "sink '{name}' has its WAL in {}, inside sink '{other}''s path {}: put wal_path elsewhere",
                wal.display(),
                out.display()
            );
        }
    }
    Ok(())
}

/// `path` as it would be once it exists: absolute, its symlinks resolved as
/// far as it does exist, without `.` or `..`.
fn resolved(path: &Path) -> Result<PathBuf> {
    let mut out = PathBuf::new();
    for c in std::path::absolute(path)?.components() {
        match c {
            Component::CurDir => {}
            Component::ParentDir => {
                out.pop();
            }
            c => {
                out.push(c);
                if let Ok(real) = out.canonicalize() {
                    out = real;
                }
            }
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::Ack;
    use anyhow::Result;
    use async_trait::async_trait;
    use bytes::BytesMut;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Mutex;

    #[derive(Default)]
    struct RecordingSink {
        writes: Mutex<Vec<Vec<u8>>>,
        prefixes: Mutex<Vec<Option<Arc<str>>>>,
    }

    impl RecordingSink {
        fn new() -> Arc<Self> {
            Arc::new(Self::default())
        }

        async fn take(&self) -> Vec<Vec<u8>> {
            self.writes.lock().await.drain(..).collect()
        }
    }

    #[async_trait]
    impl Sink for RecordingSink {
        async fn write(&self, req: SinkWrite) -> Result<()> {
            let prefix = req.s3.and_then(|m| m.key_prefix);
            self.prefixes.lock().await.push(prefix);
            self.writes.lock().await.push(req.payload.freeze().to_vec());
            Ok(())
        }
    }

    #[derive(Default)]
    struct TestAck {
        count: AtomicUsize,
    }

    impl TestAck {
        fn count(&self) -> usize {
            self.count.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl Ack for TestAck {
        async fn ack(&self) -> Result<()> {
            self.count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn join_drains_inflight_items() {
        let sink_name: Arc<str> = Arc::from("recorder");
        let recorder = RecordingSink::new();
        let manager = SinkManager::for_test(vec![(sink_name.clone(), recorder.clone())], 2);

        let ack = Arc::new(TestAck::default());
        let ack_dyn: Arc<dyn Ack> = ack.clone();
        manager
            .enqueue(
                sink_name.clone(),
                BytesMut::from("{\"msg\":1}\n"),
                vec![ack_dyn],
            )
            .await
            .unwrap();

        manager
            .enqueue(
                sink_name.clone(),
                BytesMut::from("{\"msg\":2}\n"),
                Vec::new(),
            )
            .await
            .unwrap();

        manager.join().await.unwrap();

        let writes = recorder.take().await;
        assert_eq!(writes.len(), 2);
        assert_eq!(ack.count(), 1);
    }

    /// An s3 sink's write carries its `key_prefix` rendered when the write
    /// is routed, as a rolling_file's carries the directory its path renders.
    #[tokio::test]
    async fn an_s3_write_carries_its_key_prefix_rendered_when_routed() {
        let name: Arc<str> = Arc::from("archive");
        let recorder = RecordingSink::new();
        let entry = SinkEntry::Wal {
            sink: recorder.clone(),
            bucket: Arc::from("b"),
            path: None,
            key_prefix: Some(Arc::new(s3::KeyPrefix::parse("logs/%Y/%m/%d").unwrap())),
        };
        let manager = SinkManager::from_entries(HashMap::from([(name.clone(), entry)]), |_| 1);
        let day = || chrono::Utc::now().format("logs/%Y/%m/%d").to_string();
        let before = day();
        manager
            .enqueue(name, BytesMut::from("{}\n"), Vec::new())
            .await
            .unwrap();
        manager.join().await.unwrap();
        let after = day();
        let prefixes = recorder.prefixes.lock().await.clone();
        let [Some(got)] = prefixes.as_slice() else {
            panic!("{prefixes:?}")
        };
        assert!([before, after].contains(&got.to_string()), "{got}");
    }

    /// `tangent run` stops at a key_prefix that is not a strftime template,
    /// naming the sink, before any sink starts: a sink named before it has
    /// not made its WAL, so none recovers or sends what it finds there.
    #[tokio::test]
    async fn a_key_prefix_not_strftime_stops_the_sinks_at_load() {
        let root = std::env::temp_dir().join(format!("tangent-badprefix-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let s3 = |wal: &str, extra: &str| -> SinkConfig {
            let wal = root.join(wal);
            let yaml = format!(
                "type: s3\nbucket_name: b\nwal_path: {}\n{extra}",
                wal.display()
            );
            serde_yaml::from_str(&yaml).unwrap()
        };
        // 'a' sorts first, so it would be built first
        let cfgs: BTreeMap<Arc<str>, SinkConfig> = BTreeMap::from([
            (Arc::from("a"), s3("a-wal", "")),
            (
                Arc::from("archive"),
                s3("archive-wal", "key_prefix: logs/%Q\n"),
            ),
        ]);
        let Err(e) = SinkManager::new(&cfgs).await else {
            panic!("a bad key_prefix loaded");
        };
        assert!(format!("{e:#}").contains("sink 'archive'"), "{e:#}");
        assert!(!root.join("a-wal").exists(), "sink 'a' made its WAL");
        let _ = std::fs::remove_dir_all(&root);
    }

    /// Fails every write until `up` is set, as a sink whose destination is
    /// down does.
    #[derive(Default)]
    struct DownSink {
        up: std::sync::atomic::AtomicBool,
        tries: AtomicUsize,
    }

    #[async_trait]
    impl Sink for DownSink {
        async fn write(&self, _: SinkWrite) -> Result<()> {
            self.tries.fetch_add(1, Ordering::SeqCst);
            if self.up.load(Ordering::SeqCst) {
                Ok(())
            } else {
                anyhow::bail!("down")
            }
        }
    }

    #[tokio::test]
    async fn a_sink_that_is_down_does_not_stop_the_others() {
        let (alerts, lake): (Arc<str>, Arc<str>) = (Arc::from("alerts"), Arc::from("lake"));
        let down = Arc::new(DownSink::default());
        let recorder = RecordingSink::new();
        let manager = SinkManager::for_test(
            vec![
                (alerts.clone(), down.clone() as Arc<dyn Sink>),
                (lake.clone(), recorder.clone() as Arc<dyn Sink>),
            ],
            1,
        );
        let ack = Arc::new(TestAck::default());
        // one write holds alerts' write slot while it is retried, the next waits for it
        for _ in 0..2 {
            manager
                .enqueue(
                    alerts.clone(),
                    BytesMut::from("{\"a\":1}\n"),
                    vec![ack.clone() as Arc<dyn Ack>],
                )
                .await
                .unwrap();
        }
        // alerts is down, and being retried, when the lake gets a write
        while down.tries.load(Ordering::SeqCst) == 0 {
            sleep(Duration::from_millis(10)).await;
        }
        manager
            .enqueue(
                lake.clone(),
                BytesMut::from("{\"l\":1}\n"),
                vec![ack.clone() as Arc<dyn Ack>],
            )
            .await
            .unwrap();
        let lake_written = tokio::time::timeout(Duration::from_secs(2), async {
            while recorder.writes.lock().await.is_empty() {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        assert!(
            lake_written.is_ok(),
            "lake had no write while alerts was down"
        );
        assert_eq!(ack.count(), 1, "only the lake's write is acked");

        down.up.store(true, Ordering::SeqCst);
        manager.join().await.unwrap();
        assert_eq!(ack.count(), 3, "alerts' writes are acked once it is up");
    }

    fn s3(wal: &str) -> SinkConfig {
        let wal = if wal.is_empty() {
            String::new()
        } else {
            format!("wal_path: {wal}\n")
        };
        serde_yaml::from_str(&format!("type: s3\nbucket_name: b\n{wal}")).unwrap()
    }

    #[test]
    fn a_wal_directory_is_one_sinks() {
        let root = std::env::temp_dir().join(format!("tangent-s3wal-{}", std::process::id()));
        let check = |sinks: Vec<(&str, SinkConfig)>| {
            let cfgs: BTreeMap<Arc<str>, SinkConfig> =
                sinks.into_iter().map(|(n, c)| (Arc::from(n), c)).collect();
            check_wal_dirs(&cfgs)
        };
        let (a, b) = (root.join("a"), root.join("b"));
        let (a, b) = (a.to_str().unwrap(), b.to_str().unwrap());
        assert!(check(vec![("a", s3(a)), ("b", s3(b))]).is_ok());

        // both on the default /tmp/wal
        let default = check(vec![("a", s3("")), ("b", s3(""))]).unwrap_err();
        assert!(default.to_string().contains("share the WAL"), "{default}");
        // the same directory, spelled another way and not there yet
        let other = root.join("x/../a/.");
        let shared = check(vec![("a", s3(a)), ("b", s3(other.to_str().unwrap()))]).unwrap_err();
        assert!(shared.to_string().contains("share the WAL"), "{shared}");
    }

    fn rolling_file(path: &Path, wal: &Path) -> SinkConfig {
        serde_yaml::from_str(&format!(
            "type: rolling_file\npath: {}\nwal_path: {}\n",
            path.display(),
            wal.display()
        ))
        .unwrap()
    }

    #[test]
    fn a_wal_directory_is_one_sinks_and_outside_every_output() {
        let root = std::env::temp_dir().join(format!("tangent-waldirs-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let check = |sinks: Vec<(&str, SinkConfig)>| {
            let cfgs: BTreeMap<Arc<str>, SinkConfig> =
                sinks.into_iter().map(|(n, c)| (Arc::from(n), c)).collect();
            check_wal_dirs(&cfgs)
        };
        let (out, wal) = (root.join("out"), root.join("wal"));
        assert!(check(vec![("a", rolling_file(&out, &wal))]).is_ok());

        let same = check(vec![("a", rolling_file(&out, &out))]).unwrap_err();
        assert!(
            same.to_string().contains("inside sink 'a''s path"),
            "{same}"
        );
        let below = check(vec![("a", rolling_file(&out, &out.join("wal")))]).unwrap_err();
        assert!(below.to_string().contains("inside"), "{below}");
        // the same directory, spelled another way and not there yet
        let shared = check(vec![
            ("a", rolling_file(&out, &wal)),
            (
                "b",
                rolling_file(&root.join("out2"), &root.join("x/../wal/.")),
            ),
        ])
        .unwrap_err();
        assert!(shared.to_string().contains("share the WAL"), "{shared}");
        let into_other = check(vec![
            ("a", rolling_file(&out, &wal)),
            ("b", rolling_file(&root.join("out2"), &out.join("b-wal"))),
        ])
        .unwrap_err();
        assert!(
            into_other.to_string().contains("sink 'a''s path"),
            "{into_other}"
        );
        let _ = std::fs::remove_dir_all(root);
    }
}
