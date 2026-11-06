use anyhow::{Context, Result};
use async_trait::async_trait;
use bytes::BytesMut;
use chrono::{DateTime, Utc};
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
use crate::sinks::event_time::{EventTime, FieldPath};
use crate::sinks::file;
use crate::sinks::rolling_file;
use crate::sinks::s3::S3SinkItem;
use crate::sinks::slack;
use crate::sinks::sqlite;
use crate::sinks::template::Template;
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
    /// bucket (s3's; empty for rolling_file) and the key prefix `prefix`
    /// renders: a rolling_file's directory below its `path`'s root, an s3
    /// sink's `key_prefix`. With `time`, each record goes under the prefix
    /// its own time renders; without, the prefix has no specifier, and a
    /// frame goes whole under it.
    Wal {
        sink: Arc<dyn Sink>,
        bucket: Arc<str>,
        prefix: Option<Arc<Template>>,
        time: Option<Arc<EventTime>>,
    },
    Other {
        sink: Arc<dyn Sink>,
    },
}

impl SinkEntry {
    /// A WAL sink whose prefix renders each record's own time, read from
    /// `field`: only a prefix with a specifier reads one.
    fn wal(
        name: &Arc<str>,
        sink: Arc<dyn Sink>,
        bucket: Arc<str>,
        prefix: Option<Template>,
        field: FieldPath,
    ) -> Self {
        let time = prefix
            .as_ref()
            .filter(|p| p.timed())
            .map(|_| Arc::new(EventTime::new(name, field)));
        Self::Wal {
            sink,
            bucket,
            prefix: prefix.map(Arc::new),
            time,
        }
    }

    fn sink(&self) -> &Arc<dyn Sink> {
        match self {
            Self::Wal { sink, .. } | Self::Other { sink } => sink,
        }
    }

    /// The writes a frame reaching the sink at `now` makes, each with its
    /// route: the frame whole, or a part for each prefix its records' times
    /// render.
    fn writes(&self, frame: BytesMut, now: DateTime<Utc>) -> Vec<(Option<S3SinkItem>, BytesMut)> {
        let Self::Wal {
            bucket,
            prefix,
            time,
            ..
        } = self
        else {
            return vec![(None, frame)];
        };
        let route = |key_prefix| {
            Some(S3SinkItem {
                bucket_name: bucket.clone(),
                key_prefix,
            })
        };
        match (prefix, time) {
            (Some(p), Some(t)) => t
                .split(frame, p, now)
                .into_iter()
                .map(|(key_prefix, part)| (route(Some(key_prefix)), part))
                .collect(),
            _ => vec![(route(prefix.as_ref().map(|p| p.render(now))), frame)],
        }
    }
}

pub struct SinkManager {
    queues: HashMap<Arc<str>, Queue>,
    sinks: HashMap<Arc<str>, SinkEntry>,
}

impl SinkManager {
    pub async fn new(cfgs: &BTreeMap<Arc<str>, SinkConfig>) -> Result<Self> {
        check_wal_dirs(cfgs)?;
        s3::check_sinks(cfgs)?;
        for (name, cfg) in cfgs {
            encoding::check(&cfg.common.encoding, &cfg.common.compression)
                .with_context(|| format!("sink '{name}'"))?;
        }
        let mut sinks: HashMap<Arc<str>, SinkEntry> = HashMap::with_capacity(cfgs.len());

        for (name, cfg) in cfgs {
            match &cfg.kind {
                SinkKind::S3(s3cfg) => {
                    let prefix = s3cfg
                        .key_prefix
                        .as_deref()
                        .map(s3::key_prefix)
                        .transpose()
                        .with_context(|| format!("sink '{name}'"))?;
                    let field = FieldPath::parse(&s3cfg.timestamp_field)
                        .with_context(|| format!("sink '{name}'"))?;
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
                        Arc::clone(name),
                        SinkEntry::wal(
                            name,
                            s3_sink,
                            Arc::from(s3cfg.bucket_name.as_str()),
                            prefix,
                            field,
                        ),
                    );
                }
                SinkKind::RollingFile(rcfg) => {
                    let path = rolling_file::PathTemplate::parse(&rcfg.path)
                        .with_context(|| format!("sink '{name}'"))?;
                    let field = FieldPath::parse(&rcfg.timestamp_field)
                        .with_context(|| format!("sink '{name}'"))?;
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
                        SinkEntry::wal(name, wal, Arc::from(""), path.into_rest(), field),
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

                            // the frame's own task takes a copy of the entry: its Arcs
                            let entry = entry.clone();
                            // what a record without a time of its own goes under
                            let now = Utc::now();

                            let Ok(permit) = sem.clone().acquire_owned().await else { break };

                            // the split reads every record, so it is done in the
                            // frame's own task: every frame of a sink passes
                            // through this one loop, which would read them one
                            // frame at a time
                            js.spawn(async move {
                                let _permit: OwnedSemaphorePermit = permit;
                                let start = Instant::now();
                                let frame = std::mem::take(&mut item.req.payload);
                                // the frame is acked once every part is in, and a
                                // part in is not written again
                                for (route, part) in entry.writes(frame, now) {
                                    write_until_in(entry.sink(), &sink_name, route, part).await;
                                }
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
            if let Err(e) = entry.sink().flush().await {
                tracing::warn!("sink '{nm}' flush failed during shutdown: {e}");
                return Err(e);
            }
        }
        Ok(())
    }
}

/// Writes `payload` until the sink takes it, backing off between tries.
async fn write_until_in(
    sink: &Arc<dyn Sink>,
    sink_name: &Arc<str>,
    route: Option<S3SinkItem>,
    payload: BytesMut,
) {
    let mut delay = Duration::from_millis(50);
    // moved on the first try; each retry copies a frozen snapshot
    let frozen = payload.clone().freeze();
    let mut first = Some(payload);
    loop {
        let write = SinkWrite {
            sink_name: sink_name.clone(),
            payload: first
                .take()
                .unwrap_or_else(|| BytesMut::from(frozen.as_ref())),
            s3: route.clone(),
        };
        match sink.write(write).await {
            Ok(()) => return,
            Err(e) => {
                tracing::warn!("sink write failed: {e}");
                let j = rng().random_range(0..=delay.as_millis() as u64 / 4);
                sleep(delay + Duration::from_millis(j)).await;
                delay = (delay * 2).min(Duration::from_secs(5));
            }
        }
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
    use tangent_shared::sinks::common::{Compression, Encoding};
    use tokio::sync::Mutex;

    /// Each write's key prefix (empty without one) and payload, in the
    /// order written.
    #[derive(Default)]
    struct RecordingSink {
        writes: Mutex<Vec<(String, Vec<u8>)>>,
    }

    impl RecordingSink {
        fn new() -> Arc<Self> {
            Arc::new(Self::default())
        }

        async fn take(&self) -> Vec<Vec<u8>> {
            self.writes.lock().await.drain(..).map(|w| w.1).collect()
        }

        async fn got(&self) -> Vec<(String, String)> {
            let writes = self.writes.lock().await;
            writes
                .iter()
                .map(|(prefix, payload)| {
                    (prefix.clone(), String::from_utf8(payload.clone()).unwrap())
                })
                .collect()
        }
    }

    #[async_trait]
    impl Sink for RecordingSink {
        async fn write(&self, req: SinkWrite) -> Result<()> {
            let prefix = req.s3.and_then(|m| m.key_prefix);
            let prefix = prefix.as_deref().unwrap_or_default().to_string();
            let payload = req.payload.freeze().to_vec();
            self.writes.lock().await.push((prefix, payload));
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

    /// An s3 sink's entry over `sink`, its key_prefix `prefix` read from
    /// each record's `field`, as `SinkManager::new` makes one.
    fn s3_entry(name: &Arc<str>, sink: Arc<dyn Sink>, prefix: &str, field: &str) -> SinkEntry {
        let prefix = Some(s3::key_prefix(prefix).unwrap());
        let field = FieldPath::parse(field).unwrap();
        SinkEntry::wal(name, sink, Arc::from("b"), prefix, field)
    }

    fn manager_of(name: &Arc<str>, entry: SinkEntry) -> SinkManager {
        SinkManager::from_entries(HashMap::from([(name.clone(), entry)]), |_| 1)
    }

    fn record(ts: &str, n: usize) -> String {
        format!("{{\"ts\":\"{ts}\",\"n\":{n}}}\n")
    }

    fn untimed(sink: &str) -> u64 {
        crate::SINK_RECORDS_UNTIMED_TOTAL
            .with_label_values(&[sink])
            .get()
    }

    const DAY1: &str = "2026-11-01T23:59:59Z";
    const DAY2: &str = "2026-11-02T00:00:00Z";

    /// An s3 sink's key_prefix renders each record's own time, in UTC: a
    /// frame of two days is a write under each, each day's records in the
    /// frame's order, and its ack follows both. A blank line goes with the
    /// record before it, and a last line without its `\n` is a record.
    #[tokio::test]
    async fn a_frame_of_two_days_is_written_under_two_prefixes() {
        let name: Arc<str> = Arc::from("two-days");
        let recorder = RecordingSink::new();
        let manager = manager_of(
            &name,
            s3_entry(&name, recorder.clone(), "logs/%Y/%m/%d", "ts"),
        );
        // 01:30 on the 2nd in UTC
        let west = "2026-11-01T17:30:00-08:00";
        let last = record(west, 3);
        let last = last.trim_end();
        let frame = [
            record(DAY1, 0),
            "\n".into(),
            record(DAY2, 1),
            record(DAY1, 2),
        ]
        .concat()
            + last;
        let ack = Arc::new(TestAck::default());
        manager
            .enqueue(name, BytesMut::from(frame.as_str()), vec![ack.clone()])
            .await
            .unwrap();
        manager.join().await.unwrap();
        assert_eq!(
            recorder.got().await,
            [
                (
                    "logs/2026/11/01".to_string(),
                    record(DAY1, 0) + "\n" + &record(DAY1, 2)
                ),
                ("logs/2026/11/02".to_string(), record(DAY2, 1) + last),
            ]
        );
        assert_eq!(ack.count(), 1);
        assert_eq!(untimed("two-days"), 0, "a blank line is no record");
    }

    /// A dotted `timestamp_field` is read as a node's `group_by` reads one:
    /// a key holding all of it, as a flattened record has, or else the
    /// field inside an object.
    #[tokio::test]
    async fn a_dotted_timestamp_field_reads_a_flat_key_or_a_nested_time() {
        let name: Arc<str> = Arc::from("dotted");
        let recorder = RecordingSink::new();
        let manager = manager_of(
            &name,
            s3_entry(&name, recorder.clone(), "logs/%Y/%m/%d", "event.time"),
        );
        let nested = format!("{{\"event\":{{\"time\":\"{DAY1}\"}}}}\n");
        let flat = format!("{{\"event.time\":\"{DAY2}\"}}\n");
        manager
            .enqueue(
                name,
                BytesMut::from((nested.clone() + &flat).as_str()),
                Vec::new(),
            )
            .await
            .unwrap();
        manager.join().await.unwrap();
        assert_eq!(
            recorder.got().await,
            [
                ("logs/2026/11/01".to_string(), nested),
                ("logs/2026/11/02".to_string(), flat),
            ]
        );
        assert_eq!(untimed("dotted"), 0);
    }

    /// A record without an RFC 3339 time in its field goes under the time
    /// it reaches the sink, and is counted for its sink: it is not dropped.
    #[tokio::test]
    async fn a_record_without_a_time_goes_under_the_time_it_reaches_the_sink_and_is_counted() {
        let name: Arc<str> = Arc::from("untimed");
        let recorder = RecordingSink::new();
        let manager = manager_of(
            &name,
            s3_entry(&name, recorder.clone(), "logs/%Y/%m/%d", "ts"),
        );
        let without = [
            "{\"n\":0}\n",
            "{\"ts\":1792501200,\"n\":1}\n",
            "{\"ts\":\"noon\",\"n\":2}\n",
            "not json\n",
        ]
        .concat();
        let day = || Utc::now().format("logs/%Y/%m/%d").to_string();
        let before = day();
        manager
            .enqueue(
                name,
                BytesMut::from((without.clone() + &record(DAY1, 4)).as_str()),
                Vec::new(),
            )
            .await
            .unwrap();
        manager.join().await.unwrap();
        let got = recorder.got().await;
        assert_eq!(got.len(), 2, "{got:?}");
        assert!([before, day()].contains(&got[0].0), "{got:?}");
        assert_eq!(got[0].1, without);
        assert_eq!(got[1], ("logs/2026/11/01".to_string(), record(DAY1, 4)));
        assert_eq!(untimed("untimed"), 4);
    }

    /// A key_prefix without a specifier renders the same for every time, so
    /// the sink reads none: the frame goes whole under it, and nothing is
    /// counted.
    #[tokio::test]
    async fn a_static_prefix_reads_no_time() {
        let name: Arc<str> = Arc::from("static");
        let recorder = RecordingSink::new();
        let entry = s3_entry(&name, recorder.clone(), "logs/cloudtrail", "ts");
        assert!(matches!(entry, SinkEntry::Wal { time: None, .. }));
        let manager = manager_of(&name, entry);
        let frame = [record(DAY2, 0), "not json\n".into(), record(DAY1, 2)].concat();
        manager
            .enqueue(name, BytesMut::from(frame.as_str()), Vec::new())
            .await
            .unwrap();
        manager.join().await.unwrap();
        assert_eq!(
            recorder.got().await,
            [("logs/cloudtrail".to_string(), frame)]
        );
        assert_eq!(untimed("static"), 0);
    }

    /// Fails every write under `failing` until `released`, then records as
    /// [`RecordingSink`] does.
    struct FailsUnder {
        failing: &'static str,
        released: std::sync::atomic::AtomicBool,
        failed: AtomicUsize,
        rec: RecordingSink,
    }

    #[async_trait]
    impl Sink for FailsUnder {
        async fn write(&self, req: SinkWrite) -> Result<()> {
            let prefix = req.s3.as_ref().and_then(|m| m.key_prefix.as_deref());
            if prefix == Some(self.failing) && !self.released.load(Ordering::SeqCst) {
                self.failed.fetch_add(1, Ordering::SeqCst);
                anyhow::bail!("the bucket is down");
            }
            self.rec.write(req).await
        }
    }

    /// A frame is acked once every part is in: a part that fails holds the
    /// ack back, and is tried again alone, the part already in not written
    /// twice.
    #[tokio::test]
    async fn a_frames_ack_waits_for_every_part_and_a_failed_part_alone_is_tried_again() {
        let name: Arc<str> = Arc::from("fails");
        let sink = Arc::new(FailsUnder {
            failing: "logs/2026/11/02",
            released: Default::default(),
            failed: Default::default(),
            rec: RecordingSink::default(),
        });
        let manager = manager_of(&name, s3_entry(&name, sink.clone(), "logs/%Y/%m/%d", "ts"));
        let frame = [record(DAY1, 0), record(DAY2, 1), record(DAY1, 2)].concat();
        let ack = Arc::new(TestAck::default());
        manager
            .enqueue(name, BytesMut::from(frame.as_str()), vec![ack.clone()])
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while sink.failed.load(Ordering::SeqCst) < 2 {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the second day's part fails, and fails again");
        assert_eq!(ack.count(), 0, "acked with a part not in");
        let day1 = (
            "logs/2026/11/01".to_string(),
            record(DAY1, 0) + &record(DAY1, 2),
        );
        assert_eq!(sink.rec.got().await, vec![day1.clone()]);
        sink.released.store(true, Ordering::SeqCst);
        manager.join().await.unwrap();
        assert_eq!(
            sink.rec.got().await,
            [day1, ("logs/2026/11/02".to_string(), record(DAY2, 1))]
        );
        assert_eq!(ack.count(), 1);
    }

    /// Each sealed file's key prefix, from its meta.
    #[derive(Default)]
    struct PrefixRecorder {
        got: Mutex<Vec<(String, String)>>,
    }

    #[async_trait]
    impl wal::WALSink for PrefixRecorder {
        async fn write_path_with(
            &self,
            path: &Path,
            _name: &str,
            _encoding: &Encoding,
            _compression: &Compression,
            meta: &S3SinkItem,
        ) -> Result<()> {
            let prefix = meta.key_prefix.as_deref().unwrap_or_default().to_string();
            let bytes = std::fs::read_to_string(path)?;
            self.got.lock().await.push((prefix, bytes));
            Ok(())
        }
    }

    /// The parts of a frame are WAL files of their own, each with its
    /// prefix in its meta: a start after a stop without a flush sends each
    /// under its own.
    #[tokio::test]
    async fn a_restart_sends_each_wal_file_under_its_own_prefix() {
        let dir = std::env::temp_dir().join(format!("tangent-evt-restart-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let wal_on = |sink: Arc<PrefixRecorder>| {
            wal::DurableFileSink::new(
                sink,
                &dir,
                4,
                1 << 20,
                Duration::from_secs(3600),
                Compression::None,
                Encoding::NDJSON,
            )
        };
        let gone = Arc::new(PrefixRecorder::default());
        let first = wal_on(gone.clone()).await.unwrap();
        let name: Arc<str> = Arc::from("restart");
        let manager = manager_of(&name, s3_entry(&name, first.clone(), "logs/%Y/%m/%d", "ts"));
        let frame = [record(DAY1, 0), record(DAY2, 1), record(DAY1, 2)].concat();
        let ack = Arc::new(TestAck::default());
        manager
            .enqueue(name, BytesMut::from(frame.as_str()), vec![ack.clone()])
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while ack.count() == 0 {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("acked once in the WAL");
        // killed: no join, no flush
        first.stop_without_flush().await;
        drop((manager, first));

        let rec = Arc::new(PrefixRecorder::default());
        let second = wal_on(rec.clone()).await.unwrap();
        second.flush().await.unwrap();
        let mut got = rec.got.lock().await.clone();
        got.sort();
        assert_eq!(
            got,
            [
                (
                    "logs/2026/11/01".to_string(),
                    record(DAY1, 0) + &record(DAY1, 2)
                ),
                ("logs/2026/11/02".to_string(), record(DAY2, 1)),
            ]
        );
        assert!(gone.got.lock().await.is_empty());
        let _ = std::fs::remove_dir_all(dir);
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
