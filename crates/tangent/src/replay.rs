//! `tangent replay`: a past range of a config's sources through it, in one
//! process. Each source whose past replay can read goes in an hour at a
//! time, as fast as it is read: a sweep of the range for bad behavior, not a
//! re-run of live, so an actor's group can hold more of its records than
//! live would have closed together. After the last hour every open group
//! closes, and replay ends once each is through: the judge has answered and
//! the sinks have what it said.

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, ensure, Result};
use async_trait::async_trait;
use bytes::BytesMut;
use chrono::{DateTime, TimeDelta, Utc};
use parking_lot::Mutex;
use serde_json::Value;
use tangent_shared::dag::NodeRef;
use tangent_shared::sinks::blackhole::BlackholeConfig;
use tangent_shared::sinks::common::{Encoding, SinkKind};
use tangent_shared::sources::common::SourceConfig;
use tangent_shared::Config;
use tokio::sync::oneshot;
use tokio_util::sync::CancellationToken;

use crate::dag::DagRuntime;
use crate::router::Router;
use crate::sources::cloudtrail_history;
use crate::sources::decoding::chunk_ndjson;
use crate::sources::poll::frame;
use crate::wait_for_shutdown_signal;
use crate::worker::Ack;

pub struct ReplayOptions {
    pub config: PathBuf,
    /// The range, `[from, to)`: each a date (midnight UTC), an RFC 3339
    /// time, or a span before now (`1d`, `12h`). Without `to`, five minutes
    /// ago.
    pub from: String,
    pub to: Option<String>,
    /// Where the history replay builds is kept, as `state.sqlite`: it starts
    /// empty, and a live pipeline's is left as it is.
    pub state_dir: PathBuf,
}

/// A source's past, as replay reads it: the range's records in time order,
/// an hour at a time.
#[async_trait]
pub trait Past: Send {
    /// The range's records from before `t` not returned yet, each as the
    /// live source hands it on (its `source` and `ts` stamped).
    async fn until(&mut self, t: DateTime<Utc>) -> Result<Vec<Value>>;
    /// What the range holds, said before the DAG starts.
    fn size(&self) -> String;
    /// About how many records the range holds, once its first hour is read.
    fn estimate(&self) -> Option<u64>;
    /// What was read, for the summary.
    fn read(&self) -> String;
}

/// The past of the source `name`, when replay can read its kind's.
async fn past(
    name: &str,
    src: &SourceConfig,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<Option<Box<dyn Past>>> {
    match src {
        SourceConfig::Cloudtrail(c) => Ok(Some(Box::new(
            cloudtrail_history::past(name, c, from, to).await?,
        ))),
        _ => Ok(None),
    }
}

/// A source replay reads, where its records go, and how many it has sent.
struct Replayed {
    name: Arc<str>,
    node: NodeRef,
    past: Box<dyn Past>,
    records: u64,
}

pub async fn replay(opts: ReplayOptions) -> Result<()> {
    let mut cfg = Config::from_file(&opts.config)?;
    cfg.validate()?;
    let (from, to) = range(&opts.from, opts.to.as_deref())?;

    // A source with a past is read here and stays in the config, its edges
    // taking what is read on; the rest would mix today's records in.
    let mut sources = Vec::new();
    for (name, src) in std::mem::take(&mut cfg.sources) {
        match past(&name, &src, from, to).await? {
            Some(past) => {
                sources.push(Replayed {
                    node: NodeRef::Source {
                        name: Arc::clone(&name),
                    },
                    name: Arc::clone(&name),
                    past,
                    records: 0,
                });
                cfg.sources.insert(name, src);
            }
            None => {
                tracing::warn!(source = %name, "replay reads only cloudtrail sources' past; this source is left out")
            }
        }
    }
    ensure!(
        !sources.is_empty(),
        "replay reads a cloudtrail source's Event history, and {} has no cloudtrail source",
        opts.config.display()
    );
    sweep(cfg, &opts.config, &opts.state_dir, sources, from, to).await
}

/// `[from, to)`, `to` five minutes ago when left out: CloudTrail typically
/// has an event in Event history within five minutes of its call, so a range
/// up to now would miss the last few minutes' late events.
fn range(from: &str, to: Option<&str>) -> Result<(DateTime<Utc>, DateTime<Utc>)> {
    let from = parse_when(from)?;
    let to = match to {
        Some(to) => parse_when(to)?,
        None => Utc::now() - cloudtrail_history::lag(),
    };
    ensure!(from < to, "from ({from}) is not before to ({to})");
    Ok((from, to))
}

/// The range of `sources` through `cfg` an hour at a time, then every group
/// closed and judged, then what it read and wrote.
async fn sweep(
    mut cfg: Config,
    path: &Path,
    state_dir: &Path,
    mut sources: Vec<Replayed>,
    from: DateTime<Utc>,
    to: DateTime<Utc>,
) -> Result<()> {
    // The history it builds starts empty, and is its own to write: folded
    // into a live pipeline's, it would be neither.
    cfg.runtime.state.path = state_dir.join("state.sqlite");
    cfg.runtime.state.read_only = false;
    keep_local(&mut cfg);
    let outputs = outputs(&cfg);

    let started = Instant::now();
    let mut hour = (from + TimeDelta::hours(1)).min(to);
    let mut first = Vec::new();
    for s in &mut sources {
        first.push(s.past.until(hour).await?);
    }
    println!("replay {} to {}", when(from), when(to));
    for s in &sources {
        let records = s
            .past
            .estimate()
            .map(|n| format!("; about {} records", thousands(n)))
            .unwrap_or_default();
        println!("  {}: {}{records}", s.name, s.past.size());
    }

    let chunks = cfg.batch_size_kb();
    let stop_models = CancellationToken::new();
    let dag = DagRuntime::fed(cfg, path, stop_models.clone()).await?;

    let mut read = false;
    let swept = tokio::select! {
        r = async {
            step(&mut sources, first, &mut hour, to, &dag, chunks).await?;
            read = true;
            tracing::info!("replay: the range is read; closing every group");
            dag.finish().await;
            Ok::<_, anyhow::Error>(())
        } => r,
        r = wait_for_shutdown_signal() => {
            r?;
            stop_models.cancel();
            // a signal within a second of the first is the same stop
            tokio::spawn(async {
                tokio::time::sleep(Duration::from_secs(1)).await;
                if wait_for_shutdown_signal().await.is_ok() {
                    tracing::warn!("second signal; exiting now");
                    std::process::exit(130);
                }
            });
            Err(if read {
                anyhow!("stopped while the last groups were judged: the range was read, but groups not yet judged were left out")
            } else {
                anyhow!("stopped before {}: the rest of the range, and groups not yet judged, were left out", when(hour))
            })
        }
    };

    dag.shutdown(Duration::from_secs(120), Duration::from_secs(120))
        .await?;

    let records: u64 = sources.iter().map(|s| s.records).sum();
    println!(
        "replayed {} records in {}",
        thousands(records),
        took(started.elapsed())
    );
    for s in &sources {
        println!(
            "  {}: {} records {}",
            s.name,
            thousands(s.records),
            s.past.read()
        );
    }
    for (path, before) in outputs {
        match lines_after(&path, before) {
            Ok(n) => println!("  {}: {} records", path.display(), thousands(n)),
            Err(e) => println!("  {}: not read ({e})", path.display()),
        }
    }
    swept
}

/// Records replay lets wait in groups behind their key's before it reads
/// the next hour. Parsed, a CloudTrail record takes about 6 KB, so this is
/// about 300 MB. A key sends one group at a time, so reading further ahead
/// would not get the judge through them any sooner.
const WAITING: usize = 50_000;

/// Each hour's records into the DAG, until the range is read.
async fn step(
    sources: &mut [Replayed],
    first: Vec<Vec<Value>>,
    hour: &mut DateTime<Utc>,
    to: DateTime<Utc>,
    dag: &DagRuntime,
    chunks: usize,
) -> Result<()> {
    let mut got = first;
    loop {
        for (s, records) in sources.iter_mut().zip(got) {
            s.records += records.len() as u64;
            store(&dag.router, &s.node, frames(records, chunks)?).await?;
        }
        tracing::info!(hour = %when(*hour), "replay: stored up to");
        if *hour >= to {
            return Ok(());
        }
        // A busy actor's next groups wait in memory; on a long range they
        // would fill it.
        if dag.waiting() > WAITING {
            tracing::info!(
                waiting = dag.waiting(),
                "replay: waiting for the judge to catch up"
            );
            while dag.waiting() > WAITING {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        *hour = (*hour + TimeDelta::hours(1)).min(to);
        got = Vec::new();
        for s in sources.iter_mut() {
            got.push(s.past.until(*hour).await?);
        }
    }
}

/// Records as NDJSON frames of about `chunks` bytes, as a source hands them on.
fn frames(records: Vec<Value>, chunks: usize) -> Result<Vec<BytesMut>> {
    let mut ndjson = BytesMut::new();
    for v in &records {
        ndjson.extend_from_slice(&frame(v)?);
    }
    Ok(chunk_ndjson(&mut ndjson, chunks))
}

/// Hands `frames` to the DAG as the source `node` would, and waits until
/// each record is in its group or written by its sinks.
async fn store(router: &Router, node: &NodeRef, frames: Vec<BytesMut>) -> Result<()> {
    if frames.is_empty() {
        return Ok(());
    }
    let (tx, rx) = oneshot::channel();
    router
        .forward(node, frames, vec![Arc::new(Stored(Mutex::new(Some(tx))))])
        .await?;
    if rx.await.is_err() {
        tracing::warn!("the DAG let go of records without taking them; the judge may miss some");
    }
    Ok(())
}

struct Stored(Mutex<Option<oneshot::Sender<()>>>);

#[async_trait]
impl Ack for Stored {
    async fn ack(&self) -> Result<()> {
        if let Some(tx) = self.0.lock().take() {
            let _ = tx.send(());
        }
        Ok(())
    }
}

/// Sinks that post, upload or archive become blackholes: a replay of last
/// month sends no alerts and archives nothing a second time. A rolling_file
/// sink's WAL is the live pipeline's when replay runs beside it, and opening
/// it would seal and publish the file the live process is writing.
fn keep_local(cfg: &mut Config) {
    for (name, sink) in &mut cfg.sinks {
        if matches!(
            sink.kind,
            SinkKind::Slack(_) | SinkKind::S3(_) | SinkKind::RollingFile(_)
        ) {
            tracing::warn!(sink = %name, "replay sends and archives nothing; this sink is left out");
            sink.kind = SinkKind::Blackhole(BlackholeConfig {});
        }
    }
}

/// Each NDJSON file sink and its size now: what the replay adds comes after.
fn outputs(cfg: &Config) -> Vec<(PathBuf, u64)> {
    let mut out = Vec::new();
    for sink in cfg.sinks.values() {
        if let (SinkKind::File(f), Encoding::NDJSON) = (&sink.kind, &sink.common.encoding) {
            let size = std::fs::metadata(&f.path).map_or(0, |m| m.len());
            out.push((f.path.clone(), size));
        }
    }
    out
}

fn lines_after(path: &Path, from: u64) -> std::io::Result<u64> {
    let mut f = File::open(path)?;
    f.seek(SeekFrom::Start(from))?;
    let (mut buf, mut n) = (vec![0u8; 1 << 16], 0);
    loop {
        match f.read(&mut buf)? {
            0 => return Ok(n),
            k => n += memchr::memchr_iter(b'\n', &buf[..k]).count() as u64,
        }
    }
}

/// `2026-10-01` (midnight UTC), an RFC 3339 time, or a span before now:
/// `1d`, `12h`.
pub fn parse_when(s: &str) -> Result<DateTime<Utc>> {
    let s = s.trim();
    let ago = |unit: char, span: fn(i64) -> Option<TimeDelta>| {
        s.strip_suffix(unit)
            .and_then(|n| n.parse::<u32>().ok())
            .and_then(|n| span(n.into()))
            .map(|d| Utc::now() - d)
    };
    if let Some(t) = ago('d', TimeDelta::try_days).or_else(|| ago('h', TimeDelta::try_hours)) {
        return Ok(t);
    }
    s.parse::<DateTime<Utc>>()
        .or_else(|_| {
            chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map(|d| d.and_hms_opt(0, 0, 0).unwrap().and_utc())
        })
        .map_err(|_| {
            anyhow!(
                "{s:?}: want a date (2026-10-01), an RFC 3339 time, or a span before now (1d, 12h)"
            )
        })
}

fn when(t: DateTime<Utc>) -> String {
    t.format("%Y-%m-%dT%H:%MZ").to_string()
}

fn took(d: Duration) -> String {
    match d.as_secs() {
        s @ 0..60 => format!("{s}s"),
        s @ 60..3600 => format!("{}m {:02}s", s / 60, s % 60),
        s => format!("{}h {:02}m", s / 3600, s % 3600 / 60),
    }
}

fn thousands(n: u64) -> String {
    let s = n.to_string();
    let mut out = String::new();
    for (i, c) in s.chars().enumerate() {
        if i > 0 && (s.len() - i).is_multiple_of(3) {
            out.push(',');
        }
        out.push(c);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::VecDeque;

    #[test]
    fn numbers_as_people_read_them() {
        assert_eq!(thousands(0), "0");
        assert_eq!(thousands(999), "999");
        assert_eq!(thousands(1_000), "1,000");
        assert_eq!(thousands(6_123_456), "6,123,456");
        assert_eq!(took(Duration::from_secs(31)), "31s");
        assert_eq!(took(Duration::from_secs(86)), "1m 26s");
        assert_eq!(took(Duration::from_secs(15_123)), "4h 12m");
    }

    #[test]
    fn a_range_is_dates_times_or_a_span_before_now() {
        assert_eq!(
            parse_when("2026-10-01").unwrap(),
            parse_when("2026-10-01T00:00:00Z").unwrap()
        );
        let near =
            |t: DateTime<Utc>, ago: TimeDelta| (Utc::now() - ago - t).abs() < TimeDelta::seconds(5);
        assert!(near(parse_when("1d").unwrap(), TimeDelta::days(1)));
        assert!(near(parse_when("12h").unwrap(), TimeDelta::hours(12)));
        for bad in ["last week", "1w", "-1d", "d", "1.5h"] {
            assert!(parse_when(bad).is_err(), "{bad}");
        }
    }

    /// Without --to the range ends five minutes before the replay starts:
    /// Event history reads it whole, so past() says nothing of its end.
    #[test]
    fn to_left_out_is_five_minutes_ago() {
        let (from, to) = range("1d", None).unwrap();
        let started = Utc::now();
        assert!(to <= started - cloudtrail_history::lag());
        assert_eq!(
            cloudtrail_history::range_end(from, to, started).unwrap(),
            to
        );
    }

    /// Records at fixed times, as a source's past would give them.
    struct Fixed(VecDeque<(DateTime<Utc>, Value)>);

    #[async_trait]
    impl Past for Fixed {
        async fn until(&mut self, t: DateTime<Utc>) -> Result<Vec<Value>> {
            let mut out = Vec::new();
            while self.0.front().is_some_and(|(at, _)| *at < t) {
                out.push(self.0.pop_front().unwrap().1);
            }
            Ok(out)
        }
        fn size(&self) -> String {
            format!("{} records", self.0.len())
        }
        fn estimate(&self) -> Option<u64> {
            None
        }
        fn read(&self) -> String {
            String::new()
        }
    }

    /// The file a live pipeline's lake has open in its WAL.
    const LIVE_WAL: &str = "lake/wal/01JBZ5Q0000000000000000000.bin";

    /// A config beside a live pipeline's state.sqlite and lake, its WAL
    /// with a file open: source `test` to the lake and to a pass-through
    /// plugin grouped by actor, half an hour its expiry and with history, to
    /// `read.ndjson`. Returns the config's path.
    fn pipeline(root: &Path) -> PathBuf {
        std::fs::create_dir_all(root.join("plugins")).unwrap();
        let cwasm = tangent_shared::wasm_engine::build()
            .unwrap()
            .precompile_component(
                include_str!("../testdata/enrichment/plain.component.wat").as_bytes(),
            )
            .unwrap();
        std::fs::write(root.join("plugins/judge.cwasm"), cwasm).unwrap();
        std::fs::write(root.join("state.sqlite"), "a live pipeline's").unwrap();
        std::fs::create_dir_all(root.join("lake/wal")).unwrap();
        std::fs::write(root.join(LIVE_WAL), "{\"live\":1}\n").unwrap();
        let path = root.join("tangent.yaml");
        std::fs::write(
            &path,
            format!(
                "runtime: {{ workers: 2, plugins_path: plugins/, state: {{ backend: sqlite, path: state.sqlite }} }}
plugins:
  judge: {{ module_type: rust, path: ., group_by: [actor], expire_after_ms: 1800000, history: 3 }}
sources:
  test: {{ type: tcp, bind_address: 127.0.0.1:0 }}
sinks:
  read: {{ type: file, path: {r}/read.ndjson, compression: {{ type: none }} }}
  lake: {{ type: rolling_file, path: {r}/lake/logs/%H, wal_path: {r}/lake/wal, max_file_age_seconds: 3600 }}
dag:
  - from: test
    to: [judge, lake]
  - from: judge
    to: [read]
",
                r = root.display()
            ),
        )
        .unwrap();
        path
    }

    /// Two hours of two actors through [`pipeline`] with `edit` made to its
    /// config, replay's state in `state`: what reached the sink.
    async fn two_hours(root: &Path, state: &Path, edit: impl FnOnce(&mut Config)) -> Vec<Value> {
        let path = pipeline(root);
        let t0 = parse_when("2026-10-01").unwrap();
        let at = |m: i64| t0 + TimeDelta::minutes(m);
        let mut past = VecDeque::new();
        for m in [5, 20, 50, 65, 80, 110] {
            let actor = if m % 2 == 0 { "a" } else { "b" };
            let record =
                json!({"source": "test", "ts": at(m).to_rfc3339(), "actor": actor, "n": m});
            past.push_back((at(m), record));
        }
        let sources = vec![Replayed {
            name: "test".into(),
            node: NodeRef::Source {
                name: "test".into(),
            },
            past: Box::new(Fixed(past)),
            records: 0,
        }];
        let mut cfg = Config::from_file(&path).unwrap();
        edit(&mut cfg);
        sweep(cfg, &path, state, sources, at(0), at(120))
            .await
            .unwrap();
        std::fs::read_to_string(root.join("read.ndjson"))
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    /// The groups close only at the end of input, half an hour their
    /// expiry: when replay returns, every record is through the grouped
    /// plugin to its sink once, the last hour's too.
    #[tokio::test(flavor = "multi_thread")]
    async fn every_record_is_judged_once_by_the_end() {
        let (dir, state) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let read = two_hours(dir.path(), state.path(), |_| {}).await;
        let mut judged: Vec<i64> = read
            .iter()
            .filter(|v| v.get("_context").is_none())
            .map(|v| v["n"].as_i64().unwrap())
            .collect();
        judged.sort();
        assert_eq!(
            judged,
            vec![5, 20, 50, 65, 80, 110],
            "each record judged once: {read:?}"
        );
    }

    /// The history replay builds goes to a state of its own: the live
    /// pipeline's state.sqlite beside the config is as it was.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_live_pipelines_state_is_left_as_it_is() {
        let (dir, state) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        two_hours(dir.path(), state.path(), |_| {}).await;
        assert_eq!(
            std::fs::read_to_string(dir.path().join("state.sqlite")).unwrap(),
            "a live pipeline's"
        );
        assert!(state.path().join("state.sqlite").is_file());
    }

    /// A config with no source replay can read is refused, naming the kind
    /// it can.
    #[tokio::test]
    async fn a_config_without_a_cloudtrail_source_is_told_what_replay_reads() {
        let dir = tempfile::tempdir().unwrap();
        let e = replay(ReplayOptions {
            config: pipeline(dir.path()),
            from: "1d".into(),
            to: None,
            state_dir: dir.path().join("replay"),
        })
        .await
        .unwrap_err();
        assert!(e.to_string().contains("has no cloudtrail source"), "{e}");
    }

    /// A rolling_file sink is left out: the live pipeline's open WAL file
    /// is where it was, unsealed, and the lake gains no files.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_live_pipelines_lake_is_left_as_it_is() {
        let (dir, state) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        two_hours(dir.path(), state.path(), |_| {}).await;
        assert_eq!(
            std::fs::read_to_string(dir.path().join(LIVE_WAL)).unwrap(),
            "{\"live\":1}\n"
        );
        assert!(!dir.path().join("lake/logs").exists());
    }

    /// A config whose state is another pipeline's, read-only, replays with a
    /// state of its own all the same.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_read_only_state_is_replays_own() {
        let (dir, state) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let read = two_hours(dir.path(), state.path(), |cfg| {
            cfg.runtime.state.read_only = true
        })
        .await;
        assert!(!read.is_empty());
    }
}
