//! `tangent replay`: a past range of an ingest config's sources through it
//! and a judge config together, in one process, an hour at a time: the
//! hour's records of every source whose past replay can read into ingest,
//! then the judge until it has judged what live would have by then, then the
//! next hour. The judge never reads a record from a later hour, and its
//! groups form by record time as live.

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
use tangent_shared::runtime::StateBackend;
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
use crate::sources::sqlite::Watch;
use crate::worker::Ack;
use crate::{state, wait_for_shutdown_signal};

pub struct ReplayOptions {
    pub ingest: PathBuf,
    pub judge: PathBuf,
    /// The range, `[from, to)`: a date (midnight UTC) or an RFC 3339 time.
    pub from: String,
    pub to: String,
}

/// A source's past, as replay reads it: the range's records in time order,
/// an hour at a time.
#[async_trait]
pub trait Past: Send {
    /// The range's records from before `t` not returned yet, each as the
    /// live source hands it on (its `source` and `ts` stamped).
    async fn until(&mut self, t: DateTime<Utc>) -> Result<Vec<Value>>;
    /// What the range holds, said before the judge starts.
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
    let mut ingest = Config::from_file(&opts.ingest)?;
    ingest.validate()?;
    let mut judge = Config::from_file(&opts.judge)?;
    judge.validate()?;
    let (from, to) = (parse_when(&opts.from)?, parse_when(&opts.to)?);
    ensure!(from < to, "from ({from}) is not before to ({to})");

    // The history it builds starts empty: folded into a live pipeline's, it
    // would be neither.
    if matches!(ingest.runtime.state.backend, StateBackend::Sqlite) {
        let dir = opts.ingest.parent().unwrap_or(Path::new("."));
        let path = state::sqlite_path(&ingest.runtime.state, dir);
        ensure!(
            !path.exists(),
            "{} exists: replay builds its own history, so run it in a directory of its own",
            path.display()
        );
    }
    keep_local(&mut ingest);
    keep_local(&mut judge);
    let watches = judge
        .sources
        .values()
        .filter_map(|s| match s {
            SourceConfig::Sqlite(c) => Some(Watch::open(c)),
            _ => None,
        })
        .collect::<Result<Vec<_>>>()?;
    ensure!(
        !watches.is_empty(),
        "the judge config has no sqlite source: nothing reads what ingest stores"
    );
    let outputs = outputs(&[&ingest, &judge]);

    let started = Instant::now();
    // Every source comes out of the config: those with a past are read here,
    // and the rest would mix today's records in.
    let mut sources = Vec::new();
    for (name, src) in std::mem::take(&mut ingest.sources) {
        match past(&name, &src, from, to).await? {
            Some(past) => sources.push(Replayed {
                node: NodeRef::Source {
                    name: Arc::clone(&name),
                },
                name,
                past,
                records: 0,
            }),
            None => {
                tracing::warn!(source = %name, "replay cannot read this source's past; it is left out")
            }
        }
    }
    ensure!(
        !sources.is_empty(),
        "no source in {} has a past replay can read",
        opts.ingest.display()
    );
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
            .map(|n| format!("; about {} records for the judge", thousands(n)))
            .unwrap_or_default();
        println!("  {}: {}{records}", s.name, s.past.size());
    }

    let chunks = ingest.batch_size_kb();
    let stop_ingest = CancellationToken::new();
    let ingest_dag = DagRuntime::build(
        ingest,
        &opts.ingest,
        stop_ingest.clone(),
        CancellationToken::new(),
    )
    .await?;
    let stop_judge = CancellationToken::new();
    let stop_models = CancellationToken::new();
    let judge_dag =
        DagRuntime::build(judge, &opts.judge, stop_judge.clone(), stop_models.clone()).await?;

    let stepped = tokio::select! {
        r = step(&mut sources, first, &mut hour, to, &ingest_dag.router, chunks, &watches) => r,
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
            Err(anyhow!("stopped before {}: the rest of the range was not replayed", when(hour)))
        }
    };

    stop_judge.cancel();
    judge_dag
        .shutdown(Duration::from_secs(120), Duration::from_secs(120))
        .await?;
    stop_ingest.cancel();
    ingest_dag
        .shutdown(Duration::from_secs(120), Duration::from_secs(120))
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
    stepped
}

/// Each hour into ingest, then the judge until it is done with what it can
/// judge; after the last, an hour of nothing, so the range's last records go.
async fn step(
    sources: &mut [Replayed],
    first: Vec<Vec<Value>>,
    hour: &mut DateTime<Utc>,
    to: DateTime<Utc>,
    router: &Router,
    chunks: usize,
    watches: &[Watch],
) -> Result<()> {
    let mut got = first;
    loop {
        for (s, records) in sources.iter_mut().zip(got) {
            s.records += records.len() as u64;
            store(router, &s.node, frames(records, chunks)?).await?;
        }
        judged(watches).await?;
        tracing::info!(hour = %when(*hour), "replay: judged up to");
        if *hour >= to {
            break;
        }
        *hour = (*hour + TimeDelta::hours(1)).min(to);
        got = Vec::new();
        for s in sources.iter_mut() {
            got.push(s.past.until(*hour).await?);
        }
    }
    let mut marks = Vec::new();
    for w in watches {
        marks.push(w.quiet()?);
    }
    judged(watches).await?;
    for (w, mark) in watches.iter().zip(marks) {
        if let Some(id) = mark {
            w.remove(id)?;
        }
    }
    Ok(())
}

/// Records as NDJSON frames of about `chunks` bytes, as a source hands them on.
fn frames(records: Vec<Value>, chunks: usize) -> Result<Vec<BytesMut>> {
    let mut ndjson = BytesMut::new();
    for v in &records {
        ndjson.extend_from_slice(&frame(v)?);
    }
    Ok(chunk_ndjson(&mut ndjson, chunks))
}

/// Hands `frames` to ingest as the source `node` would, and waits until its
/// sinks have written them all.
async fn store(router: &Router, node: &NodeRef, frames: Vec<BytesMut>) -> Result<()> {
    if frames.is_empty() {
        return Ok(());
    }
    let (tx, rx) = oneshot::channel();
    router
        .forward(node, frames, vec![Arc::new(Stored(Mutex::new(Some(tx))))])
        .await?;
    if rx.await.is_err() {
        tracing::warn!("ingest let go of records without storing them; the judge may miss some");
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

/// Until no sqlite source of the judge has a group due or in flight.
async fn judged(watches: &[Watch]) -> Result<()> {
    loop {
        let mut due = 0;
        for w in watches {
            due += w.waiting()?;
        }
        if due == 0 {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

/// Sinks that post or upload become blackholes: a replay of last month
/// sends no alerts and archives nothing a second time.
fn keep_local(cfg: &mut Config) {
    for (name, sink) in &mut cfg.sinks {
        if matches!(sink.kind, SinkKind::Slack(_) | SinkKind::S3(_)) {
            tracing::warn!(sink = %name, "replay writes only to this machine; this sink is left out");
            sink.kind = SinkKind::Blackhole(BlackholeConfig {});
        }
    }
}

/// Each NDJSON file sink and its size now: what the replay adds comes after.
fn outputs(cfgs: &[&Config]) -> Vec<(PathBuf, u64)> {
    let mut out = Vec::new();
    for cfg in cfgs {
        for sink in cfg.sinks.values() {
            if let (SinkKind::File(f), Encoding::NDJSON) = (&sink.kind, &sink.common.encoding) {
                let size = std::fs::metadata(&f.path).map_or(0, |m| m.len());
                out.push((f.path.clone(), size));
            }
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

/// `2026-10-01` (midnight UTC) or an RFC 3339 time.
pub fn parse_when(s: &str) -> Result<DateTime<Utc>> {
    let s = s.trim();
    s.parse::<DateTime<Utc>>()
        .or_else(|_| {
            chrono::NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map(|d| d.and_hms_opt(0, 0, 0).unwrap().and_utc())
        })
        .map_err(|_| anyhow!("{s:?}: want a date (2026-10-01) or an RFC 3339 time"))
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
    fn a_range_is_dates_or_times() {
        assert_eq!(
            parse_when("2026-10-01").unwrap(),
            parse_when("2026-10-01T00:00:00Z").unwrap()
        );
        assert!(parse_when("last week").is_err());
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

    /// Two hours of two actors through ingest (a sqlite sink) and a judge
    /// (its sqlite source to a file), half an hour its timeout. Each actor's
    /// first hour is due once that hour is stored, so it is judged then and
    /// is context beside the actor's second hour; stored before the judge had
    /// done, the two hours would have gone together. Every record is judged
    /// once.
    #[tokio::test(flavor = "multi_thread")]
    async fn each_hour_is_judged_before_the_next_is_stored() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let t0 = parse_when("2026-10-01").unwrap();
        let at = |m: i64| t0 + TimeDelta::minutes(m);
        let record = |m: i64, actor: &str| json!({"source": "test", "ts": at(m).to_rfc3339(), "actor": actor, "n": m});
        let mut past = VecDeque::new();
        for m in [5, 20, 50, 65, 80, 110] {
            past.push_back((at(m), record(m, if m % 2 == 0 { "a" } else { "b" })));
        }
        let ingest = root.join("ingest.yaml");
        std::fs::write(
            &ingest,
            format!(
                "runtime: {{ workers: 1 }}
sources:
  test: {{ type: tcp, bind_address: 127.0.0.1:0 }}
sinks:
  logs: {{ type: sqlite, path: {r}/logs.sqlite, table: logs, timestamp_key: ts, partitions: {{ actor: [actor] }} }}
dag:
  - from: {{ kind: source, name: test }}
    to: [{{ kind: sink, name: logs }}]
",
                r = root.display()
            ),
        )
        .unwrap();
        let judge = root.join("judge.yaml");
        std::fs::write(
            &judge,
            format!(
                "runtime: {{ workers: 1, state: {{ backend: sqlite, path: state.sqlite, read_only: true }} }}
sources:
  actors: {{ type: sqlite, path: {r}/logs.sqlite, table: logs, partition: actor, batch_size: 40, timeout_secs: 1800 }}
sinks:
  read: {{ type: file, path: {r}/read.ndjson, compression: {{ type: none }} }}
dag:
  - from: {{ kind: source, name: actors }}
    to: [{{ kind: sink, name: read }}]
",
                r = root.display()
            ),
        )
        .unwrap();

        let mut icfg = Config::from_file(&ingest).unwrap();
        icfg.sources.clear();
        let jcfg = Config::from_file(&judge).unwrap();
        let watches: Vec<Watch> = jcfg
            .sources
            .values()
            .map(|s| match s {
                SourceConfig::Sqlite(c) => Watch::open(c).unwrap(),
                _ => unreachable!(),
            })
            .collect();
        let idag = DagRuntime::build(
            icfg,
            &ingest,
            CancellationToken::new(),
            CancellationToken::new(),
        )
        .await
        .unwrap();
        let stop = CancellationToken::new();
        let jdag = DagRuntime::build(jcfg, &judge, stop.clone(), CancellationToken::new())
            .await
            .unwrap();

        let mut sources = vec![Replayed {
            name: "test".into(),
            node: NodeRef::Source {
                name: "test".into(),
            },
            past: Box::new(Fixed(past)),
            records: 0,
        }];
        let mut hour = at(60);
        let first = vec![sources[0].past.until(hour).await.unwrap()];
        step(
            &mut sources,
            first,
            &mut hour,
            at(120),
            &idag.router,
            64,
            &watches,
        )
        .await
        .unwrap();
        stop.cancel();
        jdag.shutdown(Duration::from_secs(30), Duration::from_secs(30))
            .await
            .unwrap();

        assert_eq!(sources[0].records, 6);
        let read: Vec<Value> = std::fs::read_to_string(root.join("read.ndjson"))
            .unwrap()
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        let fresh: Vec<i64> = read
            .iter()
            .filter(|v| v.get("_context").is_none())
            .map(|v| v["n"].as_i64().unwrap())
            .collect();
        let mut sorted = fresh.clone();
        sorted.sort();
        assert_eq!(
            sorted,
            vec![5, 20, 50, 65, 80, 110],
            "each record judged once: {read:?}"
        );
        let mut first: Vec<i64> = fresh[..3].to_vec();
        first.sort();
        assert_eq!(
            first,
            vec![5, 20, 50],
            "the first hour judged first: {read:?}"
        );
        let mut context: Vec<i64> = read
            .iter()
            .filter(|v| v.get("_context").is_some())
            .map(|v| v["n"].as_i64().unwrap())
            .collect();
        context.sort();
        assert_eq!(
            context,
            vec![5, 20, 50],
            "and apart from the second: {read:?}"
        );
    }
}
