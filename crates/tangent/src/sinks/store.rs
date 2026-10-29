//! The `store` sink: every record kept in the state store under its
//! partition, indexed by the scopes an inference node reads history by,
//! marked with what is new for its scopes, and the bookkeeping the `store`
//! source closes windows by.

use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde_json::{Map, Value};
use tangent_shared::runtime::PartitionConfig;
use tangent_shared::sinks::store::StoreSinkConfig;

use crate::inference::lookup;
use crate::partition::key_of_value;
use crate::sinks::manager::{Sink, SinkWrite};
use crate::sources::poll::duration_ms;
use crate::state::{now_ms, StateStore, StateValue};
use crate::windows::{
    ev_key, follow_ns, get_int, index_ns, index_prefix, indexed_marker, remembered_marker,
    scope_value, seen_ns, set_int, time_of_value, ts_of_key, CLOSED_THROUGH, FIRST, LAST_ARRIVAL,
    LATE_PREFIX, READ_THROUGH, WATERMARK, WINDOWS_NS,
};
use crate::{SINK_BYTES_TOTAL, SINK_OBJECTS_TOTAL};

/// Where a record with no partition key goes.
const NO_PARTITION: &str = "_";

/// The field a record new for its scopes is marked with.
pub const NOVEL: &str = "_novel";

/// How far a sighting must move a seen entry's latest time before the entry
/// is written again: a busy value costs a write a minute, not one a record.
const GRAIN_MS: i64 = 60_000;

/// How many of a scope value's records one follow window marks.
const FOLLOW_CAP: i64 = 50;

/// What a record following a new one of its scope value is marked with.
const FOLLOWS: &str = "follows";

/// What a record with no value for any novel field is marked with, under
/// no scope: novelty cannot tell, so it fails open and the record is judged.
const UNSCOPED: (&str, &str) = ("_", "unscoped");

/// A scope novelty is decided for.
struct NovelScope {
    name: String,
    /// The field its values are read from.
    field: String,
    /// The fields whose values each of its values is remembered to have had.
    fields: Vec<String>,
    /// Whether a value's records after its new one are followed: not when
    /// the scope is the partition, whose every record would be.
    follow: bool,
}

/// How many runs of sightings a seen entry keeps. Records do not arrive in
/// time order: a source can run months behind another, and its records
/// fall in a run of their own beside the later one.
const RUNS: usize = 4;

/// A seen entry: the runs of record time over which a scope value had one
/// value of a field, oldest first, each its earliest and latest sighting
/// with no gap in it more than `retain`. A sighting is new when no run
/// holds it or ends within `retain` before it, so a run that arrives after
/// a later one is new at its first record, not at each of them.
#[derive(Clone, Debug, Default, PartialEq)]
struct Seen {
    runs: Vec<(i64, i64)>,
}

impl Seen {
    /// An entry as stored; one from before record time was kept (a clock
    /// time) reads as none, and the rows it came from are read again first.
    fn read(v: Option<StateValue>) -> Option<Self> {
        let Some(StateValue::Json(Value::Array(runs))) = v else { return None };
        let runs = runs
            .iter()
            .map(|r| Some((r.get(0)?.as_i64()?, r.get(1)?.as_i64()?)))
            .collect::<Option<_>>()?;
        Some(Self { runs })
    }

    fn value(&self) -> StateValue {
        StateValue::Json(serde_json::json!(self.runs.iter().map(|(f, l)| [f, l]).collect::<Vec<_>>()))
    }

    /// A sighting at `ts`: whether it is new, and whether the entry moved
    /// enough to be written again.
    fn sight(&mut self, ts: i64, retain: i64) -> (bool, bool) {
        if let Some(run) = self.runs.iter_mut().find(|(first, last)| *first <= ts && ts - *last <= retain) {
            if ts - run.1 <= GRAIN_MS {
                return (false, false);
            }
            run.1 = ts;
            self.join(retain, ts);
            return (false, true);
        }
        self.runs.push((ts, ts));
        self.join(retain, ts);
        (true, true)
    }

    /// Runs in time order, those less than `retain` apart joined, and the
    /// oldest dropped past `RUNS`, never the one holding `ts`: what is
    /// dropped is only marked again.
    fn join(&mut self, retain: i64, ts: i64) {
        self.runs.sort();
        let mut runs: Vec<(i64, i64)> = Vec::with_capacity(self.runs.len());
        for (first, last) in self.runs.drain(..) {
            match runs.last_mut() {
                Some(run) if first - run.1 <= retain => run.1 = run.1.max(last),
                _ => runs.push((first, last)),
            }
        }
        while runs.len() > RUNS {
            let (first, last) = runs[0];
            runs.remove(if first <= ts && ts <= last { 1 } else { 0 });
        }
        self.runs = runs;
    }
}

/// A scope value's follow window: the record time of its latest new record,
/// and how many of its records since have been marked as following it.
#[derive(Clone, Copy, Debug, PartialEq)]
struct Window {
    start: i64,
    count: i64,
}

impl Window {
    fn read(v: Option<StateValue>) -> Option<Self> {
        let Some(StateValue::Json(j)) = v else { return None };
        Some(Self { start: j.get("start")?.as_i64()?, count: j.get("count")?.as_i64()? })
    }

    fn value(&self) -> StateValue {
        StateValue::Json(serde_json::json!({"start": self.start, "count": self.count}))
    }
}

/// What one write learns of a partition's novelty: seen entries and follow
/// windows as the store had them, changed by the write's records in time
/// order, each with whether it changed. Nothing is written until the
/// write's records are stored, so a write that fails and is tried again
/// marks the same records.
#[derive(Default)]
struct Learned {
    seen: HashMap<String, (Option<Seen>, bool)>,
    windows: HashMap<String, (Option<Window>, bool)>,
}

/// One entry of what a write has learned, read from the store the first
/// time it is needed.
async fn cached<'a, T>(
    learned: &'a mut HashMap<String, (Option<T>, bool)>,
    store: &dyn StateStore,
    ns: &str,
    part: &str,
    key: String,
    read: fn(Option<StateValue>) -> Option<T>,
) -> Result<&'a mut (Option<T>, bool)> {
    if !learned.contains_key(&key) {
        let had = read(store.get(ns, part, &key).await?);
        learned.insert(key.clone(), (had, false));
    }
    Ok(learned.get_mut(&key).expect("read above"))
}

pub struct StoreSink {
    store: Arc<dyn StateStore>,
    partition: Option<PartitionConfig>,
    ns: String,
    retain_ms: u64,
    time: String,
    /// (scope, field) each record is indexed by; a scope whose field is a
    /// partition key is not: the partition is its history.
    indexed: Vec<(String, String)>,
    novel: Vec<NovelScope>,
    /// How long in record time a scope value's records after a new one are
    /// marked too.
    follow_ms: Option<i64>,
    /// Writes run side by side; a partition's window bookkeeping (read, then
    /// written) and its first reading of older rows happen under this, one
    /// at a time.
    bookkeeping: tokio::sync::Mutex<HashSet<String>>,
    /// Per partition, held by a write with novelty from deciding its marks
    /// to writing what it learned: a write reads what the one before it
    /// learned, so writes side by side neither mark one value twice nor
    /// miss a follow window. With SQLite, one connection runs them one at a
    /// time anyway.
    turns: std::sync::Mutex<HashMap<String, Arc<tokio::sync::Mutex<()>>>>,
}

impl StoreSink {
    pub fn new(
        cfg: &StoreSinkConfig,
        store: Arc<dyn StateStore>,
        partition: Option<PartitionConfig>,
    ) -> Result<Arc<Self>> {
        let retain_ms = duration_ms(&cfg.retain)
            .filter(|ms| *ms > 0)
            .ok_or_else(|| anyhow!("store sink: retain {:?} is not a duration like 30d", cfg.retain))?
            as u64;
        let follow_ms = cfg
            .follow
            .as_deref()
            .map(|f| duration_ms(f).filter(|ms| *ms > 0).ok_or_else(|| anyhow!("store sink: follow {f:?} is not a duration like 1h")))
            .transpose()?;
        let partition_fields: Vec<&String> = partition.iter().flat_map(|p| p.key.iter()).collect();
        let indexed = cfg
            .recent
            .iter()
            .filter(|(_, field)| !partition_fields.contains(field))
            .map(|(s, f)| (s.clone(), f.clone()))
            .collect();
        let novel = cfg
            .novel
            .iter()
            .map(|(scope, fields)| match cfg.recent.get(scope) {
                Some(field) => Ok(NovelScope {
                    name: scope.clone(),
                    field: field.clone(),
                    fields: fields.clone(),
                    follow: !partition_fields.contains(&field),
                }),
                None => Err(anyhow!("store sink: novel scope {scope:?} is not one of `recent`'s, which give a scope its field")),
            })
            .collect::<Result<_>>()?;
        Ok(Arc::new(Self {
            store,
            partition,
            ns: cfg.namespace.clone(),
            retain_ms,
            time: cfg.time.clone(),
            indexed,
            novel,
            follow_ms,
            bookkeeping: tokio::sync::Mutex::new(HashSet::new()),
            turns: Default::default(),
        }))
    }

    /// The index entries of one stored record: one per scope of `scopes` it
    /// has a value for, each holding the record's key.
    async fn index(&self, part: &str, key: &str, v: &Value, scopes: &[(String, String)]) -> Result<()> {
        let ns = index_ns(&self.ns);
        for (scope, field) in scopes {
            if let Some(value) = lookup(v, field).and_then(scope_value) {
                let at = format!("{}{key}", index_prefix(scope, &value));
                self.store
                    .set(&ns, part, &at, &StateValue::Str(key.to_string()), Some(self.retain_ms))
                    .await?;
            }
        }
        Ok(())
    }

    /// What of one record, at `ts`, its scopes had not had within `retain`
    /// before it, by scope, read through what `learned` holds; what the
    /// record shows is added there. With `follow`, a scope value's records
    /// within it after its new one are marked `follows`, those that reach
    /// the sink with or after it: one that came before is stored already. A
    /// record with no value for any field of any scope is `unscoped`.
    async fn check(
        &self,
        part: &str,
        ts: i64,
        v: &Value,
        scopes: &[NovelScope],
        follow: Option<i64>,
        learned: &mut Learned,
    ) -> Result<Map<String, Value>> {
        let mut new = Map::new();
        let mut any = false;
        for s in scopes {
            let Some(sv) = lookup(v, &s.field).and_then(scope_value) else { continue };
            let mut marks = Vec::new();
            for f in &s.fields {
                let Some(value) = lookup(v, f).and_then(scope_value) else { continue };
                any = true;
                let at = format!("{}{f}\u{1f}{value}", index_prefix(&s.name, &sv));
                if self.sighted(part, at, ts, learned).await? {
                    marks.push(Value::String(f.clone()));
                }
            }
            if let Some(follow) = follow.filter(|_| s.follow) {
                let at = index_prefix(&s.name, &sv);
                let (window, changed) =
                    cached(&mut learned.windows, &*self.store, &follow_ns(&self.ns), part, at, Window::read).await?;
                if !marks.is_empty() {
                    // Something new starts the window again.
                    if window.is_none_or(|w| ts > w.start) {
                        *window = Some(Window { start: ts, count: 0 });
                        *changed = true;
                    }
                } else if let Some(w) = window.filter(|w| ts >= w.start && ts - w.start <= follow && w.count < FOLLOW_CAP) {
                    *window = Some(Window { count: w.count + 1, ..w });
                    *changed = true;
                    marks.push(Value::String(FOLLOWS.to_string()));
                }
            }
            if !marks.is_empty() {
                new.insert(s.name.clone(), Value::Array(marks));
            }
        }
        if !any && !scopes.is_empty() {
            new.insert(UNSCOPED.0.to_string(), Value::Array(vec![Value::String(UNSCOPED.1.to_string())]));
        }
        Ok(new)
    }

    /// Whether a sighting at `ts` of one seen key is new: no run of its
    /// entry holds `ts` or ends within `retain` before it. A first sighting
    /// arriving after a later one is new, and the later one stays marked
    /// too. Record time decides, not arrival or the clock, so a replayed
    /// year forgets as a real one would.
    async fn sighted(&self, part: &str, at: String, ts: i64, learned: &mut Learned) -> Result<bool> {
        let (seen, changed) = cached(&mut learned.seen, &*self.store, &seen_ns(&self.ns), part, at, Seen::read).await?;
        let (new, moved) = seen.get_or_insert_with(Seen::default).sight(ts, self.retain_ms as i64);
        *changed |= moved;
        Ok(new)
    }

    /// What a write learned, written once its records are stored. An entry
    /// is kept on the clock for half a retention past `retain`, so it
    /// outlives the record time it is new again at; the clock only clears
    /// what is stale.
    async fn commit(&self, part: &str, learned: Learned) -> Result<()> {
        let ttl = Some(self.retain_ms + self.retain_ms / 2);
        let ns = seen_ns(&self.ns);
        for (at, (seen, changed)) in learned.seen {
            if let (Some(s), true) = (seen, changed) {
                self.store.set(&ns, part, &at, &s.value(), ttl).await?;
            }
        }
        let ns = follow_ns(&self.ns);
        for (at, (window, changed)) in learned.windows {
            if let (Some(w), true) = (window, changed) {
                self.store.set(&ns, part, &at, &w.value(), ttl).await?;
            }
        }
        Ok(())
    }

    /// A partition's rows stored before this sink indexed a scope or
    /// remembered a field, read once for those alone, each row at its own
    /// time: a store written by an older version, or by a sink that listed
    /// fewer scopes or fields, has history and knows what is usual from its
    /// first write on. Nothing is marked or followed from them.
    async fn prepare(&self, part: &str) -> Result<()> {
        let mut index = Vec::new();
        for (scope, field) in &self.indexed {
            if get_int(&self.store, part, &indexed_marker(scope, field)).await?.is_none() {
                index.push((scope.clone(), field.clone()));
            }
        }
        let mut remember = Vec::new();
        for s in &self.novel {
            let mut fields = Vec::new();
            for f in &s.fields {
                if get_int(&self.store, part, &remembered_marker(&s.name, &s.field, f)).await?.is_none() {
                    fields.push(f.clone());
                }
            }
            if !fields.is_empty() {
                remember.push(NovelScope { name: s.name.clone(), field: s.field.clone(), fields, follow: false });
            }
        }
        if index.is_empty() && remember.is_empty() {
            return Ok(());
        }
        let (mut from, mut n) = ("ev:".to_string(), 0usize);
        loop {
            let page = self.store.range(&self.ns, part, &from, "ev:~", 2_000).await?;
            let Some(last) = page.last() else { break };
            from = format!("{}!", last.key);
            let mut learned = Learned::default();
            for e in &page {
                if let (StateValue::Str(text), Some(ts)) = (&e.value, ts_of_key(&e.key)) {
                    if let Ok(v) = serde_json::from_str::<Value>(text) {
                        self.index(part, &e.key, &v, &index).await?;
                        if !remember.is_empty() {
                            self.check(part, ts, &v, &remember, None, &mut learned).await?;
                        }
                        n += 1;
                    }
                }
            }
            self.commit(part, learned).await?;
        }
        if n > 0 {
            let fields: usize = remember.iter().map(|s| s.fields.len()).sum();
            tracing::info!(partition = %part, records = n, scopes = index.len(), fields, "store sink: read the records stored before");
        }
        for (scope, field) in &index {
            set_int(&self.store, part, &indexed_marker(scope, field), now_ms() as i64).await?;
        }
        for s in &remember {
            for f in &s.fields {
                set_int(&self.store, part, &remembered_marker(&s.name, &s.field, f), now_ms() as i64).await?;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Sink for StoreSink {
    async fn write(&self, req: SinkWrite) -> Result<()> {
        let now = now_ms() as i64;
        let mut records = Vec::new();
        for line in req.payload[..].split(|b: &u8| *b == b'\n') {
            if line.iter().all(u8::is_ascii_whitespace) {
                continue;
            }
            let v: Value = match serde_json::from_slice(line) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("store sink: a line that is not JSON is skipped: {e}");
                    continue;
                }
            };
            let part = self
                .partition
                .as_ref()
                .and_then(|p| key_of_value(&v, p))
                .map(|p| p.to_string())
                .unwrap_or_else(|| NO_PARTITION.to_string());
            let ts = time_of_value(&v, &self.time).unwrap_or(now);
            records.push((part, ts, v, line));
        }
        // A partition's older rows are read before its first new one is
        // stored, so what they had is not new.
        {
            let mut ready = self.bookkeeping.lock().await;
            for (part, ..) in &records {
                if !ready.contains(part) {
                    self.prepare(part).await?;
                    ready.insert(part.clone());
                }
            }
        }

        // What is new, in record time order: of a write's sightings of a
        // value, the earliest is the one marked. Only read here; what the
        // records show is written after they are, before the partition's
        // next write reads it. Turns are taken in partition order, so two
        // writes never each hold one the other waits for.
        let mut learned: BTreeMap<String, Learned> = BTreeMap::new();
        let mut marks = vec![Map::new(); records.len()];
        let mut turns = Vec::new();
        if !self.novel.is_empty() {
            for part in records.iter().map(|(part, ..)| part).collect::<BTreeSet<_>>() {
                let turn = Arc::clone(self.turns.lock().expect("turns").entry(part.clone()).or_default());
                turns.push(turn.lock_owned().await);
            }
            let mut order: Vec<usize> = (0..records.len()).collect();
            order.sort_by_key(|&i| records[i].1);
            for i in order {
                let (part, ts, v, _) = &records[i];
                let l = learned.entry(part.clone()).or_default();
                marks[i] = self.check(part, *ts, v, &self.novel, self.follow_ms, l).await?;
            }
        }

        // Per partition: (record time, key) of what this write stored.
        let mut stored: BTreeMap<String, Vec<(i64, String)>> = BTreeMap::new();
        for ((part, ts, mut v, line), new) in records.into_iter().zip(marks) {
            let key = ev_key(ts, &ulid::Ulid::new().to_string());
            let text = match v.as_object_mut() {
                Some(obj) if !new.is_empty() => {
                    obj.insert(NOVEL.to_string(), Value::Object(new));
                    v.to_string()
                }
                _ => String::from_utf8_lossy(line).into_owned(),
            };
            self.store
                .set(&self.ns, &part, &key, &StateValue::Str(text), Some(self.retain_ms))
                .await?;
            self.index(&part, &key, &v, &self.indexed).await?;
            stored.entry(part).or_default().push((ts, key));
        }

        let held = self.bookkeeping.lock().await;
        for (part, rows) in stored {
            let max = rows.iter().map(|(t, _)| *t).max().unwrap_or(now);
            let min = rows.iter().map(|(t, _)| *t).min().unwrap_or(now);
            if get_int(&self.store, &part, WATERMARK).await?.is_none_or(|w| max > w) {
                set_int(&self.store, &part, WATERMARK, max).await?;
            }
            if get_int(&self.store, &part, FIRST).await?.is_none_or(|f| min < f) {
                set_int(&self.store, &part, FIRST, min).await?;
            }
            set_int(&self.store, &part, LAST_ARRIVAL, now).await?;
            // Read after the rows are written: a window the source read
            // before this write did not see them, so they are marked late.
            let read = get_int(&self.store, &part, READ_THROUGH).await?.unwrap_or(i64::MIN);
            let closed = get_int(&self.store, &part, CLOSED_THROUGH).await?.unwrap_or(i64::MIN);
            let seen_through = read.max(closed);
            for (ts, key) in rows.iter().filter(|(ts, _)| *ts < seen_through) {
                tracing::debug!(partition = %part, ts, "a record arrived after its window was read");
                self.store
                    .set(
                        WINDOWS_NS,
                        &part,
                        &format!("{LATE_PREFIX}{key}"),
                        &StateValue::Str(key.clone()),
                        Some(self.retain_ms),
                    )
                    .await?;
            }
        }
        drop(held);
        // Last: a write that failed before here is tried again from what
        // the store had, and marks the same records.
        for (part, l) in learned {
            self.commit(&part, l).await?;
        }
        drop(turns);
        SINK_OBJECTS_TOTAL.inc();
        SINK_BYTES_TOTAL.inc_by(req.payload.len() as u64);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::lru::LruStore;
    use crate::state::{Count, Entry};
    use bytes::BytesMut;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tangent_shared::runtime::StateConfig;

    fn write(lines: &[serde_json::Value]) -> SinkWrite {
        let mut payload = BytesMut::new();
        for l in lines {
            payload.extend_from_slice(l.to_string().as_bytes());
            payload.extend_from_slice(b"\n");
        }
        SinkWrite { sink_name: Arc::from("store"), payload, s3: None }
    }

    #[tokio::test]
    async fn records_are_kept_by_partition_and_time_and_late_ones_are_marked() {
        let store: Arc<dyn StateStore> = Arc::new(LruStore::new(StateConfig::default()).unwrap());
        let partition = PartitionConfig {
            key: vec!["_norm.org".into()],
            separator: "|".into(),
            retain_ms: None,
            time: None,
        };
        let cfg = StoreSinkConfig { namespace: "events".into(), retain: "30d".into(), time: "ts".into(), recent: BTreeMap::new(), novel: BTreeMap::new(), follow: None };
        let sink = StoreSink::new(&cfg, Arc::clone(&store), Some(partition)).unwrap();
        sink.write(write(&[
            serde_json::json!({"ts": "2026-10-27T13:05:00Z", "_norm": {"org": "acme"}, "n": 2}),
            serde_json::json!({"ts": "2026-10-27T13:01:00Z", "_norm": {"org": "acme"}, "n": 1}),
            serde_json::json!({"ts": "2026-10-27T13:02:00Z", "_norm": {"org": "globex"}}),
        ]))
        .await
        .unwrap();
        let rows = store.range("events", "acme", "ev:", "ev:~", 100).await.unwrap();
        assert_eq!(rows.len(), 2);
        assert!(ts_of_key(&rows[0].key) < ts_of_key(&rows[1].key), "time order, whatever the arrival order");
        assert_eq!(get_int(&store, "acme", WATERMARK).await.unwrap(), Some(1_793_106_300_000));
        assert_eq!(get_int(&store, "acme", FIRST).await.unwrap(), Some(1_793_106_060_000));
        assert!(get_int(&store, "globex", WATERMARK).await.unwrap().is_some(), "each partition its own");

        // The source has read acme's windows up to 13:15; a record for 13:10
        // arriving now is marked late, one for 13:20 is not.
        set_int(&store, "acme", READ_THROUGH, 1_793_106_900_000).await.unwrap();
        sink.write(write(&[
            serde_json::json!({"ts": "2026-10-27T13:10:00Z", "_norm": {"org": "acme"}}),
            serde_json::json!({"ts": "2026-10-27T13:20:00Z", "_norm": {"org": "acme"}}),
        ]))
        .await
        .unwrap();
        let late = store.scan(Some(WINDOWS_NS), Some("acme"), LATE_PREFIX, 10).await.unwrap();
        assert_eq!(late.len(), 1, "{late:?}");
        let StateValue::Str(ev) = &late[0].value else { panic!() };
        assert_eq!(ts_of_key(ev), Some(1_793_106_600_000));
    }

    /// The latest record keys of one scope value before `bound`, newest first,
    /// as an inference node reads them.
    async fn latest(store: &Arc<dyn StateStore>, scope: &str, value: &str, bound: &str, n: usize) -> Vec<String> {
        let prefix = crate::windows::index_prefix(scope, value);
        store
            .range_rev(&index_ns("events"), "acme", &format!("{prefix}ev:"), &format!("{prefix}{bound}"), n)
            .await
            .unwrap()
            .into_iter()
            .map(|e| match e.value { StateValue::Str(k) => k, _ => panic!() })
            .collect()
    }

    fn sink_with_scopes(store: &Arc<dyn StateStore>) -> Arc<StoreSink> {
        let partition = PartitionConfig { key: vec!["_norm.org".into()], separator: "|".into(), retain_ms: None, time: None };
        let mut recent = BTreeMap::new();
        recent.insert("actor".to_string(), "_norm.actor".to_string());
        recent.insert("org".to_string(), "_norm.org".to_string());
        let cfg = StoreSinkConfig { namespace: "events".into(), retain: "30d".into(), time: "ts".into(), recent, novel: BTreeMap::new(), follow: None };
        StoreSink::new(&cfg, Arc::clone(store), Some(partition)).unwrap()
    }

    fn rec(min: u32, actor: &str) -> serde_json::Value {
        serde_json::json!({"ts": format!("2026-10-27T13:{min:02}:00Z"), "_norm": {"org": "acme", "actor": actor}})
    }

    /// However many records come after, a scope's latest ones before any
    /// time are there: nothing is evicted but by retention.
    #[tokio::test]
    async fn every_record_is_indexed_by_its_actor_and_read_back_newest_first() {
        let store: Arc<dyn StateStore> = Arc::new(LruStore::new(StateConfig::default()).unwrap());
        let sink = sink_with_scopes(&store);
        sink.write(write(&[rec(5, "alice"), rec(1, "alice"), rec(4, "alice"), rec(2, "bob"), rec(3, "alice")])).await.unwrap();
        // Six hundred of alice's after 13:05: her records before 13:04 stay readable.
        let busy: Vec<serde_json::Value> = (0..600).map(|i| serde_json::json!({"ts": format!("2026-10-27T14:{:02}:{:02}Z", i / 60, i % 60), "_norm": {"org": "acme", "actor": "alice"}})).collect();
        sink.write(write(&busy)).await.unwrap();
        let before = crate::windows::ev_bound(1_793_106_240_000); // 13:04
        let mins: Vec<i64> = latest(&store, "actor", "alice", &before, 2).await.iter().map(|k| (ts_of_key(k).unwrap() - 1_793_106_000_000) / 60_000).collect();
        assert_eq!(mins, [3, 1], "alice's latest two before 13:04, newest first");
        assert_eq!(latest(&store, "actor", "bob", "ev:~", 10).await.len(), 1);
        assert!(latest(&store, "org", "acme", "ev:~", 10).await.is_empty(), "the org is the partition: not indexed");
        for k in latest(&store, "actor", "alice", "ev:~", 1000).await {
            assert!(store.get("events", "acme", &k).await.unwrap().is_some(), "an indexed key is a stored record");
        }
    }

    fn novelty_sink(store: &Arc<dyn StateStore>, novel: &[(&str, &[&str])], follow: Option<&str>) -> Arc<StoreSink> {
        let partition = PartitionConfig { key: vec!["_norm.org".into()], separator: "|".into(), retain_ms: None, time: None };
        let recent = BTreeMap::from([("actor".to_string(), "_norm.actor".to_string()), ("org".to_string(), "_norm.org".to_string())]);
        let novel = novel.iter().map(|(s, fs)| (s.to_string(), fs.iter().map(|f| f.to_string()).collect())).collect();
        let cfg = StoreSinkConfig { namespace: "events".into(), retain: "30d".into(), time: "ts".into(), recent, novel, follow: follow.map(String::from) };
        StoreSink::new(&cfg, Arc::clone(store), Some(partition)).unwrap()
    }

    fn sink_with_novelty(store: &Arc<dyn StateStore>) -> Arc<StoreSink> {
        novelty_sink(store, &[("actor", &["_norm.kind", "_norm.ip"]), ("org", &["_norm.actor"])], None)
    }

    fn act(min: u32, actor: &str, kind: &str, ip: &str) -> serde_json::Value {
        json!({"ts": format!("2026-10-27T13:{min:02}:00Z"), "_norm": {"org": "acme", "actor": actor, "kind": kind, "ip": ip}})
    }

    const MIN: i64 = 60_000;
    const DAY: i64 = 86_400_000;
    /// 2020-01-01T00:00:00Z: an old year, replayed.
    const Y2020: i64 = 1_577_836_800_000;

    /// One record at `t`, epoch ms.
    fn at(t: i64, actor: &str, kind: &str, ip: &str) -> serde_json::Value {
        json!({"ts": t, "_norm": {"org": "acme", "actor": actor, "kind": kind, "ip": ip}})
    }

    /// Each record its own write, as a stream brings them.
    async fn each(sink: &StoreSink, records: &[serde_json::Value]) {
        for r in records {
            sink.write(write(std::slice::from_ref(r))).await.unwrap();
        }
    }

    /// The `_novel` of a partition's stored records, in record time order.
    async fn novelty_in(store: &Arc<dyn StateStore>, part: &str) -> Vec<serde_json::Value> {
        store
            .range("events", part, "ev:", "ev:~", 100)
            .await
            .unwrap()
            .into_iter()
            .map(|e| match e.value {
                StateValue::Str(t) => serde_json::from_str::<serde_json::Value>(&t).unwrap()[NOVEL].clone(),
                _ => panic!(),
            })
            .collect()
    }

    async fn novelty(store: &Arc<dyn StateStore>) -> Vec<serde_json::Value> {
        novelty_in(store, "acme").await
    }

    fn lru() -> Arc<dyn StateStore> {
        Arc::new(LruStore::new(StateConfig::default()).unwrap())
    }

    /// A record is marked with what its scopes had not had; the same again is not.
    #[tokio::test]
    async fn a_record_is_marked_with_what_is_new_for_its_scopes() {
        let store = lru();
        let sink = sink_with_novelty(&store);
        sink.write(write(&[act(1, "alice", "Describe", "10.0.0.1")])).await.unwrap();
        sink.write(write(&[act(2, "alice", "Describe", "10.0.0.1"), act(3, "alice", "CreateUser", "10.0.0.1")])).await.unwrap();
        sink.write(write(&[act(4, "alice", "CreateUser", "5.6.7.8"), act(5, "bob", "Describe", "10.0.0.1")])).await.unwrap();
        assert_eq!(
            novelty(&store).await,
            [
                json!({"actor": ["_norm.kind", "_norm.ip"], "org": ["_norm.actor"]}),
                serde_json::Value::Null,
                json!({"actor": ["_norm.kind"]}),
                json!({"actor": ["_norm.ip"]}),
                json!({"actor": ["_norm.kind", "_norm.ip"], "org": ["_norm.actor"]}),
            ],
            "alice's first record; the same again; a new action; a new address; bob's first"
        );
    }

    /// Record time decides, not arrival: a first sighting that arrives after
    /// a later one is marked, and the later one stays marked; within one
    /// write, the earliest is the one marked.
    #[tokio::test]
    async fn a_first_sighting_arriving_late_is_new() {
        let store = lru();
        let sink = sink_with_novelty(&store);
        each(&sink, &[
            act(1, "alice", "Describe", "10.0.0.1"),
            act(30, "alice", "Describe", "5.6.7.8"),
            act(10, "alice", "Describe", "5.6.7.8"),
            act(20, "alice", "Describe", "5.6.7.8"),
        ])
        .await;
        sink.write(write(&[act(50, "alice", "Get", "10.0.0.1"), act(40, "alice", "Get", "10.0.0.1")])).await.unwrap();
        assert_eq!(
            novelty(&store).await,
            [
                json!({"actor": ["_norm.kind", "_norm.ip"], "org": ["_norm.actor"]}),
                json!({"actor": ["_norm.ip"]}),
                serde_json::Value::Null,
                json!({"actor": ["_norm.ip"]}),
                json!({"actor": ["_norm.kind"]}),
                serde_json::Value::Null,
            ],
            "13:01; 13:10, the real first, though it came after 13:30; 13:20; 13:30, which came first; 13:40 and 13:50 in one write"
        );
    }

    /// A replayed year (an eval ingests one in seconds) forgets as the year
    /// did: a value is new again only when its latest sighting is more than
    /// `retain` before the record, and known for all of `retain` after it,
    /// however irregular the sightings.
    #[tokio::test]
    async fn a_replay_forgets_by_record_time() {
        let store = lru();
        let sink = sink_with_novelty(&store);
        let days = [0, 10, 32, 63, 64];
        each(&sink, &days.map(|d| at(Y2020 + d * DAY, "alice", "Describe", "10.0.0.1"))).await;
        let all = json!({"actor": ["_norm.kind", "_norm.ip"], "org": ["_norm.actor"]});
        assert_eq!(
            novelty(&store).await,
            [all.clone(), serde_json::Value::Null, serde_json::Value::Null, all, serde_json::Value::Null],
            "day 0; day 10; day 32, 22 days after day 10; day 63, 31 after day 32; day 64"
        );
    }

    /// A source running months behind another (a worker's records reaching
    /// the partition's owner late) sends a run of its own: its first record
    /// is new, not each of them.
    #[tokio::test]
    async fn a_run_arriving_after_a_later_one_is_new_once() {
        let store = lru();
        let sink = sink_with_novelty(&store);
        let days = [100, 0, 101, 1, 102, 2, 103, 3];
        each(&sink, &days.map(|d| at(Y2020 + d * DAY, "alice", "Describe", "10.0.0.1"))).await;
        let all = json!({"actor": ["_norm.kind", "_norm.ip"], "org": ["_norm.actor"]});
        let null = serde_json::Value::Null;
        assert_eq!(
            novelty(&store).await,
            [all.clone(), null.clone(), null.clone(), null.clone(), all, null.clone(), null.clone(), null],
            "days 0 to 3, arriving behind days 100 to 103: day 0 and day 100"
        );
    }

    /// Runs less than `retain` apart join; past four, the oldest is dropped,
    /// never the one a sighting is in, and is only new again.
    #[test]
    fn runs_join_and_the_oldest_is_dropped() {
        let retain = 30 * DAY;
        let mut seen = Seen::default();
        for d in [120, 80, 40, 0] {
            assert_eq!(seen.sight(d * DAY, retain), (true, true), "day {d}");
        }
        assert_eq!(seen.sight(60 * DAY, retain), (false, true), "day 60 is within 30 days of day 40");
        assert_eq!(seen.runs, [(0, 0), (40 * DAY, 80 * DAY), (120 * DAY, 120 * DAY)], "and joins it to day 80");
        assert_eq!(seen.sight(70 * DAY, retain), (false, false), "inside a run: nothing to write");
        seen.sight(200 * DAY, retain);
        seen.sight(300 * DAY, retain);
        assert_eq!(seen.runs[0], (40 * DAY, 80 * DAY), "day 0 dropped");
        assert_eq!(seen.sight(0, retain), (true, true), "so new again");
        assert_eq!(seen.runs, [(0, 0), (120 * DAY, 120 * DAY), (200 * DAY, 200 * DAY), (300 * DAY, 300 * DAY)]);
    }

    /// A busy value's entry moves a minute at a time: not a write a record.
    #[tokio::test]
    async fn a_busy_value_is_written_once_a_minute() {
        let store = lru();
        let sink = sink_with_novelty(&store);
        let key = format!("{}_norm.kind\u{1f}Describe", index_prefix("actor", "alice"));
        let t = Y2020;
        sink.write(write(&[at(t, "alice", "Describe", "10.0.0.1")])).await.unwrap();
        sink.write(write(&[at(t + 30_000, "alice", "Describe", "10.0.0.1")])).await.unwrap();
        let entry = Seen::read(store.get(&seen_ns("events"), "acme", &key).await.unwrap());
        assert_eq!(entry, Some(Seen { runs: vec![(t, t)] }), "30 s on: not written");
        sink.write(write(&[at(t + 90_000, "alice", "Describe", "10.0.0.1")])).await.unwrap();
        let entry = Seen::read(store.get(&seen_ns("events"), "acme", &key).await.unwrap());
        assert_eq!(entry, Some(Seen { runs: vec![(t, t + 90_000)] }));
    }

    /// What rows stored before novelty was remembered had is not new.
    #[tokio::test]
    async fn what_older_rows_had_is_not_new() {
        let store = lru();
        let old = crate::windows::ev_key(1_793_105_000_000, "01OLD");
        store
            .set("events", "acme", &old, &StateValue::Str(act(0, "alice", "Describe", "10.0.0.1").to_string()), None)
            .await
            .unwrap();
        let sink = sink_with_novelty(&store);
        sink.write(write(&[act(5, "alice", "Describe", "10.0.0.1")])).await.unwrap();
        assert_eq!(novelty(&store).await[1], serde_json::Value::Null);
        for (scope, field, f) in [("actor", "_norm.actor", "_norm.kind"), ("actor", "_norm.actor", "_norm.ip"), ("org", "_norm.org", "_norm.actor")] {
            assert!(get_int(&store, "acme", &remembered_marker(scope, field, f)).await.unwrap().is_some(), "{scope} {f}");
        }
    }

    /// Older rows are remembered at their own time, not at the first write:
    /// what was last had 40 days before is new, what 15 days before is not.
    #[tokio::test]
    async fn older_rows_are_remembered_at_their_own_time() {
        let store = lru();
        for (d, kind) in [(0, "Describe"), (25, "Get")] {
            let key = crate::windows::ev_key(Y2020 + d * DAY, &format!("01OLD{d}"));
            let row = at(Y2020 + d * DAY, "alice", kind, "10.0.0.1").to_string();
            store.set("events", "acme", &key, &StateValue::Str(row), None).await.unwrap();
        }
        let sink = sink_with_novelty(&store);
        each(&sink, &[at(Y2020 + 40 * DAY, "alice", "Describe", "10.0.0.1"), at(Y2020 + 40 * DAY + MIN, "alice", "Get", "10.0.0.1")]).await;
        assert_eq!(
            novelty(&store).await[2..],
            [json!({"actor": ["_norm.kind"]}), serde_json::Value::Null]
        );
    }

    /// A field listed later learns what the rows had of it, at the first
    /// write after: adding it marks nothing that was usual.
    #[tokio::test]
    async fn a_field_listed_later_learns_what_older_rows_had() {
        let store = lru();
        let before = novelty_sink(&store, &[("actor", &["_norm.kind"])], None);
        before.write(write(&[act(1, "alice", "Describe", "10.0.0.1")])).await.unwrap();
        assert!(get_int(&store, "acme", &remembered_marker("actor", "_norm.actor", "_norm.ip")).await.unwrap().is_none());
        let after = sink_with_novelty(&store);
        after.write(write(&[act(5, "alice", "Describe", "10.0.0.1"), act(6, "alice", "Describe", "5.6.7.8")])).await.unwrap();
        assert_eq!(
            novelty(&store).await,
            [json!({"actor": ["_norm.kind"]}), serde_json::Value::Null, json!({"actor": ["_norm.ip"]})],
            "the address and the actor were in the rows; only a new address is new"
        );
    }

    /// A record with no value for any novel field cannot be told either way:
    /// it is marked, every time, so a source the config does not cover is
    /// still judged.
    #[tokio::test]
    async fn a_record_novelty_cannot_read_is_marked() {
        let store = lru();
        let sink = sink_with_novelty(&store);
        let vpn = json!({"ts": "2026-10-27T13:00:00Z", "source": "vpn", "user": "dave", "action": "connect"});
        each(&sink, &[vpn.clone(), vpn, json!({"ts": "2026-10-27T13:01:00Z", "_norm": {"org": "acme"}})]).await;
        let unscoped = json!({"_": ["unscoped"]});
        assert_eq!(novelty_in(&store, NO_PARTITION).await, [unscoped.clone(), unscoped.clone()]);
        assert_eq!(novelty(&store).await, [unscoped], "an org with none of its fields");
    }

    /// With `follow`, an actor's records within it after something new for
    /// the actor are marked too, beside any real marks; later ones and
    /// other actors' are not, and the org, the partition, is not followed.
    #[tokio::test]
    async fn the_records_after_a_new_one_follow_it() {
        let store = lru();
        let sink = novelty_sink(&store, &[("actor", &["_norm.kind", "_norm.ip"]), ("org", &["_norm.actor", "_norm.dest"])], Some("1h"));
        let t = Y2020 + 10 * DAY;
        let mut to_dest = at(t + 20 * MIN, "alice", "Describe", "10.0.0.1");
        to_dest["_norm"]["dest"] = json!("paste.example");
        each(&sink, &[
            at(t - DAY, "alice", "Describe", "10.0.0.1"),
            at(t - DAY + MIN, "bob", "Describe", "10.0.0.1"),
            at(t, "alice", "Describe", "5.6.7.8"),
            at(t + 10 * MIN, "alice", "Describe", "10.0.0.1"),
            at(t + 11 * MIN, "bob", "Describe", "10.0.0.1"),
            to_dest,
            at(t + 25 * MIN, "bob", "Describe", "10.0.0.1"),
            at(t + 120 * MIN, "alice", "Describe", "10.0.0.1"),
        ])
        .await;
        let first = json!({"actor": ["_norm.kind", "_norm.ip"], "org": ["_norm.actor"]});
        assert_eq!(
            novelty(&store).await,
            [
                first.clone(),
                first,
                json!({"actor": ["_norm.ip"]}),
                json!({"actor": ["follows"]}),
                serde_json::Value::Null,
                json!({"org": ["_norm.dest"], "actor": ["follows"]}),
                serde_json::Value::Null,
                serde_json::Value::Null,
            ],
            "a day of routine; alice from a new network; her routine 10 min on; bob's; \
             hers to a new destination; bob's after it; hers 2 h on"
        );
    }

    /// One window marks fifty records at most.
    #[tokio::test]
    async fn a_follow_window_marks_fifty_at_most() {
        let store = lru();
        let sink = novelty_sink(&store, &[("actor", &["_norm.kind"])], Some("1h"));
        let routine: Vec<serde_json::Value> = (0..61).map(|i| at(Y2020 + i * 10_000, "alice", "Describe", "10.0.0.1")).collect();
        each(&sink, &routine).await;
        let marks = novelty(&store).await;
        assert_eq!(marks[0], json!({"actor": ["_norm.kind"]}));
        assert_eq!(marks[1..].iter().filter(|m| **m == json!({"actor": ["follows"]})).count(), 50);
        assert!(marks[51..].iter().all(serde_json::Value::is_null));
    }

    /// A store whose `fail_at`th record write fails, once (a disk filling
    /// up), and whose write of a record with a `slow` field waits for
    /// `release` (a write still in flight while others run).
    struct Probe {
        inner: LruStore,
        writes: AtomicUsize,
        fail_at: usize,
        held: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    fn probe(fail_at: usize) -> Arc<Probe> {
        let inner = LruStore::new(StateConfig::default()).unwrap();
        Arc::new(Probe { inner, writes: AtomicUsize::new(0), fail_at, held: Default::default(), release: Default::default() })
    }

    #[async_trait]
    impl StateStore for Probe {
        async fn get(&self, ns: &str, partition: &str, key: &str) -> Result<Option<StateValue>> {
            self.inner.get(ns, partition, key).await
        }
        async fn set(&self, ns: &str, partition: &str, key: &str, value: &StateValue, ttl_ms: Option<u64>) -> Result<()> {
            if ns == "events" {
                if self.writes.fetch_add(1, Ordering::SeqCst) == self.fail_at {
                    anyhow::bail!("disk full");
                }
                if matches!(value, StateValue::Str(t) if t.contains("\"slow\"")) {
                    self.held.notify_one();
                    self.release.notified().await;
                }
            }
            self.inner.set(ns, partition, key, value, ttl_ms).await
        }
        async fn del(&self, ns: &str, partition: &str, key: &str) -> Result<bool> {
            self.inner.del(ns, partition, key).await
        }
        async fn incr(&self, ns: &str, partition: &str, key: &str, by: i64, ttl_ms: Option<u64>) -> Result<i64> {
            self.inner.incr(ns, partition, key, by, ttl_ms).await
        }
        async fn scan(&self, ns: Option<&str>, partition: Option<&str>, key_prefix: &str, limit: usize) -> Result<Vec<Entry>> {
            self.inner.scan(ns, partition, key_prefix, limit).await
        }
        async fn range(&self, ns: &str, partition: &str, from: &str, to: &str, limit: usize) -> Result<Vec<Entry>> {
            self.inner.range(ns, partition, from, to, limit).await
        }
        async fn range_rev(&self, ns: &str, partition: &str, from: &str, to: &str, limit: usize) -> Result<Vec<Entry>> {
            self.inner.range_rev(ns, partition, from, to, limit).await
        }
        async fn counts(&self, ns: Option<&str>) -> Result<Vec<Count>> {
            self.inner.counts(ns).await
        }
        async fn purge_expired(&self) -> Result<u64> {
            self.inner.purge_expired().await
        }
        async fn reset(&self) -> Result<()> {
            self.inner.reset().await
        }
    }

    /// A write that fails after deciding its marks is tried again with the
    /// same marks: nothing is remembered until its records are stored.
    #[tokio::test]
    async fn a_write_tried_again_marks_the_same_records() {
        let store: Arc<dyn StateStore> = probe(1);
        let sink = sink_with_novelty(&store);
        let payload = || write(&[act(1, "alice", "Describe", "10.0.0.1"), act(2, "alice", "Describe", "5.6.7.8")]);
        assert!(sink.write(payload()).await.is_err(), "the second record's write fails");
        sink.write(payload()).await.unwrap();
        let first = json!({"actor": ["_norm.kind", "_norm.ip"], "org": ["_norm.actor"]});
        assert_eq!(
            novelty(&store).await,
            [first.clone(), first, json!({"actor": ["_norm.ip"]})],
            "13:01 twice (stored by both tries), and 13:02, the record in flight, still marked"
        );
    }

    /// `rec` with a `slow` field: its write waits until the probe releases it.
    fn slow(mut rec: serde_json::Value) -> serde_json::Value {
        rec["slow"] = json!(true);
        rec
    }

    /// Writes `a`, holds it in flight while `then` is sent to the same
    /// partition, and lets it finish: `then` waits its turn.
    async fn beside(store: &Arc<Probe>, sink: &Arc<StoreSink>, a: serde_json::Value, then: serde_json::Value) {
        let first = tokio::spawn({
            let sink = Arc::clone(sink);
            async move { sink.write(write(&[slow(a)])).await }
        });
        store.held.notified().await;
        let second = tokio::spawn({
            let sink = Arc::clone(sink);
            async move { sink.write(write(&[then])).await }
        });
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(!second.is_finished(), "a write waits for the one before it");
        store.release.notify_one();
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
    }

    /// A record written while a new one is still in flight follows it: its
    /// write reads the window once the new one's write has written it.
    #[tokio::test]
    async fn a_record_written_beside_a_new_one_follows_it() {
        let probe = probe(usize::MAX);
        let store: Arc<dyn StateStore> = probe.clone();
        let sink = novelty_sink(&store, &[("actor", &["_norm.ip"])], Some("1h"));
        each(&sink, &[at(Y2020 - DAY, "alice", "Describe", "10.0.0.1")]).await;
        beside(&probe, &sink, at(Y2020, "alice", "Describe", "5.6.7.8"), at(Y2020 + 10 * MIN, "alice", "Describe", "10.0.0.1")).await;
        assert_eq!(novelty(&store).await[1..], [json!({"actor": ["_norm.ip"]}), json!({"actor": ["follows"]})]);
    }

    /// Two writes side by side: the second reads what the first saw, so a
    /// value is new once, and the first does not write over the second's
    /// sightings.
    #[tokio::test]
    async fn writes_side_by_side_read_what_the_one_before_saw() {
        let probe = probe(usize::MAX);
        let store: Arc<dyn StateStore> = probe.clone();
        let sink = novelty_sink(&store, &[("actor", &["_norm.kind"])], None);
        beside(&probe, &sink, at(Y2020, "alice", "Describe", "10.0.0.1"), at(Y2020 + MIN, "alice", "Describe", "10.0.0.1")).await;
        beside(&probe, &sink, at(Y2020 + DAY, "alice", "Describe", "10.0.0.1"), at(Y2020 + 100 * DAY, "alice", "Describe", "10.0.0.1")).await;
        each(&sink, &[at(Y2020 + 101 * DAY, "alice", "Describe", "10.0.0.1")]).await;
        let new = json!({"actor": ["_norm.kind"]});
        let null = serde_json::Value::Null;
        assert_eq!(
            novelty(&store).await,
            [new.clone(), null.clone(), null.clone(), new, null],
            "day 0; a minute on, written beside it; day 1; day 100, written beside day 1; day 101"
        );
    }

    #[test]
    fn a_novel_scope_needs_a_field() {
        let store: Arc<dyn StateStore> = Arc::new(LruStore::new(StateConfig::default()).unwrap());
        let novel = BTreeMap::from([("actor".to_string(), vec!["_norm.kind".to_string()])]);
        let cfg = StoreSinkConfig { namespace: "events".into(), retain: "30d".into(), time: "ts".into(), recent: BTreeMap::new(), novel, follow: None };
        assert!(StoreSink::new(&cfg, store, None).is_err());
    }

    #[test]
    fn a_follow_is_a_duration() {
        let cfg = |follow: &str| StoreSinkConfig { namespace: "events".into(), retain: "30d".into(), time: "ts".into(), recent: BTreeMap::new(), novel: BTreeMap::new(), follow: Some(follow.into()) };
        assert!(StoreSink::new(&cfg("soon"), lru(), None).is_err());
        assert_eq!(StoreSink::new(&cfg("1h"), lru(), None).unwrap().follow_ms, Some(3_600_000));
    }

    /// Rows stored before the index existed are indexed at the first write.
    #[tokio::test]
    async fn records_stored_before_the_index_are_indexed_once() {
        let store: Arc<dyn StateStore> = Arc::new(LruStore::new(StateConfig::default()).unwrap());
        let old = crate::windows::ev_key(1_793_105_000_000, "01OLD");
        store
            .set("events", "acme", &old, &StateValue::Str(serde_json::json!({"ts": "2026-10-27T12:43:20Z", "_norm": {"org": "acme", "actor": "carol"}}).to_string()), None)
            .await
            .unwrap();
        let sink = sink_with_scopes(&store);
        assert!(latest(&store, "actor", "carol", "ev:~", 10).await.is_empty());
        sink.write(write(&[rec(5, "alice")])).await.unwrap();
        assert_eq!(latest(&store, "actor", "carol", "ev:~", 10).await, [old], "carol's old record, indexed");
        assert!(get_int(&store, "acme", &indexed_marker("actor", "_norm.actor")).await.unwrap().is_some());
    }
}
