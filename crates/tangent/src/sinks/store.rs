use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use anyhow::{bail, Result};
use async_trait::async_trait;
use serde_json::{json, Map, Value};
use tangent_shared::runtime::PartitionConfig;
use tangent_shared::sinks::store::StoreSinkConfig;

use crate::inference::lookup;
use crate::partition::key_of_value;
use crate::sinks::manager::{Sink, SinkWrite};
use crate::state::{now_ms, StateStore, StateValue};
use crate::windows::{
    ev_key, get_int, index_prefix, scope_value, set_int, time_of_value, ts_of_key, CLOSED_THROUGH,
    EVENTS_NS, FOLLOW_NS, INDEXED, INDEX_NS, LAST_ARRIVAL, LATE_PREFIX, READ_THROUGH, REMEMBERED,
    SEEN_NS, TIME_FIELD, WATERMARK, WINDOWS_NS,
};
use crate::{SINK_BYTES_TOTAL, SINK_OBJECTS_TOTAL};

const NO_PARTITION: &str = "_";
const NOVEL: &str = "_novel";

#[derive(Debug)]
struct NovelScope {
    name: String,
    field: String,
    fields: Vec<String>,
    follow: bool, // not when the scope is the partition
}

/// What one write read and changed: last-seen times, and follow windows as (start, count).
#[derive(Default)]
struct Learned {
    seen: HashMap<String, (Option<i64>, bool)>,
    windows: HashMap<String, (Option<(i64, i64)>, bool)>,
}

async fn cached<'a, T>(
    learned: &'a mut HashMap<String, (Option<T>, bool)>,
    store: &dyn StateStore,
    ns: &str,
    part: &str,
    key: String,
    read: fn(StateValue) -> Option<T>,
) -> Result<&'a mut (Option<T>, bool)> {
    if !learned.contains_key(&key) {
        let had = store.get(ns, part, &key).await?.and_then(read);
        learned.insert(key.clone(), (had, false));
    }
    Ok(learned.get_mut(&key).expect("read above"))
}

pub struct StoreSink {
    store: Arc<dyn StateStore>,
    partition: Option<PartitionConfig>,
    retain_ms: u64,
    /// (scope, field), less the scopes that are a partition key
    indexed: Vec<(String, String)>,
    novel: Vec<NovelScope>,
    follow_ms: Option<i64>,
    /// held for a whole write; the partitions prepared since the start
    bookkeeping: tokio::sync::Mutex<HashSet<String>>,
}

impl StoreSink {
    pub fn new(
        cfg: &StoreSinkConfig,
        store: Arc<dyn StateStore>,
        partition: Option<PartitionConfig>,
    ) -> Result<Arc<Self>> {
        let partition_fields: Vec<&String> = partition.iter().flat_map(|p| p.key.iter()).collect();
        let indexed = cfg
            .recent
            .iter()
            .filter(|(_, field)| !partition_fields.contains(field))
            .map(|(s, f)| (s.clone(), f.clone()))
            .collect();
        let mut novel = Vec::new();
        for (scope, fields) in &cfg.novel {
            let Some(field) = cfg.recent.get(scope) else {
                bail!("store sink: novel scope {scope} is not in recent");
            };
            novel.push(NovelScope {
                name: scope.clone(),
                field: field.clone(),
                fields: fields.clone(),
                follow: !partition_fields.contains(&field),
            });
        }
        Ok(Arc::new(Self {
            store,
            partition,
            retain_ms: cfg.retain_secs * 1000,
            indexed,
            novel,
            follow_ms: cfg.follow_secs.map(|s| s as i64 * 1000),
            bookkeeping: tokio::sync::Mutex::new(HashSet::new()),
        }))
    }

    async fn index(&self, part: &str, key: &str, v: &Value) -> Result<()> {
        for (scope, field) in &self.indexed {
            if let Some(value) = lookup(v, field).and_then(scope_value) {
                let at = format!("{}{key}", index_prefix(scope, &value));
                self.store
                    .set(INDEX_NS, part, &at, &StateValue::Str(key.to_string()), Some(self.retain_ms))
                    .await?;
            }
        }
        Ok(())
    }

    /// A record's marks by scope: fields new within `retain`, `follows`, or `unscoped`.
    async fn check(&self, part: &str, ts: i64, v: &Value, follow: Option<i64>, learned: &mut Learned) -> Result<Map<String, Value>> {
        let retain = self.retain_ms as i64;
        let mut new = Map::new();
        let mut any = false;
        for s in &self.novel {
            let Some(sv) = lookup(v, &s.field).and_then(scope_value) else { continue };
            let mut marks = Vec::new();
            for f in &s.fields {
                let Some(value) = lookup(v, f).and_then(scope_value) else { continue };
                any = true;
                let at = format!("{}{f}\u{1f}{value}", index_prefix(&s.name, &sv));
                let int = |v| match v {
                    StateValue::Int(t) => Some(t),
                    _ => None,
                };
                let (last, changed) = cached(&mut learned.seen, &*self.store, SEEN_NS, part, at, int).await?;
                if last.is_none_or(|t| ts - t > retain) {
                    marks.push(Value::String(f.clone()));
                }
                // a busy value is written once a grain, not once a record
                if last.is_none_or(|t| ts - t > retain / 720) {
                    *last = Some(ts);
                    *changed = true;
                }
            }
            if let Some(follow) = follow.filter(|_| s.follow) {
                let pair = |v| match v {
                    StateValue::Json(j) => Some((j[0].as_i64()?, j[1].as_i64()?)),
                    _ => None,
                };
                let at = index_prefix(&s.name, &sv);
                let (w, changed) = cached(&mut learned.windows, &*self.store, FOLLOW_NS, part, at, pair).await?;
                if !marks.is_empty() {
                    if w.is_none_or(|(start, _)| ts >= start) {
                        *w = Some((ts, 0));
                        *changed = true;
                    }
                } else if let Some(w) = w.as_mut().filter(|w| ts >= w.0 && ts - w.0 <= follow && w.1 < 50) {
                    w.1 += 1;
                    *changed = true;
                    marks.push(Value::String("follows".into()));
                }
            }
            if !marks.is_empty() {
                new.insert(s.name.clone(), Value::Array(marks));
            }
        }
        if !any && !self.novel.is_empty() {
            new.insert("_".into(), json!(["unscoped"]));
        }
        Ok(new)
    }

    async fn commit(&self, learned: BTreeMap<String, Learned>) -> Result<()> {
        // past retain on the clock, so an entry outlives the record time it is new again at
        let ttl = Some(self.retain_ms * 3 / 2);
        for (part, l) in learned {
            for (at, (w, changed)) in l.windows {
                if let (Some((start, count)), true) = (w, changed) {
                    self.store.set(FOLLOW_NS, &part, &at, &StateValue::Json(json!([start, count])), ttl).await?;
                }
            }
            for (at, (last, changed)) in l.seen {
                if let (Some(last), true) = (last, changed) {
                    self.store.set(SEEN_NS, &part, &at, &StateValue::Int(last), ttl).await?;
                }
            }
        }
        Ok(())
    }

    /// A partition's rows stored before, read once per config for the index and novelty.
    async fn prepare(&self, part: &str) -> Result<()> {
        let (indexed, novel) = (format!("{:?}", self.indexed), format!("{:?}", self.novel));
        let index = !self.indexed.is_empty()
            && self.store.get(WINDOWS_NS, part, INDEXED).await? != Some(StateValue::Str(indexed.clone()));
        let remember = !self.novel.is_empty()
            && self.store.get(WINDOWS_NS, part, REMEMBERED).await? != Some(StateValue::Str(novel.clone()));
        if !index && !remember {
            return Ok(());
        }
        let (mut from, mut n) = ("ev:".to_string(), 0usize);
        loop {
            let page = self.store.range(EVENTS_NS, part, &from, "ev:~", 2_000).await?;
            let Some(last) = page.last() else { break };
            from = format!("{}!", last.key);
            let mut learned = Learned::default();
            for e in &page {
                if let (StateValue::Str(text), Some(ts)) = (&e.value, ts_of_key(&e.key)) {
                    if let Ok(v) = serde_json::from_str::<Value>(text) {
                        if index {
                            self.index(part, &e.key, &v).await?;
                        }
                        if remember {
                            self.check(part, ts, &v, None, &mut learned).await?;
                        }
                        n += 1;
                    }
                }
            }
            self.commit(BTreeMap::from([(part.to_string(), learned)])).await?;
        }
        if n > 0 {
            tracing::info!(partition = %part, records = n, "store sink: read the records stored before");
        }
        if index {
            self.store.set(WINDOWS_NS, part, INDEXED, &StateValue::Str(indexed), Some(0)).await?;
        }
        if remember {
            self.store.set(WINDOWS_NS, part, REMEMBERED, &StateValue::Str(novel), Some(0)).await?;
        }
        Ok(())
    }
}

#[async_trait]
impl Sink for StoreSink {
    async fn write(&self, req: SinkWrite) -> Result<()> {
        let now = now_ms() as i64;
        let mut records = Vec::new();
        for line in crate::inference::lines(&req.payload) {
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
            let ts = time_of_value(&v, TIME_FIELD).unwrap_or(now);
            records.push((part, ts, v, line));
        }
        let mut ready = self.bookkeeping.lock().await;
        for (part, ..) in &records {
            if !ready.contains(part) {
                self.prepare(part).await?;
                ready.insert(part.clone());
            }
        }

        // in record time order: of a write's sightings of a value, the earliest is marked
        let mut learned: BTreeMap<String, Learned> = BTreeMap::new();
        let mut marks = vec![Map::new(); records.len()];
        if !self.novel.is_empty() {
            let mut order: Vec<usize> = (0..records.len()).collect();
            order.sort_by_key(|&i| records[i].1);
            for i in order {
                let (part, ts, v, _) = &records[i];
                let l = learned.entry(part.clone()).or_default();
                marks[i] = self.check(part, *ts, v, self.follow_ms, l).await?;
            }
        }

        // per partition: (time, key)
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
                .set(EVENTS_NS, &part, &key, &StateValue::Str(text), Some(self.retain_ms))
                .await?;
            self.index(&part, &key, &v).await?;
            stored.entry(part).or_default().push((ts, key));
        }

        for (part, rows) in stored {
            let max = rows.iter().map(|(t, _)| *t).max().unwrap_or(now);
            if get_int(&self.store, &part, WATERMARK).await?.is_none_or(|w| max > w) {
                set_int(&self.store, &part, WATERMARK, max).await?;
            }
            set_int(&self.store, &part, LAST_ARRIVAL, now).await?;
            // read after the rows are written: a window read before missed them
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
        // last: a write that failed before here is tried again with the same marks
        self.commit(learned).await?;
        drop(ready);
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
    use crate::windows::ev_bound;
    use bytes::BytesMut;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tangent_shared::runtime::StateConfig;

    const MIN: i64 = 60_000;
    const DAY: i64 = 86_400_000;
    const Y2020: i64 = 1_577_836_800_000;

    fn write(lines: &[Value]) -> SinkWrite {
        let mut payload = BytesMut::new();
        for l in lines {
            payload.extend_from_slice(l.to_string().as_bytes());
            payload.extend_from_slice(b"\n");
        }
        SinkWrite { sink_name: Arc::from("store"), payload, s3: None }
    }

    fn lru() -> Arc<dyn StateStore> {
        Arc::new(LruStore::new(StateConfig::default()).unwrap())
    }

    fn sink(store: &Arc<dyn StateStore>, novel: &[(&str, &[&str])], follow_secs: Option<u64>) -> Arc<StoreSink> {
        let partition = PartitionConfig { key: vec!["_norm.org".into()], separator: "|".into(), retain_ms: None, time: None };
        let recent = BTreeMap::from([("actor".to_string(), "_norm.actor".to_string()), ("org".to_string(), "_norm.org".to_string())]);
        let novel = novel.iter().map(|(s, fs)| (s.to_string(), fs.iter().map(|f| f.to_string()).collect())).collect();
        let cfg = StoreSinkConfig { retain_secs: 2_592_000, recent, novel, follow_secs };
        StoreSink::new(&cfg, Arc::clone(store), Some(partition)).unwrap()
    }

    const ALL: &[(&str, &[&str])] = &[("actor", &["_norm.kind", "_norm.ip"]), ("org", &["_norm.actor"])];

    fn at(t: i64, actor: &str, kind: &str, ip: &str) -> Value {
        json!({"ts": t, "_norm": {"org": "acme", "actor": actor, "kind": kind, "ip": ip}})
    }

    async fn each(sink: &StoreSink, records: &[Value]) {
        for r in records {
            sink.write(write(std::slice::from_ref(r))).await.unwrap();
        }
    }

    async fn novelty_in(store: &Arc<dyn StateStore>, part: &str) -> Vec<Value> {
        store
            .range("events", part, "ev:", "ev:~", 100)
            .await
            .unwrap()
            .into_iter()
            .map(|e| match e.value {
                StateValue::Str(t) => serde_json::from_str::<Value>(&t).unwrap()[NOVEL].clone(),
                _ => panic!(),
            })
            .collect()
    }

    async fn novelty(store: &Arc<dyn StateStore>) -> Vec<Value> {
        novelty_in(store, "acme").await
    }

    async fn latest(store: &Arc<dyn StateStore>, scope: &str, value: &str, bound: &str, n: usize) -> Vec<String> {
        let prefix = index_prefix(scope, value);
        store
            .range_rev(INDEX_NS, "acme", &format!("{prefix}ev:"), &format!("{prefix}{bound}"), n)
            .await
            .unwrap()
            .into_iter()
            .map(|e| match e.value { StateValue::Str(k) => k, _ => panic!() })
            .collect()
    }

    #[tokio::test]
    async fn records_are_kept_by_partition_and_time_and_late_ones_are_marked() {
        let store = lru();
        let sink = sink(&store, &[], None);
        sink.write(write(&[
            json!({"ts": "2026-10-27T13:05:00Z", "_norm": {"org": "acme"}, "n": 2}),
            json!({"ts": "2026-10-27T13:01:00Z", "_norm": {"org": "acme"}, "n": 1}),
            json!({"ts": "2026-10-27T13:02:00Z", "_norm": {"org": "globex"}}),
        ]))
        .await
        .unwrap();
        let rows = store.range("events", "acme", "ev:", "ev:~", 100).await.unwrap();
        assert_eq!(rows.len(), 2);
        assert!(ts_of_key(&rows[0].key) < ts_of_key(&rows[1].key), "time order, whatever the arrival order");
        assert_eq!(get_int(&store, "acme", WATERMARK).await.unwrap(), Some(1_793_106_300_000));
        assert!(get_int(&store, "globex", WATERMARK).await.unwrap().is_some(), "each partition its own");

        // read through 13:15: 13:10 is late, 13:20 is not
        set_int(&store, "acme", READ_THROUGH, 1_793_106_900_000).await.unwrap();
        sink.write(write(&[
            json!({"ts": "2026-10-27T13:10:00Z", "_norm": {"org": "acme"}}),
            json!({"ts": "2026-10-27T13:20:00Z", "_norm": {"org": "acme"}}),
        ]))
        .await
        .unwrap();
        let late = store.scan(Some(WINDOWS_NS), Some("acme"), LATE_PREFIX, 10).await.unwrap();
        assert_eq!(late.len(), 1, "{late:?}");
        let StateValue::Str(ev) = &late[0].value else { panic!() };
        assert_eq!(ts_of_key(ev), Some(1_793_106_600_000));
    }

    #[tokio::test]
    async fn every_record_is_indexed_by_its_actor_and_read_back_newest_first() {
        let store = lru();
        let sink = sink(&store, &[], None);
        let rec = |m: i64, actor: &str| at(Y2020 + m * MIN, actor, "Describe", "10.0.0.1");
        sink.write(write(&[rec(5, "alice"), rec(1, "alice"), rec(4, "alice"), rec(2, "bob"), rec(3, "alice")])).await.unwrap();
        let mins: Vec<i64> = latest(&store, "actor", "alice", &ev_bound(Y2020 + 4 * MIN), 2)
            .await
            .iter()
            .map(|k| (ts_of_key(k).unwrap() - Y2020) / MIN)
            .collect();
        assert_eq!(mins, [3, 1], "alice's latest two before minute 4, newest first");
        assert_eq!(latest(&store, "actor", "bob", "ev:~", 10).await.len(), 1);
        assert!(latest(&store, "org", "acme", "ev:~", 10).await.is_empty(), "the org is the partition: not indexed");
        for k in latest(&store, "actor", "alice", "ev:~", 1000).await {
            assert!(store.get("events", "acme", &k).await.unwrap().is_some(), "an indexed key is a stored record");
        }
    }

    #[tokio::test]
    async fn records_stored_before_the_index_are_indexed_once() {
        let store = lru();
        let old = ev_key(Y2020, "01OLD");
        store.set("events", "acme", &old, &StateValue::Str(at(Y2020, "carol", "Get", "10.0.0.1").to_string()), None).await.unwrap();
        let sink = sink(&store, &[], None);
        assert!(latest(&store, "actor", "carol", "ev:~", 10).await.is_empty());
        sink.write(write(&[at(Y2020 + MIN, "alice", "Get", "10.0.0.1")])).await.unwrap();
        assert_eq!(latest(&store, "actor", "carol", "ev:~", 10).await, [old], "carol's old record, indexed");
        assert!(store.get(WINDOWS_NS, "acme", INDEXED).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn marks_what_is_new() {
        let store = lru();
        let sink = sink(&store, ALL, None);
        let m = |m: i64, actor: &str, kind: &str, ip: &str| at(Y2020 + m * MIN, actor, kind, ip);
        sink.write(write(&[m(1, "alice", "Describe", "10.0.0.1")])).await.unwrap();
        sink.write(write(&[m(3, "alice", "CreateUser", "10.0.0.1"), m(2, "alice", "CreateUser", "10.0.0.1")])).await.unwrap();
        sink.write(write(&[m(4, "alice", "CreateUser", "5.6.7.8"), m(5, "bob", "Describe", "10.0.0.1")])).await.unwrap();
        let all = json!({"actor": ["_norm.kind", "_norm.ip"], "org": ["_norm.actor"]});
        assert_eq!(
            novelty(&store).await,
            [all.clone(), json!({"actor": ["_norm.kind"]}), Value::Null, json!({"actor": ["_norm.ip"]}), all],
            "alice's first; a new action, the earlier of a write's two; the same again; a new address; bob's first"
        );
    }

    #[tokio::test]
    async fn forgets_by_record_time() {
        let store = lru();
        let sink = sink(&store, ALL, None);
        let days = [0, 10, 32, 63, 64];
        each(&sink, &days.map(|d| at(Y2020 + d * DAY, "alice", "Describe", "10.0.0.1"))).await;
        let all = json!({"actor": ["_norm.kind", "_norm.ip"], "org": ["_norm.actor"]});
        assert_eq!(
            novelty(&store).await,
            [all.clone(), Value::Null, Value::Null, all, Value::Null],
            "day 0; day 10; day 32, 22 days after day 10; day 63, 31 after day 32; day 64"
        );
    }

    #[tokio::test]
    async fn busy_value_written_once_a_grain() {
        let store = lru();
        let sink = sink(&store, ALL, None);
        let key = format!("{}_norm.kind\u{1f}Describe", index_prefix("actor", "alice"));
        let mut written = Vec::new();
        for m in [0, 59, 61] {
            sink.write(write(&[at(Y2020 + m * MIN, "alice", "Describe", "10.0.0.1")])).await.unwrap();
            written.push(store.get(SEEN_NS, "acme", &key).await.unwrap());
        }
        assert_eq!(written, [0, 0, 61].map(|m| Some(StateValue::Int(Y2020 + m * MIN))), "an hour a grain at 30 days");
    }

    #[tokio::test]
    async fn remembers_older_rows() {
        let store = lru();
        for (d, kind) in [(0, "Describe"), (25, "Get")] {
            let row = at(Y2020 + d * DAY, "alice", kind, "10.0.0.1").to_string();
            store.set("events", "acme", &ev_key(Y2020 + d * DAY, &format!("01OLD{d}")), &StateValue::Str(row), None).await.unwrap();
        }
        let sink = sink(&store, ALL, None);
        each(&sink, &[at(Y2020 + 40 * DAY, "alice", "Describe", "10.0.0.1"), at(Y2020 + 40 * DAY + MIN, "alice", "Get", "10.0.0.1")]).await;
        assert_eq!(
            novelty(&store).await[2..],
            [json!({"actor": ["_norm.kind"]}), Value::Null],
            "each row at its own time: Describe last 40 days before, Get 15"
        );
        assert!(store.get(WINDOWS_NS, "acme", REMEMBERED).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn new_field_learns_older_rows() {
        let store = lru();
        let before = sink(&store, &[("actor", &["_norm.kind"])], None);
        before.write(write(&[at(Y2020 + MIN, "alice", "Describe", "10.0.0.1")])).await.unwrap();
        let after = sink(&store, ALL, None);
        after
            .write(write(&[at(Y2020 + 5 * MIN, "alice", "Describe", "10.0.0.1"), at(Y2020 + 6 * MIN, "alice", "Describe", "5.6.7.8")]))
            .await
            .unwrap();
        assert_eq!(
            novelty(&store).await,
            [json!({"actor": ["_norm.kind"]}), Value::Null, json!({"actor": ["_norm.ip"]})],
            "the address and the actor were in the rows; only a new address is new"
        );
    }

    #[tokio::test]
    async fn unscoped_is_marked() {
        let store = lru();
        let sink = sink(&store, ALL, None);
        let vpn = json!({"ts": Y2020, "source": "vpn", "user": "dave", "action": "connect"});
        each(&sink, &[vpn.clone(), vpn, json!({"ts": Y2020 + MIN, "_norm": {"org": "acme"}})]).await;
        let unscoped = json!({"_": ["unscoped"]});
        assert_eq!(novelty_in(&store, NO_PARTITION).await, [unscoped.clone(), unscoped.clone()]);
        assert_eq!(novelty(&store).await, [unscoped], "an org with none of its fields");
    }

    #[tokio::test]
    async fn follows_a_new_one() {
        let store = lru();
        let sink = sink(&store, &[("actor", &["_norm.kind", "_norm.ip"]), ("org", &["_norm.actor", "_norm.dest"])], Some(3600));
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
                Value::Null,
                json!({"org": ["_norm.dest"], "actor": ["follows"]}),
                Value::Null,
                Value::Null,
            ],
            "a day of routine; alice from a new network; her routine 10 min on; bob's; \
             hers to a new destination; bob's after it; hers 2 h on"
        );
    }

    #[tokio::test]
    async fn follows_fifty_at_most() {
        let store = lru();
        let sink = sink(&store, &[("actor", &["_norm.kind"])], Some(3600));
        let routine: Vec<Value> = (0..61).map(|i| at(Y2020 + i * 10_000, "alice", "Describe", "10.0.0.1")).collect();
        each(&sink, &routine).await;
        let marks = novelty(&store).await;
        assert_eq!(marks[0], json!({"actor": ["_norm.kind"]}));
        assert_eq!(marks[1..].iter().filter(|m| **m == json!({"actor": ["follows"]})).count(), 50);
        assert!(marks[51..].iter().all(Value::is_null));
    }

    /// fails its `fail_at`th events write, once; holds a `slow` record's write until released
    struct Probe {
        inner: LruStore,
        writes: AtomicUsize,
        fail_at: usize,
        held: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    fn probe(fail_at: usize) -> Arc<Probe> {
        let inner = LruStore::new(StateConfig::default()).unwrap();
        let (held, release) = Default::default();
        Arc::new(Probe { inner, writes: AtomicUsize::new(0), fail_at, held, release })
    }

    #[async_trait]
    impl StateStore for Probe {
        async fn get(&self, ns: &str, partition: &str, key: &str) -> Result<Option<StateValue>> {
            self.inner.get(ns, partition, key).await
        }
        async fn set(&self, ns: &str, partition: &str, key: &str, value: &StateValue, ttl_ms: Option<u64>) -> Result<()> {
            if ns == "events" && self.writes.fetch_add(1, Ordering::SeqCst) == self.fail_at {
                bail!("disk full");
            }
            if ns == "events" && matches!(value, StateValue::Str(t) if t.contains("\"slow\"")) {
                self.held.notify_one();
                self.release.notified().await;
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

    #[tokio::test]
    async fn retry_marks_the_same() {
        let store: Arc<dyn StateStore> = probe(1);
        let sink = sink(&store, ALL, None);
        let payload = || write(&[at(Y2020 + MIN, "alice", "Describe", "10.0.0.1"), at(Y2020 + 2 * MIN, "alice", "Describe", "5.6.7.8")]);
        assert!(sink.write(payload()).await.is_err(), "the second record's write fails");
        sink.write(payload()).await.unwrap();
        let first = json!({"actor": ["_norm.kind", "_norm.ip"], "org": ["_norm.actor"]});
        assert_eq!(
            novelty(&store).await,
            [first.clone(), first, json!({"actor": ["_norm.ip"]})],
            "the first twice (stored by both tries), and the second, in flight, still marked"
        );
    }

    #[tokio::test]
    async fn writes_take_turns() {
        let probe = probe(usize::MAX);
        let store: Arc<dyn StateStore> = probe.clone();
        let sink = sink(&store, &[("actor", &["_norm.kind"])], None);
        let mut slow = at(Y2020, "alice", "Describe", "10.0.0.1");
        slow["slow"] = json!(true);
        let first = tokio::spawn({
            let sink = Arc::clone(&sink);
            async move { sink.write(write(&[slow])).await }
        });
        probe.held.notified().await;
        let second = tokio::spawn({
            let sink = Arc::clone(&sink);
            async move { sink.write(write(&[at(Y2020 + MIN, "alice", "Describe", "10.0.0.1")])).await }
        });
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert!(!second.is_finished(), "a write waits for the one before it");
        probe.release.notify_one();
        first.await.unwrap().unwrap();
        second.await.unwrap().unwrap();
        assert_eq!(novelty(&store).await, [json!({"actor": ["_norm.kind"]}), Value::Null]);
    }

    #[test]
    fn novel_needs_recent() {
        let novel = BTreeMap::from([("actor".to_string(), vec!["_norm.kind".to_string()])]);
        let cfg = StoreSinkConfig { retain_secs: 2_592_000, recent: BTreeMap::new(), novel, follow_secs: None };
        assert!(StoreSink::new(&cfg, lru(), None).is_err());
    }
}
