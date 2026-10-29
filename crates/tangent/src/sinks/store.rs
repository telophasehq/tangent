//! The `store` sink: every record kept in the state store under its
//! partition, indexed by the scopes an inference node reads history by,
//! marked with what is new for its scopes, and the bookkeeping the `store`
//! source closes windows by.

use std::collections::{BTreeMap, HashSet};
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde_json::{Map, Value};
use tangent_shared::runtime::PartitionConfig;
use tangent_shared::sinks::store::StoreSinkConfig;

use crate::partition::key_of_value;
use crate::sinks::manager::{Sink, SinkWrite};
use crate::sources::poll::duration_ms;
use crate::state::{now_ms, StateStore, StateValue};
use crate::windows::{
    ev_key, get_int, index_ns, index_prefix, scope_value, seen_ns, set_int, time_of_value,
    CLOSED_THROUGH, FIRST, INDEXED, LAST_ARRIVAL, LATE_PREFIX, READ_THROUGH, REMEMBERED, WATERMARK,
    WINDOWS_NS,
};
use crate::{SINK_BYTES_TOTAL, SINK_OBJECTS_TOTAL};

/// Where a record with no partition key goes.
const NO_PARTITION: &str = "_";

/// The field a record new for its scopes is marked with.
pub const NOVEL: &str = "_novel";

pub struct StoreSink {
    store: Arc<dyn StateStore>,
    partition: Option<PartitionConfig>,
    ns: String,
    retain_ms: u64,
    time: String,
    /// (scope, field) each record is indexed by; a scope whose field is a
    /// partition key is not: the partition is its history.
    indexed: Vec<(String, String)>,
    /// (scope, its field, the fields whose values it is remembered to have had).
    novel: Vec<(String, String, Vec<String>)>,
    /// Writes run side by side; a partition's window bookkeeping (read, then
    /// written) and its first reading of older rows happen under this, one
    /// at a time.
    bookkeeping: tokio::sync::Mutex<HashSet<String>>,
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
                Some(field) => Ok((scope.clone(), field.clone(), fields.clone())),
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
            bookkeeping: tokio::sync::Mutex::new(HashSet::new()),
        }))
    }

    /// The index entries of one stored record: one per indexed scope it has a
    /// value for, each holding the record's key.
    async fn index(&self, part: &str, key: &str, v: &Value) -> Result<()> {
        let ns = index_ns(&self.ns);
        for (scope, field) in &self.indexed {
            if let Some(value) = crate::inference::lookup(v, field).and_then(scope_value) {
                let at = format!("{}{key}", index_prefix(scope, &value));
                self.store
                    .set(&ns, part, &at, &StateValue::Str(key.to_string()), Some(self.retain_ms))
                    .await?;
            }
        }
        Ok(())
    }

    /// What of one record its scopes have not had within `retain`, by scope;
    /// each value it has is remembered, from now, for `retain`.
    async fn remember(&self, part: &str, v: &Value) -> Result<Map<String, Value>> {
        let ns = seen_ns(&self.ns);
        let now = now_ms() as i64;
        let mut new = Map::new();
        for (scope, field, fields) in &self.novel {
            let Some(sv) = crate::inference::lookup(v, field).and_then(scope_value) else { continue };
            for f in fields {
                let Some(value) = crate::inference::lookup(v, f).and_then(scope_value) else { continue };
                let at = format!("{}{f}\u{1f}{value}", index_prefix(scope, &sv));
                let last = match self.store.get(&ns, part, &at).await? {
                    Some(StateValue::Int(t)) => Some(t),
                    _ => None,
                };
                if last.is_none() {
                    new.entry(scope.clone()).or_insert_with(|| Value::Array(Vec::new()))
                        .as_array_mut()
                        .expect("an array")
                        .push(Value::String(f.clone()));
                }
                // Kept from its last sighting, rewritten at most twice a retention.
                if last.is_none_or(|t| now - t > self.retain_ms as i64 / 2) {
                    self.store.set(&ns, part, &at, &StateValue::Int(now), Some(self.retain_ms)).await?;
                }
            }
        }
        Ok(new)
    }

    /// A partition's rows stored before this sink indexed or remembered them,
    /// read once: a store written by an older version, or by a sink that
    /// listed fewer scopes, has history and knows what is usual from its
    /// first write on.
    async fn prepare(&self, part: &str) -> Result<()> {
        let index = !self.indexed.is_empty() && get_int(&self.store, part, INDEXED).await?.is_none();
        let remember = !self.novel.is_empty() && get_int(&self.store, part, REMEMBERED).await?.is_none();
        if !index && !remember {
            return Ok(());
        }
        let (mut from, mut n) = ("ev:".to_string(), 0usize);
        loop {
            let page = self.store.range(&self.ns, part, &from, "ev:~", 2_000).await?;
            let Some(last) = page.last() else { break };
            from = format!("{}!", last.key);
            for e in &page {
                if let StateValue::Str(text) = &e.value {
                    if let Ok(v) = serde_json::from_str::<Value>(text) {
                        if index {
                            self.index(part, &e.key, &v).await?;
                        }
                        if remember {
                            self.remember(part, &v).await?;
                        }
                        n += 1;
                    }
                }
            }
        }
        if n > 0 {
            tracing::info!(partition = %part, records = n, index, remember, "store sink: read the records stored before");
        }
        if index {
            set_int(&self.store, part, INDEXED, now_ms() as i64).await?;
        }
        if remember {
            set_int(&self.store, part, REMEMBERED, now_ms() as i64).await?;
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

        // Per partition: (record time, key) of what this write stored.
        let mut stored: BTreeMap<String, Vec<(i64, String)>> = BTreeMap::new();
        for (part, ts, mut v, line) in records {
            let key = ev_key(ts, &ulid::Ulid::new().to_string());
            let new = self.remember(&part, &v).await?;
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
            self.index(&part, &key, &v).await?;
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
        SINK_OBJECTS_TOTAL.inc();
        SINK_BYTES_TOTAL.inc_by(req.payload.len() as u64);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::lru::LruStore;
    use crate::windows::ts_of_key;
    use bytes::BytesMut;
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
        let cfg = StoreSinkConfig { namespace: "events".into(), retain: "30d".into(), time: "ts".into(), recent: BTreeMap::new(), novel: BTreeMap::new() };
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
        let cfg = StoreSinkConfig { namespace: "events".into(), retain: "30d".into(), time: "ts".into(), recent, novel: BTreeMap::new() };
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

    fn sink_with_novelty(store: &Arc<dyn StateStore>) -> Arc<StoreSink> {
        let partition = PartitionConfig { key: vec!["_norm.org".into()], separator: "|".into(), retain_ms: None, time: None };
        let recent = BTreeMap::from([("actor".to_string(), "_norm.actor".to_string()), ("org".to_string(), "_norm.org".to_string())]);
        let novel = BTreeMap::from([
            ("actor".to_string(), vec!["_norm.kind".to_string(), "_norm.ip".to_string()]),
            ("org".to_string(), vec!["_norm.actor".to_string()]),
        ]);
        let cfg = StoreSinkConfig { namespace: "events".into(), retain: "30d".into(), time: "ts".into(), recent, novel };
        StoreSink::new(&cfg, Arc::clone(store), Some(partition)).unwrap()
    }

    fn act(min: u32, actor: &str, kind: &str, ip: &str) -> serde_json::Value {
        serde_json::json!({"ts": format!("2026-10-27T13:{min:02}:00Z"), "_norm": {"org": "acme", "actor": actor, "kind": kind, "ip": ip}})
    }

    async fn novelty(store: &Arc<dyn StateStore>) -> Vec<serde_json::Value> {
        store
            .range("events", "acme", "ev:", "ev:~", 100)
            .await
            .unwrap()
            .into_iter()
            .map(|e| match e.value {
                StateValue::Str(t) => serde_json::from_str::<serde_json::Value>(&t).unwrap()[NOVEL].clone(),
                _ => panic!(),
            })
            .collect()
    }

    /// A record is marked with what its scopes had not had; the same again is not.
    #[tokio::test]
    async fn a_record_is_marked_with_what_is_new_for_its_scopes() {
        let store: Arc<dyn StateStore> = Arc::new(LruStore::new(StateConfig::default()).unwrap());
        let sink = sink_with_novelty(&store);
        sink.write(write(&[act(1, "alice", "Describe", "10.0.0.1")])).await.unwrap();
        sink.write(write(&[act(2, "alice", "Describe", "10.0.0.1"), act(3, "alice", "CreateUser", "10.0.0.1")])).await.unwrap();
        sink.write(write(&[act(4, "alice", "CreateUser", "5.6.7.8"), act(5, "bob", "Describe", "10.0.0.1")])).await.unwrap();
        assert_eq!(
            novelty(&store).await,
            [
                serde_json::json!({"actor": ["_norm.kind", "_norm.ip"], "org": ["_norm.actor"]}),
                serde_json::Value::Null,
                serde_json::json!({"actor": ["_norm.kind"]}),
                serde_json::json!({"actor": ["_norm.ip"]}),
                serde_json::json!({"actor": ["_norm.kind", "_norm.ip"], "org": ["_norm.actor"]}),
            ],
            "alice's first record; the same again; a new action; a new address; bob's first"
        );
    }

    /// What rows stored before novelty was remembered had is not new.
    #[tokio::test]
    async fn what_older_rows_had_is_not_new() {
        let store: Arc<dyn StateStore> = Arc::new(LruStore::new(StateConfig::default()).unwrap());
        let old = crate::windows::ev_key(1_793_105_000_000, "01OLD");
        store
            .set("events", "acme", &old, &StateValue::Str(act(0, "alice", "Describe", "10.0.0.1").to_string()), None)
            .await
            .unwrap();
        let sink = sink_with_novelty(&store);
        sink.write(write(&[act(5, "alice", "Describe", "10.0.0.1")])).await.unwrap();
        assert_eq!(novelty(&store).await[1], serde_json::Value::Null);
        assert!(get_int(&store, "acme", REMEMBERED).await.unwrap().is_some());
    }

    #[test]
    fn a_novel_scope_needs_a_field() {
        let store: Arc<dyn StateStore> = Arc::new(LruStore::new(StateConfig::default()).unwrap());
        let novel = BTreeMap::from([("actor".to_string(), vec!["_norm.kind".to_string()])]);
        let cfg = StoreSinkConfig { namespace: "events".into(), retain: "30d".into(), time: "ts".into(), recent: BTreeMap::new(), novel };
        assert!(StoreSink::new(&cfg, store, None).is_err());
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
        assert!(get_int(&store, "acme", INDEXED).await.unwrap().is_some());
    }
}
