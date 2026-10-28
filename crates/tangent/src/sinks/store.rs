//! The `store` sink: every record kept in the state store under its
//! partition, and the bookkeeping the `store` source closes windows by.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde_json::Value;
use tangent_shared::runtime::PartitionConfig;
use tangent_shared::sinks::store::StoreSinkConfig;

use crate::partition::key_of_value;
use crate::sinks::manager::{Sink, SinkWrite};
use crate::sources::poll::duration_ms;
use crate::state::{now_ms, StateStore, StateValue};
use crate::windows::{
    ev_key, get_int, set_int, time_of_value, CLOSED_THROUGH, FIRST, LAST_ARRIVAL, LATE_PREFIX,
    READ_THROUGH, WATERMARK, WINDOWS_NS,
};
use crate::{SINK_BYTES_TOTAL, SINK_OBJECTS_TOTAL};

/// Where a record with no partition key goes.
const NO_PARTITION: &str = "_";

pub struct StoreSink {
    store: Arc<dyn StateStore>,
    partition: Option<PartitionConfig>,
    ns: String,
    retain_ms: u64,
    time: String,
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
        Ok(Arc::new(Self {
            store,
            partition,
            ns: cfg.namespace.clone(),
            retain_ms,
            time: cfg.time.clone(),
        }))
    }
}

#[async_trait]
impl Sink for StoreSink {
    async fn write(&self, req: SinkWrite) -> Result<()> {
        let now = now_ms() as i64;
        // Per partition: (record time, key) of what this write stored.
        let mut stored: BTreeMap<String, Vec<(i64, String)>> = BTreeMap::new();
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
            let key = ev_key(ts, &ulid::Ulid::new().to_string());
            let text = String::from_utf8_lossy(line).into_owned();
            self.store
                .set(&self.ns, &part, &key, &StateValue::Str(text), Some(self.retain_ms))
                .await?;
            stored.entry(part).or_default().push((ts, key));
        }

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
        let cfg = StoreSinkConfig { namespace: "events".into(), retain: "30d".into(), time: "ts".into() };
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
}
