use anyhow::Result;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;
use tangent_shared::sinks::store::StoreConfig;

use crate::partition;
use crate::state::{now_ms, StateStore, StateValue, PLUGIN_NS};
use crate::wasm::host::tangent::logs::log::Scalar;
use crate::wasm::host::JsonLogView;

static SEQ: AtomicU32 = AtomicU32::new(0);

/// Each record into every partition's history, as ev:<ms>:<seq>.
pub struct StoreSink {
    store: Arc<dyn StateStore>,
    partitions: BTreeMap<String, Vec<String>>,
    cfg: StoreConfig,
}

impl StoreSink {
    pub fn new(
        store: Arc<dyn StateStore>,
        partitions: BTreeMap<String, Vec<String>>,
        cfg: StoreConfig,
    ) -> Self {
        Self {
            store,
            partitions,
            cfg,
        }
    }

    pub async fn write(&self, views: &[JsonLogView]) -> Result<()> {
        let ttl = Some(self.cfg.retain_ms);
        for lv in views {
            let ts = time_of(lv, self.cfg.time.as_deref()).unwrap_or_else(|| now_ms() as i64);
            let seq = SEQ.fetch_add(1, Ordering::Relaxed) % 1_000_000;
            let key = format!("ev:{ts:013}:{seq:06}");
            let value = StateValue::Str(lv.to_json());
            if self.partitions.is_empty() {
                self.store.set(PLUGIN_NS, "", &key, &value, ttl).await?;
            }
            for (name, fields) in &self.partitions {
                let scope = partition::scope(lv, name, fields);
                self.store.set(PLUGIN_NS, &scope, &key, &value, ttl).await?;
            }
        }
        Ok(())
    }
}

/// A record's time as epoch ms from `field`: RFC 3339, or epoch seconds or
/// milliseconds.
fn time_of(view: &JsonLogView, field: Option<&str>) -> Option<i64> {
    match view.lookup(field?).and_then(JsonLogView::to_scalar)? {
        Scalar::Str(s) => chrono::DateTime::parse_from_rfc3339(s.trim())
            .ok()
            .map(|t| t.timestamp_millis()),
        Scalar::Int(i) => Some(if i.abs() > 100_000_000_000 {
            i
        } else {
            i * 1000
        }),
        Scalar::Float(f) => Some(if f.abs() > 1e11 {
            f as i64
        } else {
            (f * 1000.0) as i64
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::lru::LruStore;
    use crate::wasm::host::tangent::logs::state::Host;
    use crate::wasm::host::HostEngine;
    use bytes::BytesMut;
    use tangent_shared::runtime::StateConfig;
    use wasmtime_wasi::WasiCtxBuilder;

    #[tokio::test]
    async fn writes_every_partition() {
        let store: Arc<dyn StateStore> = Arc::new(LruStore::new(StateConfig::default()).unwrap());
        let partitions = BTreeMap::from([
            ("org".to_string(), vec!["org_id".to_string()]),
            ("user".to_string(), vec!["user.id".to_string()]),
        ]);
        let sink = StoreSink::new(
            Arc::clone(&store),
            partitions,
            StoreConfig {
                time: Some("ts".into()),
                retain_ms: 60_000,
            },
        );
        let rec = r#"{"ts":"2026-10-20T13:00:00Z","org_id":"acme","user":{"id":"u1"}}"#;
        sink.write(&[JsonLogView::from_bytes(BytesMut::from(rec)).unwrap()])
            .await
            .unwrap();

        let mut host = HostEngine::new(
            WasiCtxBuilder::new().build(),
            store,
            Arc::from(PLUGIN_NS),
            Arc::default(),
            true,
        );
        for scope in ["org:acme", "user:u1"] {
            host.partition = Arc::from(scope);
            let rows = host.scan("ev:".into(), "ev:~".into(), 10).await.unwrap();
            assert_eq!(rows.len(), 1, "{scope}");
            assert!(rows[0].0.starts_with("ev:1792501200000:"));
            assert!(matches!(&rows[0].1, Scalar::Str(s) if s.contains("acme")));
        }
    }
}
