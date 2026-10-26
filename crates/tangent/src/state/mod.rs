//! Keyed state shared by everything in a pipeline: plugins (through the WIT
//! `state` interface) and the runtime's own stateful nodes. Entries live in
//! a namespace (`plugins` for plugin state, a node's name otherwise), within
//! a partition of the stream (empty when unpartitioned), under a key.
//!
//! The store is an async trait so backends can be local or networked:
//! `sqlite` is durable on one host and queryable from outside the process
//! (`tangent state ...`), `lru` is in-process and bounded by `max_entries`, and a
//! distributed backend (DynamoDB: `ns#partition` as the hash key, `key` as
//! the range key, `incr` as an atomic ADD, TTL as the item TTL) fits the same
//! interface.

use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tangent_shared::runtime::{StateBackend, StateConfig};

pub mod lru;
pub mod sqlite;

/// Namespace of plugin state (the WIT `state` interface, formerly `cache`).
pub const PLUGIN_NS: &str = "plugins";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "lowercase")]
pub enum StateValue {
    Str(String),
    Int(i64),
    Float(f64),
    Bool(bool),
    Bytes(Vec<u8>),
    Json(serde_json::Value),
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Entry {
    pub ns: String,
    pub partition: String,
    pub key: String,
    pub value: StateValue,
    /// None: never expires.
    pub expires_at_ms: Option<u64>,
    pub updated_at_ms: u64,
}

/// How many keys a (namespace, partition) holds.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Count {
    pub ns: String,
    pub partition: String,
    pub keys: u64,
}

#[async_trait]
pub trait StateStore: Send + Sync {
    async fn get(&self, ns: &str, partition: &str, key: &str) -> Result<Option<StateValue>>;

    /// `ttl_ms`: None takes the configured default; Some(0) never expires.
    async fn set(
        &self,
        ns: &str,
        partition: &str,
        key: &str,
        value: &StateValue,
        ttl_ms: Option<u64>,
    ) -> Result<()>;

    async fn del(&self, ns: &str, partition: &str, key: &str) -> Result<bool>;

    /// Add `by` to an integer entry (missing counts as 0); returns the new value.
    async fn incr(
        &self,
        ns: &str,
        partition: &str,
        key: &str,
        by: i64,
        ttl_ms: Option<u64>,
    ) -> Result<i64>;

    /// Live entries, filtered by namespace, partition and key prefix when given.
    async fn scan(
        &self,
        ns: Option<&str>,
        partition: Option<&str>,
        key_prefix: &str,
        limit: usize,
    ) -> Result<Vec<Entry>>;

    /// Key counts per (namespace, partition), filtered by namespace when given.
    async fn counts(&self, ns: Option<&str>) -> Result<Vec<Count>>;

    /// Drop expired entries; returns how many.
    async fn purge_expired(&self) -> Result<u64>;

    /// Drop everything.
    async fn reset(&self) -> Result<()>;
}

/// Resolve a write's TTL against the config: None -> default, 0 -> never,
/// anything else capped by `max_ttl_ms` (when that is not 0).
pub fn expires_at(cfg: &StateConfig, ttl_ms: Option<u64>, now_ms: u64) -> Option<u64> {
    let mut ttl = ttl_ms.unwrap_or(cfg.default_ttl_ms);
    if ttl == 0 {
        return None;
    }
    if cfg.max_ttl_ms > 0 {
        ttl = ttl.min(cfg.max_ttl_ms);
    }
    Some(now_ms.saturating_add(ttl))
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub fn open(cfg: &StateConfig, base_dir: &Path) -> Result<Arc<dyn StateStore>> {
    if cfg.max_ttl_ms > 0 && cfg.default_ttl_ms > cfg.max_ttl_ms {
        anyhow::bail!(
            "runtime.state.default_ttl_ms ({}) is above max_ttl_ms ({}); raise max_ttl_ms or set it to 0 for no cap",
            cfg.default_ttl_ms,
            cfg.max_ttl_ms
        );
    }
    Ok(match cfg.backend {
        StateBackend::Sqlite => Arc::new(sqlite::SqliteStore::open(cfg, base_dir)?),
        StateBackend::Lru => Arc::new(lru::LruStore::new(cfg.clone())?),
    })
}

/// A read-only handle on a store from outside the pipeline's process: nothing
/// is created or locked. Which backends can be read this way is the
/// backend's business (`lru` has no outside; a networked store would
/// return a client).
pub fn open_reader(cfg: &StateConfig, base_dir: &Path) -> Result<Arc<dyn StateStore>> {
    match cfg.backend {
        StateBackend::Sqlite => {
            let path = sqlite_path(cfg, base_dir);
            if !path.exists() {
                anyhow::bail!("no state at {} (has the pipeline run yet?)", path.display());
            }
            Ok(Arc::new(sqlite::SqliteStore::open_readonly(&path)?))
        }
        StateBackend::Lru => anyhow::bail!(
            "the lru backend lives inside the pipeline's process; there is nothing to read from outside"
        ),
    }
}

/// The SQLite file a config points at, resolved like `open` does.
pub fn sqlite_path(cfg: &StateConfig, base_dir: &Path) -> std::path::PathBuf {
    if cfg.path.is_absolute() {
        cfg.path.clone()
    } else {
        base_dir.join(&cfg.path)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_default_ttl_above_the_cap_is_refused() {
        let dir = std::env::temp_dir();
        let bad = StateConfig {
            backend: StateBackend::Lru,
            default_ttl_ms: 86_400_000,
            max_ttl_ms: 3_600_000,
            ..StateConfig::default()
        };
        let err = open(&bad, &dir).err().expect("refused").to_string();
        assert!(err.contains("max_ttl_ms"), "{err}");

        let uncapped = StateConfig {
            max_ttl_ms: 0,
            ..bad
        };
        assert!(open(&uncapped, &dir).is_ok());
    }

    #[test]
    fn ttl_resolution() {
        let cfg = StateConfig {
            default_ttl_ms: 1_000,
            max_ttl_ms: 5_000,
            ..StateConfig::default()
        };
        assert_eq!(expires_at(&cfg, None, 100), Some(1_100), "default");
        assert_eq!(expires_at(&cfg, Some(0), 100), None, "0 never expires");
        assert_eq!(expires_at(&cfg, Some(9_000), 100), Some(5_100), "capped");
        let uncapped = StateConfig {
            max_ttl_ms: 0,
            ..cfg
        };
        assert_eq!(expires_at(&uncapped, Some(9_000), 100), Some(9_100));
    }
}

#[cfg(test)]
pub(crate) mod conformance {
    //! The same behaviour for every backend.
    use super::*;

    pub async fn run(store: &dyn StateStore) {
        let v = StateValue::Str("hello".into());
        store.set("ns", "", "k", &v, None).await.unwrap();
        assert_eq!(store.get("ns", "", "k").await.unwrap(), Some(v.clone()));
        assert_eq!(store.get("ns", "", "missing").await.unwrap(), None);
        assert_eq!(
            store.get("other", "", "k").await.unwrap(),
            None,
            "namespaces are separate"
        );
        assert_eq!(
            store.get("ns", "p1", "k").await.unwrap(),
            None,
            "partitions are separate"
        );

        store
            .set("ns", "p1", "k", &StateValue::Int(1), None)
            .await
            .unwrap();
        assert_eq!(
            store.get("ns", "p1", "k").await.unwrap(),
            Some(StateValue::Int(1))
        );
        assert_eq!(store.get("ns", "", "k").await.unwrap(), Some(v.clone()));

        assert_eq!(store.incr("ns", "p1", "n", 5, None).await.unwrap(), 5);
        assert_eq!(store.incr("ns", "p1", "n", -2, None).await.unwrap(), 3);
        assert_eq!(
            store.get("ns", "p1", "n").await.unwrap(),
            Some(StateValue::Int(3))
        );
        assert!(
            store.incr("ns", "", "k", 1, None).await.is_err(),
            "incr on a string fails"
        );

        let json = StateValue::Json(serde_json::json!({"a": [1, 2], "b": "c"}));
        store.set("ns", "", "j", &json, Some(0)).await.unwrap();
        assert_eq!(store.get("ns", "", "j").await.unwrap(), Some(json));
        store
            .set("ns", "", "bytes", &StateValue::Bytes(vec![0, 255, 7]), None)
            .await
            .unwrap();
        assert_eq!(
            store.get("ns", "", "bytes").await.unwrap(),
            Some(StateValue::Bytes(vec![0, 255, 7]))
        );
        store
            .set("ns", "", "f", &StateValue::Float(1.5), None)
            .await
            .unwrap();
        store
            .set("ns", "", "t", &StateValue::Bool(true), None)
            .await
            .unwrap();

        // Expiry: a 1ms TTL is gone by the time we look.
        store
            .set("ns", "", "short", &StateValue::Int(9), Some(1))
            .await
            .unwrap();
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        assert_eq!(store.get("ns", "", "short").await.unwrap(), None);
        assert!(store.purge_expired().await.unwrap() <= 1);

        let all = store.scan(None, None, "", 100).await.unwrap();
        assert_eq!(all.len(), 7, "{all:?}");
        let p1 = store.scan(Some("ns"), Some("p1"), "", 100).await.unwrap();
        assert_eq!(p1.len(), 2);
        let ks = store.scan(Some("ns"), Some(""), "k", 100).await.unwrap();
        assert_eq!(ks.len(), 1);
        assert_eq!(ks[0].key, "k");
        assert_eq!(
            store.scan(None, None, "", 3).await.unwrap().len(),
            3,
            "limit"
        );
        let never = store.scan(Some("ns"), Some(""), "j", 10).await.unwrap();
        assert_eq!(never[0].expires_at_ms, None);

        let mut counts = store.counts(None).await.unwrap();
        counts.sort_by(|a, b| (&a.ns, &a.partition).cmp(&(&b.ns, &b.partition)));
        assert_eq!(counts.len(), 2);
        assert_eq!((counts[0].partition.as_str(), counts[0].keys), ("", 5));
        assert_eq!((counts[1].partition.as_str(), counts[1].keys), ("p1", 2));

        assert!(store.del("ns", "p1", "n").await.unwrap());
        assert!(!store.del("ns", "p1", "n").await.unwrap());
        store.reset().await.unwrap();
        assert!(store.scan(None, None, "", 10).await.unwrap().is_empty());
    }
}
