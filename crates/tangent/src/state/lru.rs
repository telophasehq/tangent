//! In-process state bounded by an LRU: once `max_entries` are held, the
//! least recently used entry goes to make room; expired entries go when read
//! or purged. Nothing survives the process. For tests, benchmarks, and
//! pipelines whose state may start over on restart.

use anyhow::{bail, Result};
use async_trait::async_trait;
use lru::LruCache;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use tangent_shared::runtime::StateConfig;

use super::{expires_at, now_ms, Count, Entry, StateStore, StateValue};

type Key = (String, String, String);

struct Slot {
    value: StateValue,
    expires_at_ms: Option<u64>,
    updated_at_ms: u64,
}

pub struct LruStore {
    cfg: StateConfig,
    entries: Mutex<LruCache<Key, Slot>>,
}

impl LruStore {
    pub fn new(cfg: StateConfig) -> Result<Self> {
        let Some(cap) = NonZeroUsize::new(cfg.max_entries) else {
            bail!("runtime.state.max_entries must be at least 1 for the lru backend");
        };
        Ok(Self {
            cfg,
            entries: Mutex::new(LruCache::new(cap)),
        })
    }
}

fn key(ns: &str, partition: &str, key: &str) -> Key {
    (ns.to_string(), partition.to_string(), key.to_string())
}

fn live(slot: &Slot, now: u64) -> bool {
    slot.expires_at_ms.is_none_or(|e| e > now)
}

#[async_trait]
impl StateStore for LruStore {
    async fn get(&self, ns: &str, partition: &str, k: &str) -> Result<Option<StateValue>> {
        let now = now_ms();
        let mut entries = self.entries.lock();
        let k = key(ns, partition, k);
        // A read is a use: the entry moves to the front.
        match entries.get(&k) {
            Some(s) if live(s, now) => Ok(Some(s.value.clone())),
            Some(_) => {
                entries.pop(&k);
                Ok(None)
            }
            None => Ok(None),
        }
    }

    async fn set(
        &self,
        ns: &str,
        partition: &str,
        k: &str,
        value: &StateValue,
        ttl_ms: Option<u64>,
    ) -> Result<()> {
        let now = now_ms();
        self.entries.lock().put(
            key(ns, partition, k),
            Slot {
                value: value.clone(),
                expires_at_ms: expires_at(&self.cfg, ttl_ms, now),
                updated_at_ms: now,
            },
        );
        Ok(())
    }

    async fn del(&self, ns: &str, partition: &str, k: &str) -> Result<bool> {
        let now = now_ms();
        Ok(self
            .entries
            .lock()
            .pop(&key(ns, partition, k))
            .is_some_and(|s| live(&s, now)))
    }

    async fn incr(
        &self,
        ns: &str,
        partition: &str,
        k: &str,
        by: i64,
        ttl_ms: Option<u64>,
    ) -> Result<i64> {
        let now = now_ms();
        let mut entries = self.entries.lock();
        let k = key(ns, partition, k);
        let current = match entries.peek(&k) {
            Some(s) if live(s, now) => match &s.value {
                StateValue::Int(i) => *i,
                other => bail!("incr on a non-integer value: {other:?}"),
            },
            _ => 0,
        };
        let next = current.saturating_add(by);
        entries.put(
            k,
            Slot {
                value: StateValue::Int(next),
                expires_at_ms: expires_at(&self.cfg, ttl_ms, now),
                updated_at_ms: now,
            },
        );
        Ok(next)
    }

    async fn scan(
        &self,
        ns: Option<&str>,
        partition: Option<&str>,
        key_prefix: &str,
        limit: usize,
    ) -> Result<Vec<Entry>> {
        let now = now_ms();
        // Looking is not using: iteration leaves the recency order alone.
        let mut out: Vec<Entry> = self
            .entries
            .lock()
            .iter()
            .filter(|((n, p, k), s)| {
                live(s, now)
                    && ns.is_none_or(|x| x == n)
                    && partition.is_none_or(|x| x == p)
                    && k.starts_with(key_prefix)
            })
            .map(|((n, p, k), s)| Entry {
                ns: n.clone(),
                partition: p.clone(),
                key: k.clone(),
                value: s.value.clone(),
                expires_at_ms: s.expires_at_ms,
                updated_at_ms: s.updated_at_ms,
            })
            .collect();
        out.sort_by(|a, b| (&a.ns, &a.partition, &a.key).cmp(&(&b.ns, &b.partition, &b.key)));
        out.truncate(limit);
        Ok(out)
    }

    async fn range(
        &self,
        ns: &str,
        partition: &str,
        from: &str,
        to: &str,
        limit: usize,
    ) -> Result<Vec<Entry>> {
        let now = now_ms();
        let mut out: Vec<Entry> = self
            .entries
            .lock()
            .iter()
            .filter(|((n, p, k), s)| {
                live(s, now) && n == ns && p == partition && k.as_str() >= from && k.as_str() < to
            })
            .map(|((n, p, k), s)| Entry {
                ns: n.clone(),
                partition: p.clone(),
                key: k.clone(),
                value: s.value.clone(),
                expires_at_ms: s.expires_at_ms,
                updated_at_ms: s.updated_at_ms,
            })
            .collect();
        out.sort_by(|a, b| a.key.cmp(&b.key));
        out.truncate(limit);
        Ok(out)
    }

    async fn counts(&self, ns: Option<&str>) -> Result<Vec<Count>> {
        let now = now_ms();
        let mut acc: BTreeMap<(String, String), u64> = BTreeMap::new();
        for ((n, p, _), s) in self.entries.lock().iter() {
            if live(s, now) && ns.is_none_or(|x| x == n) {
                *acc.entry((n.clone(), p.clone())).or_default() += 1;
            }
        }
        Ok(acc
            .into_iter()
            .map(|((ns, partition), keys)| Count {
                ns,
                partition,
                keys,
            })
            .collect())
    }

    async fn purge_expired(&self) -> Result<u64> {
        let now = now_ms();
        let mut entries = self.entries.lock();
        let expired: Vec<Key> = entries
            .iter()
            .filter(|(_, s)| !live(s, now))
            .map(|(k, _)| k.clone())
            .collect();
        for k in &expired {
            entries.pop(k);
        }
        Ok(expired.len() as u64)
    }

    async fn reset(&self) -> Result<()> {
        self.entries.lock().clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn conforms() {
        let store = LruStore::new(StateConfig::default()).unwrap();
        crate::state::conformance::run(&store).await;
    }

    #[tokio::test]
    async fn the_least_recently_used_entry_goes_first() {
        let store = LruStore::new(StateConfig {
            max_entries: 2,
            ..StateConfig::default()
        })
        .unwrap();
        let one = StateValue::Int(1);
        store.set("ns", "", "a", &one, None).await.unwrap();
        store
            .set("ns", "", "b", &StateValue::Int(2), None)
            .await
            .unwrap();
        // Reading `a` leaves `b` as the least recently used.
        assert_eq!(store.get("ns", "", "a").await.unwrap(), Some(one.clone()));
        store
            .set("ns", "", "c", &StateValue::Int(3), None)
            .await
            .unwrap();
        assert_eq!(store.get("ns", "", "b").await.unwrap(), None, "evicted");
        assert_eq!(store.get("ns", "", "a").await.unwrap(), Some(one));
        assert_eq!(
            store.get("ns", "", "c").await.unwrap(),
            Some(StateValue::Int(3))
        );
        assert_eq!(store.counts(None).await.unwrap()[0].keys, 2);

        assert!(LruStore::new(StateConfig {
            max_entries: 0,
            ..StateConfig::default()
        })
        .is_err());
    }
}
