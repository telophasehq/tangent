//! In-process state bounded by an LRU: once `max_entries` are held, the
//! least recently used entry goes to make room; expired entries go when read
//! or purged. Nothing survives the process. For tests, benchmarks, and
//! pipelines whose state may start over on restart.
//!
//! A transaction keeps what each of its writes replaced, and puts it back if
//! it is dropped; an entry evicted to make room for one of its writes stays
//! gone.

use anyhow::{bail, Result};
use async_trait::async_trait;
use lru::LruCache;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::num::NonZeroUsize;
use std::sync::Arc;
use tangent_shared::runtime::StateConfig;
use tokio::sync::OwnedMutexGuard;

use super::{expires_at, now_ms, Count, Entry, StateStore, StateValue, Txn};

type Key = (String, String, String);

#[derive(Clone)]
struct Slot {
    value: StateValue,
    expires_at_ms: Option<u64>,
    updated_at_ms: u64,
}

type Entries = Arc<Mutex<LruCache<Key, Slot>>>;

pub struct LruStore {
    cfg: StateConfig,
    entries: Entries,
    /// Calls take turns with transactions: none runs while one is open.
    turn: Arc<tokio::sync::Mutex<()>>,
}

impl LruStore {
    pub fn new(cfg: StateConfig) -> Result<Self> {
        let Some(cap) = NonZeroUsize::new(cfg.max_entries) else {
            bail!("runtime.state.max_entries must be at least 1 for the lru backend");
        };
        Ok(Self {
            cfg,
            entries: Arc::new(Mutex::new(LruCache::new(cap))),
            turn: Default::default(),
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
    async fn begin(&self) -> Result<Box<dyn Txn>> {
        Ok(Box::new(LruTxn {
            _turn: Arc::clone(&self.turn).lock_owned().await,
            entries: Arc::clone(&self.entries),
            cfg: self.cfg.clone(),
            undo: Vec::new(),
            open: true,
        }))
    }

    async fn scan(
        &self,
        ns: Option<&str>,
        partition: Option<&str>,
        key_prefix: &str,
        limit: usize,
    ) -> Result<Vec<Entry>> {
        let now = now_ms();
        let _turn = self.turn.lock().await;
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

    async fn counts(&self, ns: Option<&str>) -> Result<Vec<Count>> {
        let now = now_ms();
        let _turn = self.turn.lock().await;
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
        let _turn = self.turn.lock().await;
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
        let _turn = self.turn.lock().await;
        self.entries.lock().clear();
        Ok(())
    }
}

/// A transaction on the cache: what each of its writes replaced, put back
/// unless it commits.
struct LruTxn {
    entries: Entries,
    cfg: StateConfig,
    /// Each write's key and the slot it replaced, or none.
    undo: Vec<(Key, Option<Slot>)>,
    /// Not committed yet: dropping it puts `undo` back.
    open: bool,
    _turn: OwnedMutexGuard<()>,
}

impl LruTxn {
    fn put(&mut self, k: Key, slot: Slot) {
        let mut entries = self.entries.lock();
        self.undo.push((k.clone(), entries.peek(&k).cloned()));
        entries.put(k, slot);
    }
}

#[async_trait]
impl Txn for LruTxn {
    async fn get(&mut self, ns: &str, partition: &str, k: &str) -> Result<Option<StateValue>> {
        let now = now_ms();
        let k = key(ns, partition, k);
        let mut entries = self.entries.lock();
        // A read is a use: the entry moves to the front.
        match entries.get(&k) {
            Some(s) if live(s, now) => Ok(Some(s.value.clone())),
            Some(_) => {
                let gone = entries.pop(&k);
                self.undo.push((k, gone));
                Ok(None)
            }
            None => Ok(None),
        }
    }

    async fn set(
        &mut self,
        ns: &str,
        partition: &str,
        k: &str,
        value: &StateValue,
        ttl_ms: Option<u64>,
    ) -> Result<()> {
        let now = now_ms();
        let slot = Slot {
            value: value.clone(),
            expires_at_ms: expires_at(&self.cfg, ttl_ms, now),
            updated_at_ms: now,
        };
        self.put(key(ns, partition, k), slot);
        Ok(())
    }

    async fn del(&mut self, ns: &str, partition: &str, k: &str) -> Result<bool> {
        let now = now_ms();
        let k = key(ns, partition, k);
        let gone = self.entries.lock().pop(&k);
        let live = gone.as_ref().is_some_and(|s| live(s, now));
        self.undo.push((k, gone));
        Ok(live)
    }

    async fn incr(
        &mut self,
        ns: &str,
        partition: &str,
        k: &str,
        by: i64,
        ttl_ms: Option<u64>,
    ) -> Result<i64> {
        let now = now_ms();
        let k = key(ns, partition, k);
        let current = match self.entries.lock().peek(&k) {
            Some(s) if live(s, now) => match &s.value {
                StateValue::Int(i) => *i,
                other => bail!("incr on a non-integer value: {other:?}"),
            },
            _ => 0,
        };
        let next = current.saturating_add(by);
        let slot = Slot {
            value: StateValue::Int(next),
            expires_at_ms: expires_at(&self.cfg, ttl_ms, now),
            updated_at_ms: now,
        };
        self.put(k, slot);
        Ok(next)
    }

    async fn range(
        &mut self,
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

    async fn commit(mut self: Box<Self>) -> Result<()> {
        self.open = false;
        Ok(())
    }
}

impl Drop for LruTxn {
    fn drop(&mut self) {
        if !self.open {
            return;
        }
        let mut entries = self.entries.lock();
        for (k, before) in std::mem::take(&mut self.undo).into_iter().rev() {
            match before {
                Some(slot) => {
                    entries.put(k, slot);
                }
                None => {
                    entries.pop(&k);
                }
            }
        }
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

    #[tokio::test(flavor = "multi_thread")]
    async fn transactions() {
        let store = Arc::new(LruStore::new(StateConfig::default()).unwrap());
        crate::state::conformance::transactions(store).await;
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
