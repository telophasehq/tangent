//! In-process state: a map behind a mutex. Nothing survives the process.

use anyhow::{bail, Result};
use async_trait::async_trait;
use parking_lot::Mutex;
use std::collections::BTreeMap;
use tangent_shared::runtime::StateConfig;

use super::{expires_at, now_ms, Count, Entry, StateStore, StateValue};

struct Slot {
    value: StateValue,
    expires_at_ms: Option<u64>,
    updated_at_ms: u64,
}

pub struct MemoryStore {
    cfg: StateConfig,
    map: Mutex<BTreeMap<(String, String, String), Slot>>,
}

impl MemoryStore {
    pub fn new(cfg: StateConfig) -> Self {
        Self {
            cfg,
            map: Mutex::new(BTreeMap::new()),
        }
    }
}

fn live(slot: &Slot, now: u64) -> bool {
    slot.expires_at_ms.is_none_or(|e| e > now)
}

#[async_trait]
impl StateStore for MemoryStore {
    async fn get(&self, ns: &str, partition: &str, key: &str) -> Result<Option<StateValue>> {
        let now = now_ms();
        let mut map = self.map.lock();
        let k = (ns.to_string(), partition.to_string(), key.to_string());
        match map.get(&k) {
            Some(s) if live(s, now) => Ok(Some(s.value.clone())),
            Some(_) => {
                map.remove(&k);
                Ok(None)
            }
            None => Ok(None),
        }
    }

    async fn set(
        &self,
        ns: &str,
        partition: &str,
        key: &str,
        value: &StateValue,
        ttl_ms: Option<u64>,
    ) -> Result<()> {
        let now = now_ms();
        self.map.lock().insert(
            (ns.to_string(), partition.to_string(), key.to_string()),
            Slot {
                value: value.clone(),
                expires_at_ms: expires_at(&self.cfg, ttl_ms, now),
                updated_at_ms: now,
            },
        );
        Ok(())
    }

    async fn del(&self, ns: &str, partition: &str, key: &str) -> Result<bool> {
        Ok(self
            .map
            .lock()
            .remove(&(ns.to_string(), partition.to_string(), key.to_string()))
            .is_some())
    }

    async fn incr(
        &self,
        ns: &str,
        partition: &str,
        key: &str,
        by: i64,
        ttl_ms: Option<u64>,
    ) -> Result<i64> {
        let now = now_ms();
        let mut map = self.map.lock();
        let k = (ns.to_string(), partition.to_string(), key.to_string());
        let current = match map.get(&k) {
            Some(s) if live(s, now) => match &s.value {
                StateValue::Int(i) => *i,
                other => bail!("incr on a non-integer value: {other:?}"),
            },
            _ => 0,
        };
        let next = current.saturating_add(by);
        map.insert(
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
        Ok(self
            .map
            .lock()
            .iter()
            .filter(|((n, p, k), s)| {
                live(s, now)
                    && ns.is_none_or(|x| x == n)
                    && partition.is_none_or(|x| x == p)
                    && k.starts_with(key_prefix)
            })
            .take(limit)
            .map(|((n, p, k), s)| Entry {
                ns: n.clone(),
                partition: p.clone(),
                key: k.clone(),
                value: s.value.clone(),
                expires_at_ms: s.expires_at_ms,
                updated_at_ms: s.updated_at_ms,
            })
            .collect())
    }

    async fn counts(&self, ns: Option<&str>) -> Result<Vec<Count>> {
        let now = now_ms();
        let mut acc: BTreeMap<(String, String), u64> = BTreeMap::new();
        for ((n, p, _), s) in self.map.lock().iter() {
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
        let mut map = self.map.lock();
        let before = map.len();
        map.retain(|_, s| live(s, now));
        Ok((before - map.len()) as u64)
    }

    async fn reset(&self) -> Result<()> {
        self.map.lock().clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn conforms() {
        let store = MemoryStore::new(StateConfig::default());
        crate::state::conformance::run(&store).await;
    }
}
