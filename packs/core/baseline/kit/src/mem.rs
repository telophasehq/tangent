use std::collections::BTreeMap;
use std::ops::Bound;

use crate::keys::int;
use crate::{Kv, Scalar, R};

/// A store in memory: the tests' state.
#[derive(Default)]
pub struct Mem(pub BTreeMap<String, Scalar>);

impl Kv for Mem {
    fn get(&mut self, key: &str) -> R<Option<Scalar>> {
        Ok(self.0.get(key).cloned())
    }
    fn set(&mut self, key: &str, v: Scalar) -> R<()> {
        self.0.insert(key.into(), v);
        Ok(())
    }
    fn incr(&mut self, key: &str, by: i64) -> R<()> {
        let n = self.0.get(key).and_then(int).unwrap_or(0);
        self.0.insert(key.into(), Scalar::Int(n + by));
        Ok(())
    }
    fn scan(&mut self, lo: &str, hi: &str) -> R<Vec<(String, Scalar)>> {
        let r = self
            .0
            .range::<str, _>((Bound::Included(lo), Bound::Excluded(hi)));
        Ok(r.map(|(k, v)| (k.clone(), v.clone())).collect())
    }
}
