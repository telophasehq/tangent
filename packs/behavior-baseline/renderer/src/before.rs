//! The reader every key reads state through, cut at the window's start.

use std::collections::BTreeMap;

use baselinekit::keys::{assumes, blob, int, key, string, t13, unesc, Assume};
use baselinekit::time::{D, H, M, SLOT};
use baselinekit::{Kv, R};
use serde_json::Value;

pub const CHAIN: i64 = 12 * H; // the longest STS session

/// State as of the window's start: everything it hands out is from before `ws`.
pub struct Before<'a, K> {
    pub kv: &'a mut K,
    pub org: String,
    pub ws: i64,
}

impl<K: Kv> Before<'_, K> {
    fn key(&self, parts: &[&str]) -> String {
        let mut all = vec![self.org.as_str()];
        all.extend(parts);
        key(&all)
    }

    pub fn first(&mut self, parts: &[&str]) -> R<Option<i64>> {
        let k = self.key(parts);
        Ok(self
            .kv
            .get(&k)?
            .as_ref()
            .and_then(int)
            .filter(|t| *t < self.ws))
    }

    /// The first-seen times under a prefix, by what follows it, earliest first.
    pub fn firsts(&mut self, parts: &[&str]) -> R<Vec<(String, i64)>> {
        let pre = self.key(parts) + "|";
        let hi = format!("{}}}", self.key(parts)); // `}` sorts right after `|`
        let mut out: Vec<(String, i64)> = (self.kv.scan(&pre, &hi)?)
            .into_iter()
            .filter_map(|(k, v)| Some((unesc(k.strip_prefix(&pre)?), int(&v)?)))
            .filter(|(_, t)| *t < self.ws)
            .collect();
        out.sort_by(|x, y| (x.1, &x.0).cmp(&(y.1, &y.0)));
        Ok(out)
    }

    /// Rows keyed by time in [lo, ws).
    pub fn events(&mut self, parts: &[&str], lo: i64) -> R<Vec<(i64, Value)>> {
        let pre = self.key(parts) + "|";
        if lo >= self.ws {
            return Ok(Vec::new());
        }
        let rows = self.kv.scan(
            &format!("{pre}{}", t13(lo)),
            &format!("{pre}{}", t13(self.ws)),
        )?;
        Ok(rows
            .into_iter()
            .filter_map(|(k, v)| {
                let t = k.strip_prefix(&pre)?.get(..13)?.parse::<i64>().ok()?;
                let v = serde_json::from_str(&string(v)?).ok()?;
                (t < self.ws).then_some((t, v))
            })
            .collect())
    }

    /// Events per day, for the days before the window's.
    pub fn days(&mut self, a: &str) -> R<BTreeMap<i64, i64>> {
        let pre = self.key(&["a", a, "d"]) + "|";
        let rows = self
            .kv
            .scan(&pre, &format!("{pre}{:05}", self.ws.div_euclid(D)))?;
        Ok(rows
            .into_iter()
            .filter_map(|(k, v)| Some((k.strip_prefix(&pre)?.parse().ok()?, int(&v)?)))
            .collect())
    }

    /// Five-minute slots wholly in [lo, ws): the one holding ws is left out whole.
    pub fn slots(&mut self, a: &str, lo: i64) -> R<BTreeMap<i64, BTreeMap<String, i64>>> {
        let pre = self.key(&["a", a, "h"]) + "|";
        let (from, to) = (lo.div_euclid(H), self.ws.div_euclid(H) + 1);
        let rows = self
            .kv
            .scan(&format!("{pre}{from:07}"), &format!("{pre}{to:07}"))?;
        let mut out = BTreeMap::new();
        for (k, v) in rows {
            let Some(h) = k.strip_prefix(&pre).and_then(|x| x.parse::<i64>().ok()) else {
                continue;
            };
            for (sl, cats) in blob(Some(v)) {
                let Some(t) = sl
                    .parse::<i64>()
                    .ok()
                    .filter(|x| (0..H / SLOT).contains(x))
                    .map(|x| h * H + x * SLOT)
                else {
                    continue;
                };
                if lo <= t && t + SLOT <= self.ws {
                    out.insert(t, cats);
                }
            }
        }
        Ok(out)
    }

    /// Failed sign-ins in the minutes wholly in [lo, ws).
    pub fn failed(&mut self, person: &str, lo: i64) -> R<i64> {
        let pre = self.key(&["p", person, "f"]) + "|";
        let rows = self.kv.scan(
            &format!("{pre}{}", t13(lo)),
            &format!("{pre}{}", t13(self.ws - M + 1)),
        )?;
        Ok(rows.iter().filter_map(|(_, v)| int(v)).sum())
    }

    /// The latest assumption of a role session in [before - 12h, before).
    pub fn assumed(
        &mut self,
        acct: &str,
        role: &str,
        sess: &str,
        before: i64,
    ) -> R<Option<Assume>> {
        let pre = self.key(&["s", acct, role, sess]) + "|";
        let (lo, cut) = (before - CHAIN, before.min(self.ws));
        let rows = self.kv.scan(
            &format!("{pre}{:07}", lo.div_euclid(H)),
            &format!("{pre}{:07}", cut.div_euclid(H) + 1),
        )?;
        Ok(rows
            .into_iter()
            .flat_map(|(_, v)| assumes(Some(v)))
            .filter(|x| x.t < cut && x.t >= lo)
            .max_by(|x, y| (x.t, &x.by).cmp(&(y.t, &y.by))))
    }
}
