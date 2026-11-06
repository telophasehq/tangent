//! Counts, each a sum: an actor's events a day, and per five minutes of each
//! hour by category; a person's failed sign-ins a minute.

use std::collections::BTreeMap;

use baselinekit::keys::{blob, key, t13};
use baselinekit::kinds::Ev;
use baselinekit::time::{D, H, M, SLOT};
use baselinekit::{Kv, Scalar, R};
use serde_json::json;

pub fn write(kv: &mut impl Kv, evs: &[&Ev]) -> R<()> {
    let mut sums: BTreeMap<String, i64> = BTreeMap::new();
    let mut hours: BTreeMap<String, Vec<(i64, char)>> = BTreeMap::new();
    for e in evs {
        let (o, a, t) = (e.org.as_str(), e.actor.as_str(), e.ms);
        *sums
            .entry(key(&[o, "a", a, "d", &format!("{:05}", t.div_euclid(D))]))
            .or_default() += 1;
        hours
            .entry(key(&[o, "a", a, "h", &format!("{:07}", t.div_euclid(H))]))
            .or_default()
            .push((t.rem_euclid(H) / SLOT, e.cat));
        // a count a minute: a spray is one row a minute, not one an attempt
        if e.auth.as_ref().is_some_and(|x| x.kind == "login failed") {
            *sums
                .entry(key(&[o, "p", &e.person, "f", &t13(t - t.rem_euclid(M))]))
                .or_default() += 1;
        }
    }
    for (k, n) in sums {
        kv.incr(&k, n)?;
    }
    for (k, add) in hours {
        let mut blob = blob(kv.get(&k)?);
        for (slot, cat) in add {
            *blob
                .entry(slot.to_string())
                .or_default()
                .entry(cat.to_string())
                .or_default() += 1;
        }
        kv.set(&k, Scalar::Str(json!(blob).to_string()))?;
    }
    Ok(())
}
