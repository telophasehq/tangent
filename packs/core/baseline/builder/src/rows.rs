//! A person's sign-ins and credential changes, a row each keyed by its time,
//! filed under the person who acted and each person it was done to, and
//! naming them. Failed sign-ins are counted instead (counts.rs).

use std::collections::{BTreeMap, BTreeSet};

use baselinekit::keys::{key, t13};
use baselinekit::kinds::Ev;
use baselinekit::{Kv, Scalar, R};
use identitykit::People;
use serde_json::json;

pub fn write(kv: &mut impl Kv, people: &People, evs: &[&Ev]) -> R<()> {
    let mut rows: BTreeMap<String, String> = BTreeMap::new();
    for e in evs {
        let (o, a, t) = (e.org.as_str(), e.actor.as_str(), e.ms);
        let Some(x) = e.auth.as_ref().filter(|x| x.kind != "login failed") else {
            continue;
        };
        let to: BTreeSet<String> = x.subjects.iter().map(|s| people.person(s)).collect();
        let person = e.named.then_some(&e.person); // `by`'s
        let row = json!({"k": x.kind, "what": x.what, "by": a, "person": person, "to": to,
                         "src": e.src, "ip": e.ip, "asn": e.asn, "cc": e.cc, "mfa": x.mfa,
                         "okta": e.okta_says})
        .to_string();
        for p in to.iter().chain([&e.person]) {
            rows.insert(key(&[o, "p", p, "e", &t13(t), &e.h]), row.clone());
        }
    }
    for (k, v) in rows {
        kv.set(&k, Scalar::Str(v))?;
    }
    Ok(())
}
