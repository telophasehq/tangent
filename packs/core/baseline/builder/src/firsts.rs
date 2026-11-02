//! First-seen times, each a minimum: the actor; what it used, from which
//! source, where and as what; the networks its person and the org used; which
//! actors used an address; its sessions' MFA; who assumed a role. Only what
//! succeeded is learned as seen: anyone can fail a sign-in in someone's name,
//! from anywhere.

use std::collections::BTreeMap;

use baselinekit::keys::{int, key, nets, values};
use baselinekit::kinds::Ev;
use baselinekit::{Kv, Scalar, R};

pub fn write(kv: &mut impl Kv, evs: &[&Ev]) -> R<()> {
    let mut mins: BTreeMap<String, i64> = BTreeMap::new();
    let mut low = |k: String, t: i64| {
        let v = mins.entry(k).or_insert(t);
        *v = (*v).min(t);
    };
    for e in evs {
        let (o, a, t) = (e.org.as_str(), e.actor.as_str(), e.ms);
        low(key(&[o, "a", a, "first"]), t);
        if e.ok {
            for (dim, v) in values(e) {
                low(key(&[o, "a", a, "n", dim, &v]), t);
            }
            low(key(&[o, "a", a, "n", "cat", &e.cat.to_string()]), t);
            for (dim, v) in nets(e) {
                low(key(&[o, "p", &e.person, "n", dim, &v]), t);
                low(key(&[o, "o", "n", dim, &v]), t);
            }
            if let Some(ip) = &e.ip {
                low(key(&[o, "o", "ip", ip, a]), t);
            }
        }
        if let Some(m) = e.mfa {
            low(
                key(&[o, "a", a, "n", "mfa", if m { "yes" } else { "no" }]),
                t,
            );
        }
        if let Some((_, role, _)) = &e.assume {
            low(key(&[o, "a", role, "n", "by", a]), t);
        }
    }
    for (k, t) in mins {
        if kv.get(&k)?.as_ref().and_then(int).is_none_or(|v| t < v) {
            kv.set(&k, Scalar::Int(t))?;
        }
    }
    Ok(())
}
