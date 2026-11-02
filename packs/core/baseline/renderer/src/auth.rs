//! `auth`: the sign-ins and credential changes of the actor's person, and of
//! who is behind it, in every source over 72 hours: logins per source, failed
//! logins, MFA, password, key and grant changes, and the last login's address,
//! network and MFA.

use baselinekit::people::People;
use baselinekit::time::{H, M};
use baselinekit::{Kv, R};
use serde_json::Value;

use crate::before::Before;
use crate::header::Win;
use crate::text::{ago, join, nn};

const AUTH: i64 = 72 * H;
const SIGN_IN: i64 = 10 * M; // an Okta sign-in's several events
const MFA_NEAR: i64 = 5 * 60_000;

/// The line, and the logins in it, which `links` reads too.
pub fn auth<K: Kv>(
    b: &mut Before<K>,
    w: &Win,
    behind: Option<&str>,
) -> R<(String, Vec<(i64, Value)>)> {
    let (a, ws, people) = (w.actor, w.ws, w.people);
    let own = &w.own;
    let mut whose = vec![own.clone()];
    whose.extend(behind.map(|r| people.person(r)));
    let (mut ents, mut failed) = (Vec::new(), 0);
    for p in &whose {
        ents.extend(b.events(&["p", p, "e"], ws - AUTH)?);
        failed += b.failed(p, ws - AUTH)?;
    }
    ents.sort_by_cached_key(|(t, x)| (*t, x.to_string()));
    ents.dedup(); // a row filed under both
    let logins: Vec<&(i64, Value)> = ents.iter().filter(|(_, x)| kind(x) == "login").collect();
    let rest: Vec<&(i64, Value)> = ents
        .iter()
        .filter(|(_, x)| !matches!(kind(x).as_str(), "login" | "login failed" | "mfa ok"))
        .collect();
    let named = |x: &str| {
        let p = people.person(x);
        if people.names.contains_key(&p) {
            format!("person {p}")
        } else {
            x.to_string()
        }
    };
    let mut s = format!("last 72h, {}", named(a));
    if let Some(r) = behind {
        s += &format!(" and {}", named(r));
    }
    s += ": ";
    if logins.is_empty() && failed == 0 && rest.is_empty() {
        s += "no logins or credential changes";
    } else {
        let mut bysrc: Vec<(String, i64)> = Vec::new();
        for (_, x) in &logins {
            let src = x["what"]
                .as_str()
                .unwrap_or("")
                .split(' ')
                .next()
                .unwrap_or("")
                .to_string();
            match bysrc.iter_mut().find(|(k, _)| *k == src) {
                Some((_, n)) => *n += 1,
                None => bysrc.push((src, 1)),
            }
        }
        bysrc.sort_by_key(|x| std::cmp::Reverse(x.1));
        s += &nn(logins.len() as i64, "login");
        if !bysrc.is_empty() {
            s += &format!(" ({})", join(bysrc.iter().map(|(k, n)| format!("{k} {n}"))));
        }
        s += &format!(", {failed} failed");
        let mut alike: Vec<([String; 5], i64, i64)> = Vec::new();
        for (t, x) in &rest {
            let [kd, what, by, ip] =
                ["k", "what", "by", "ip"].map(|f| x[f].as_str().unwrap_or("").to_string());
            // whom it was done to, when not the actor
            let on = (x["to"].as_array().into_iter().flatten())
                .filter_map(Value::as_str)
                .filter(|p| p != own);
            let k = [kd, what, by, ip, join(on)];
            match alike.iter_mut().find(|(k2, ..)| *k2 == k) {
                Some((_, c, last)) => {
                    *c += 1;
                    *last = *t;
                }
                None => alike.push((k, 1, *t)),
            }
        }
        if alike.is_empty() {
            s += "; no mfa, password, key or grant changes";
        }
        for ([k, what, by, ip, on], c, t) in alike.iter().skip(alike.len().saturating_sub(4)) {
            let on = if on.is_empty() {
                String::new()
            } else {
                format!(" on {on}")
            };
            let by = if people.person(by) == *own {
                String::new()
            } else {
                format!(" by {by}")
            };
            let times = if *c > 1 {
                format!(" x{c}")
            } else {
                String::new()
            };
            let from = if ip.is_empty() {
                String::new()
            } else {
                format!(" from {ip}")
            };
            s += &format!("; {k} {what}{on}{by}{times} {} before{from}", ago(ws - t));
        }
        if let Some((t, x)) = logins.last() {
            let newnet = new_net(b, &by_of(people, x), x, *t)?;
            let mut mfa = match x["mfa"] {
                Value::Bool(true) => "yes",
                Value::Bool(false) => "no",
                _ => "unknown",
            };
            // Okta's MFA around an Okta sign-in is that sign-in's; a federated AWS one says
            // nothing of MFA, so Okta's stays a separate fact
            let near = |(t2, y): &(i64, Value)| kind(y) == "mfa ok" && (t2 - t).abs() <= MFA_NEAR;
            if mfa == "unknown" && ents.iter().any(near) {
                match x["src"].as_str() {
                    Some("okta") => mfa = "yes",
                    Some("cloudtrail") => mfa = "unknown (okta mfa within 5m)",
                    _ => {}
                }
            }
            s += &format!(
                "; last login {} {} before",
                x["what"].as_str().unwrap_or(""),
                ago(ws - t)
            );
            for (f, label) in [("ip", " from "), ("asn", " ASN "), ("cc", " ")] {
                if let Some(v) = x[f].as_str() {
                    s += &format!("{label}{v}");
                }
            }
            if !newnet.is_empty() {
                s += &format!(" (new {} for person then)", newnet.join("/"));
            }
            s += &format!(", mfa {mfa}");
            let okta: Vec<&str> = x["okta"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .collect();
            if !okta.is_empty() {
                s += &format!(" (okta: {})", okta.join(", "));
            }
        }
    }
    Ok((s, logins.into_iter().cloned().collect()))
}

fn kind(x: &Value) -> String {
    x["k"].as_str().unwrap_or("").to_string()
}

/// The person who signed in, in a login's row.
pub fn by_of(people: &People, x: &Value) -> String {
    people.person(x["by"].as_str().unwrap_or(""))
}

/// Which of the login's ASN and country the person first used within this
/// sign-in, having used another before it.
pub fn new_net<K: Kv>(b: &mut Before<K>, person: &str, x: &Value, t: i64) -> R<Vec<&'static str>> {
    let mut out = Vec::new();
    for d in ["asn", "cc"] {
        let Some(v) = x[d].as_str() else {
            continue;
        };
        let seen = b.firsts(&["p", person, "n", d])?;
        if seen
            .iter()
            .any(|(w, f)| w == v && t - SIGN_IN <= *f && *f <= t)
            && seen.iter().any(|(_, f)| *f < t - SIGN_IN)
        {
            out.push(d);
        }
    }
    Ok(out)
}
