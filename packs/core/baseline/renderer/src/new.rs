//! `new`: what the actor does here for the first time, or first did within
//! the hour: an api, resource, client, network, source, OAuth app, device or
//! token. And for `links`, when each OAuth app in the window was first seen.

use std::collections::BTreeSet;

use baselinekit::keys::{nets, values};
use baselinekit::kinds::quoted;
use baselinekit::time::H;
use baselinekit::{Kv, R};
use serde_json::{json, Map, Value};

use crate::before::Before;
use crate::header::Win;
use crate::text::{ago, cap};

const RECENT: i64 = H; // first seen this recently is still new

pub fn new<K: Kv>(b: &mut Before<K>, w: &Win, links: &mut Vec<String>) -> R<Value> {
    let (a, fresh, ws) = (w.actor, &w.fresh, w.ws);
    let mut win: Vec<(&str, Vec<String>)> = Vec::new();
    for e in fresh {
        for (dim, v) in values(e) {
            match win.iter_mut().find(|(d, _)| *d == dim) {
                Some((_, vs)) if !vs.contains(&v) => vs.push(v),
                Some(_) => {}
                None => win.push((dim, vec![v])),
            }
        }
    }
    let since = |f: Option<i64>| {
        f.map(|f| format!(" (first seen {} before)", ago(ws - f)))
            .unwrap_or_default()
    };
    let mut new = Map::new();
    for (dim, vals) in &win {
        let mut fresh_vals = Vec::new();
        for v in vals {
            let f = b.first(&["a", a, "n", dim, v])?;
            if f.is_some_and(|f| ws - f > RECENT) {
                continue;
            }
            let name = (fresh.iter().flat_map(|e| &e.oauth))
                .find(|(i, _)| *dim == "oauth" && i == v)
                .map(|(i, n)| quoted(n.as_deref().unwrap_or(i)));
            fresh_vals.push(name.unwrap_or_else(|| v.clone()) + &since(f));
        }
        if fresh_vals.is_empty() {
            continue;
        }
        let note = match *dim {
            "api" | "res" => format!(" ({} of {} in window)", fresh_vals.len(), vals.len()),
            "client" => {
                let mut before = b.firsts(&["a", a, "n", "client"])?;
                before.retain(|(_, f)| ws - f > RECENT); // the new ones are listed
                if before.is_empty() {
                    String::new()
                } else {
                    format!("; before: {}", cap(before.into_iter().map(|(c, _)| c), 3))
                }
            }
            _ => String::new(),
        };
        let label = match *dim {
            "res" => "resource",
            "src" => "source",
            other => other,
        };
        new.insert(label.into(), json!(cap(fresh_vals, 4) + &note));
    }
    let mut netnew = Vec::new();
    for (dim, label) in [("asn", "ASN"), ("cc", "country"), ("net", "net")] {
        let mut seen = BTreeSet::new();
        for e in fresh {
            for (d, v) in nets(e) {
                if d != dim || !seen.insert(v.clone()) {
                    continue;
                }
                let f = b.first(&["p", &w.own, "n", dim, &v])?;
                if f.is_some_and(|f| ws - f > RECENT) {
                    continue;
                }
                let org_new = b.first(&["o", "n", dim, &v])?.is_none();
                let name = e
                    .asorg
                    .as_ref()
                    .filter(|_| dim == "asn")
                    .map(|o| format!(" {o}"))
                    .unwrap_or_default();
                netnew.push(format!(
                    "{label} {v}{name}{}{}",
                    if org_new { " (new to org)" } else { "" },
                    since(f)
                ));
            }
        }
    }
    if !netnew.is_empty() {
        new.insert("network".into(), json!(netnew.join(", ")));
    }
    let mut seen = BTreeSet::new();
    for (id, name) in fresh.iter().flat_map(|e| &e.oauth) {
        if seen.insert(id) {
            if let Some(f) = b.first(&["a", a, "n", "oauth", id])? {
                let app = name.as_deref().map_or_else(|| quoted(id), quoted);
                links.push(format!(
                    "oauth app {app} first seen for this actor {} before",
                    ago(ws - f)
                ));
            }
        }
    }
    let nothing =
        "nothing: every api, resource, client and network here was first seen over an hour before";
    Ok(if new.is_empty() {
        json!(nothing)
    } else {
        Value::Object(new)
    })
}
