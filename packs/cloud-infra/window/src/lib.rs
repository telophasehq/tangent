//! Reads history back for the records the gate let through. A `store` sink
//! keeps every record in its org's history as `ev:<time>:<seq>`; under
//! `partition: org`, one `scan` over the longest span gives the org's recent
//! rows in order.
//! For each of the record's scopes those rows are narrowed to the scope (the
//! org is the partition itself; an actor is the rows where the scope field
//! matches) and to the scope's own span: a day for what the actor did, an
//! hour for what everyone else did. Only gated records come here, so the cost
//! of context follows what is judged.
//!
//! plugins.window.config:
//!   time: ts
//!   scopes: { actor: _norm.actor, org: _norm.org }
//!   spans: { actor: 24h, org: 1h }   # per scope; `span` for any not listed
//!   span: 1h
//!   max_events: 20        # the newest rows carried in full
//! Runs in the shared plugin namespace, where the store sink keeps the rows.

wit_bindgen::generate!({
    path: ".tangent/wit",
    world: "processor",
    generate_all,
});

use exports::tangent::logs::mapper::{Guest, Meta, Pred, Selector};
use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, BTreeSet};
use tangent::logs::log::{Logview, Scalar};
use tangent::logs::{config, state};

struct Component;

export!(Component);

struct Cfg {
    time: String,
    scopes: BTreeMap<String, String>,
    /// How far back each scope looks, by label; `span_ms` for the rest.
    spans: BTreeMap<String, i64>,
    span_ms: i64,
    max_events: usize,
}

fn setting(name: &str) -> Option<String> {
    let raw = config::get(name)?;
    let t = raw.trim();
    let t = t
        .strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .unwrap_or(t);
    (!t.is_empty()).then(|| t.to_string())
}

fn duration_ms(s: &str) -> Option<i64> {
    let s = s.trim();
    let split = s
        .find(|c: char| !c.is_ascii_digit() && c != '.')
        .unwrap_or(s.len());
    let n: f64 = s[..split].parse().ok()?;
    let mult = match s[split..].trim() {
        "ms" => 1.0,
        "" | "s" => 1000.0,
        "m" => 60_000.0,
        "h" => 3_600_000.0,
        "d" => 86_400_000.0,
        _ => return None,
    };
    Some((n * mult) as i64)
}

fn cfg() -> Result<Cfg, String> {
    let span = setting("span").unwrap_or_else(|| "1h".to_string());
    let scopes: BTreeMap<String, String> = setting("scopes")
        .and_then(|s| serde_json::from_str(&s).ok())
        .ok_or("window: config `scopes` must map a label to a field, like {actor: _norm.actor}")?;
    let spans_raw: BTreeMap<String, String> = setting("spans")
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default();
    let mut spans = BTreeMap::new();
    for (label, d) in spans_raw {
        let ms = duration_ms(&d).ok_or(format!("window: bad span for `{label}`"))?;
        spans.insert(label, ms);
    }
    Ok(Cfg {
        time: setting("time").unwrap_or_else(|| "ts".to_string()),
        scopes,
        spans,
        span_ms: duration_ms(&span).ok_or("window: bad `span`")?,
        max_events: setting("max_events")
            .and_then(|s| s.parse().ok())
            .unwrap_or(40),
    })
}

/// `86400000` as `24h`, `3600000` as `1h`, `90000` as `90s`.
fn human(ms: i64) -> String {
    if ms % 3_600_000 == 0 {
        format!("{}h", ms / 3_600_000)
    } else if ms % 60_000 == 0 {
        format!("{}m", ms / 60_000)
    } else {
        format!("{}s", ms / 1000)
    }
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

fn civil_from_days(z: i64) -> (i64, i64, i64) {
    let z = z + 719468;
    let era = z.div_euclid(146097);
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

fn parse_ts(v: &Value) -> Option<i64> {
    match v {
        Value::Number(n) => n.as_f64().map(epoch_to_ms),
        Value::String(s) => {
            let s = s.trim();
            if let Ok(n) = s.parse::<f64>() {
                return Some(epoch_to_ms(n));
            }
            let num = |a: usize, l: usize| s.get(a..a + l)?.parse::<i64>().ok();
            let (y, mo, d) = (num(0, 4)?, num(5, 2)?, num(8, 2)?);
            let (h, mi, se) = (num(11, 2)?, num(14, 2)?, num(17, 2)?);
            let b = s.as_bytes();
            let mut i = 19;
            let mut ms = 0;
            if b.get(i) == Some(&b'.') {
                i += 1;
                let start = i;
                while i < b.len() && b[i].is_ascii_digit() {
                    i += 1;
                }
                let frac3: String = format!("{:0<3}", &s[start..i]).chars().take(3).collect();
                ms = frac3.parse::<i64>().unwrap_or(0);
            }
            let offset = match b.get(i) {
                None | Some(b'Z') | Some(b'z') => 0,
                Some(sign @ (b'+' | b'-')) => {
                    let oh = num(i + 1, 2)?;
                    let om = num(i + 4, 2)?;
                    (oh * 3600 + om * 60) * if *sign == b'+' { 1 } else { -1 }
                }
                _ => return None,
            };
            let secs = days_from_civil(y, mo, d) * 86400 + h * 3600 + mi * 60 + se - offset;
            Some(secs * 1000 + ms)
        }
        _ => None,
    }
}

fn epoch_to_ms(n: f64) -> i64 {
    if n.abs() > 1e11 {
        n as i64
    } else {
        (n * 1000.0) as i64
    }
}

fn iso(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let (y, m, d) = civil_from_days(secs.div_euclid(86400));
    let rem = secs.rem_euclid(86400);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        (rem % 3600) / 60,
        rem % 60
    )
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn lookup<'a>(v: &'a Value, path: &str) -> Option<&'a Value> {
    if let Some(direct) = v.get(path) {
        return Some(direct);
    }
    let mut cur = v;
    for seg in path.split('.') {
        cur = cur.get(seg)?;
    }
    Some(cur)
}

fn text(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

fn annotate(cfg: &Cfg, mut rec: Value) -> Result<Value, String> {
    let ts = lookup(&rec, &cfg.time)
        .and_then(parse_ts)
        .unwrap_or_else(now_ms);
    // The record as the runtime stored it: before anything was attached.
    let mut bare = rec.clone();
    if let Some(obj) = bare.as_object_mut() {
        obj.remove("_gate");
        obj.remove("_window");
    }
    let me = serde_json::to_string(&bare).map_err(|e| e.to_string())?;

    // The partition's rows over the longest span, once; every scope is a
    // view of them, each over its own span.
    let longest = cfg
        .spans
        .values()
        .copied()
        .chain(std::iter::once(cfg.span_ms))
        .max()
        .unwrap_or(cfg.span_ms);
    let start = format!("ev:{:013}:", ts - longest);
    let end = format!("ev:{:013}:~", ts);
    let rows: Vec<(String, i64, Value)> = state::scan(&start, &end, 10000)?
        .into_iter()
        .filter_map(|(_, v)| match v {
            Scalar::Str(s) if s != me => {
                let e: Value = serde_json::from_str(&s).ok()?;
                let when = lookup(&e, &cfg.time).and_then(parse_ts).unwrap_or(0);
                Some((s, when, e))
            }
            _ => None,
        })
        .collect();

    let mut win = Map::new();
    // A row shown under one scope is not repeated under the next: the org
    // view holds what the actor view does not.
    let mut shown: BTreeSet<String> = BTreeSet::new();
    for (label, field) in &cfg.scopes {
        let Some(key) = lookup(&rec, field).and_then(text) else {
            continue;
        };
        let span_ms = cfg.spans.get(label).copied().unwrap_or(cfg.span_ms);
        let since = ts - span_ms;
        let mine: Vec<&(String, i64, Value)> = rows
            .iter()
            .filter(|(_, when, e)| {
                *when >= since
                    && lookup(e, field).and_then(text).as_deref() == Some(key.as_str())
            })
            .collect();
        let in_span = mine.len();
        let mut recent: Vec<Value> = Vec::new();
        for (s, _, e) in mine.iter().rev() {
            if recent.len() >= cfg.max_events {
                break;
            }
            if shown.insert((*s).clone()) {
                recent.push(compact(e, &cfg.time));
            }
        }
        recent.reverse();

        let mut w = Map::new();
        w.insert("from".into(), json!(iso(since)));
        w.insert("to".into(), json!(iso(ts)));
        w.insert("span".into(), json!(human(span_ms)));
        w.insert("events".into(), json!(in_span));
        w.insert("recent".into(), Value::Array(recent));
        win.insert(label.clone(), Value::Object(w));
    }
    if let Some(obj) = rec.as_object_mut() {
        obj.insert("_window".into(), Value::Object(win));
    }
    Ok(rec)
}

/// One line of history: when, who did what, from where, to what, and how it
/// ended. The record itself stays in the store whole; the judge reads the
/// current record in full and its history this way, which keeps a busy
/// actor's day in a few kilobytes. Real CloudTrail and Okta records run
/// 3-5 KB each, and twenty of them per scope killed a local model.
fn compact(e: &Value, time: &str) -> Value {
    let mut m = Map::new();
    if let Some(t) = lookup(e, time).and_then(text) {
        m.insert("ts".into(), json!(t));
    }
    if let Some(src) = e.get("source").and_then(Value::as_str) {
        m.insert("source".into(), json!(src));
    }
    for k in ["kind", "actor", "ip", "dest", "target"] {
        if let Some(v) = lookup(e, &format!("_norm.{k}")).and_then(text) {
            m.insert(k.into(), json!(v));
        }
    }
    if let Some(ip) = e.pointer("/_ip/_norm.ip").and_then(Value::as_object) {
        let mut parts: Vec<String> = Vec::new();
        for k in ["country", "org"] {
            if let Some(v) = ip.get(k).and_then(Value::as_str) {
                parts.push(v.to_string());
            }
        }
        match ip.get("hosting") {
            Some(Value::String(h)) => parts.push(format!("{h} hosting")),
            Some(Value::Bool(true)) => parts.push("hosting".into()),
            _ => {}
        }
        for k in ["tor", "vpn", "ours"] {
            if ip.get(k).and_then(Value::as_bool) == Some(true) {
                parts.push(k.into());
            }
        }
        if !parts.is_empty() {
            m.insert("where".into(), json!(parts.join(", ")));
        }
    }
    let outcome = e
        .pointer("/outcome/result")
        .or_else(|| e.get("errorCode"))
        .or_else(|| e.get("conclusion"))
        .and_then(Value::as_str);
    if let Some(o) = outcome {
        m.insert("outcome".into(), json!(o));
    }
    Value::Object(m)
}

impl Guest for Component {
    fn metadata() -> Meta {
        Meta {
            name: "window".to_string(),
            version: "0.1.0".to_string(),
        }
    }

    fn probe() -> Vec<Selector> {
        vec![Selector {
            any: Vec::new(),
            all: vec![Pred::Has("_gate".to_string())],
            none: Vec::new(),
        }]
    }

    fn process_logs(input: Vec<Logview>) -> Result<Vec<u8>, String> {
        let cfg = cfg()?;
        let mut out = Vec::new();
        for lv in input {
            let rec: Value = serde_json::from_str(&lv.log()).map_err(|e| e.to_string())?;
            let annotated = annotate(&cfg, rec)?;
            out.extend(serde_json::to_vec(&annotated).map_err(|e| e.to_string())?);
            out.push(b'\n');
        }
        Ok(out)
    }
}
