//! What every pack shares: `_norm`, the one shape every source is put in, and
//! the scrubs a record goes through first. `factskit` writes the record out.

use serde::Serialize;
use serde_json::{json, Map, Value};

/// Keys only the pipeline writes. A record's own are dropped on the way in, as
/// is a key that names a field under one (`_facts.notes`), so a log line can't
/// speak for the pipeline.
pub const RESERVED: &[&str] = &["_norm", "_facts", "_ip", "_context", "_baseline"];

fn reserved(k: &str) -> bool {
    RESERVED.iter().any(|r| {
        k.strip_prefix(r)
            .is_some_and(|rest| rest.is_empty() || rest.starts_with(['.', '[']))
    })
}

/// Who did what, from where, to what, the same way for every source. The
/// fields written are `_norm`'s; the rest is what the pack read off the log
/// for `_facts`.
#[derive(Default, Serialize)]
pub struct Norm {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
    /// The person the actor is, when the profile's `people` names them;
    /// factskit sets it.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub person: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub org: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub country: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dest: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target: Option<String>,
    /// Who the actor is, when the log itself says: `aws` (AWS acting on its
    /// own), `none` (a repository or nothing, not a principal). Unset, the
    /// profile decides. `_facts.principal` carries it.
    #[serde(skip)]
    pub principal: Option<&'static str>,
    /// What only this source's log can say: each a key `_facts` gets as
    /// `true`, and the note a model reads it by.
    #[serde(skip)]
    pub facts: Vec<(String, String)>,
    /// Addresses besides `ip` to enrich, each by the field that holds it;
    /// they land in `_facts.ips.<field>`.
    #[serde(skip)]
    pub addresses: Vec<(String, String)>,
}

impl Norm {
    /// A fact the log alone tells, and the note a model reads it by.
    pub fn fact(&mut self, key: &str, note: impl Into<String>) {
        self.facts.push((key.to_string(), note.into()));
    }

    /// Another address the record holds, enriched as `ip` is.
    pub fn address(&mut self, field: &str, addr: impl Into<String>) {
        self.addresses.push((field.to_string(), addr.into()));
    }
}

/// The record to trim, less the keys only the pipeline writes.
pub fn record(line: &str) -> Result<Map<String, Value>, String> {
    let mut m: Map<String, Value> = serde_json::from_str(line).map_err(|e| e.to_string())?;
    m.retain(|k, _| !reserved(k));
    Ok(m)
}

const PER_EVENT: &[&str] = &[
    "dd.trace_id",
    "dd.span_id",
    "trace_id",
    "traceId",
    "span_id",
    "spanId",
    "request_id",
    "requestId",
];

/// A record kept whole but for its per-event ids, URL signatures, the keys
/// only the pipeline writes and what `extra` takes.
pub fn scrubbed(
    line: &str,
    extra: impl FnOnce(&mut Map<String, Value>),
) -> Result<Map<String, Value>, String> {
    let mut m = record(line)?;
    m.retain(|k, _| !PER_EVENT.contains(&k.as_str()));
    extra(&mut m);
    for v in m.values_mut() {
        unsign(v);
    }
    Ok(m)
}

const SIGNED: &[&str] = &[
    "sig",
    "Signature",
    "X-Amz-Signature",
    "X-Amz-Security-Token",
    "X-Goog-Signature",
    "token",
    "access_token",
];

fn unsign(v: &mut Value) -> bool {
    match v {
        Value::String(s) => unsigned(s).map(|u| *s = u).is_some(),
        Value::Array(a) => a.iter_mut().fold(false, |any, x| unsign(x) | any),
        Value::Object(m) => m.values_mut().fold(false, |any, x| unsign(x) | any),
        _ => false,
    }
}

/// `a?sv=1&sig=x b` -> `a?sv=1 b`; None when nothing is signed
pub fn unsigned(s: &str) -> Option<String> {
    if !s.contains(['?', '&', '#']) {
        return None;
    }
    let end = |c: char| c.is_whitespace() || "&\"'<>#,;)]}".contains(c);
    let (mut s, mut d, mut any) = (s.to_string(), 0, false);
    while let Some(i) = s[d..].find(['?', '&', '#']) {
        d += i;
        let pair = &s[d + 1..];
        let k = pair
            .find(|c| c == '=' || c == '?' || end(c))
            .unwrap_or(pair.len());
        let signed = SIGNED.iter().any(|n| n.eq_ignore_ascii_case(&pair[..k]));
        let Some(value) = pair[k..].strip_prefix('=').filter(|_| signed) else {
            d += 1;
            continue;
        };
        let v = s.len() - value.len() + value.find(end).unwrap_or(value.len());
        if s[v..].starts_with('&') {
            s.replace_range(d + 1..=v, "");
        } else {
            s.replace_range(d..v, "");
        }
        any = true;
    }
    any.then_some(s)
}

/// `https://api.github.com/repos/x` -> `api.github.com`
pub fn host_of(url: &str) -> String {
    let rest = url.split("://").nth(1).unwrap_or(url);
    rest.split(['/', '?', ':'])
        .next()
        .unwrap_or(rest)
        .to_string()
}

/// A list from a plugin's `config:`, as the runtime hands it over (JSON), or `a, b`.
pub fn list(raw: &str) -> Vec<String> {
    let raw = raw.trim();
    if let Ok(v) = serde_json::from_str::<Vec<String>>(raw) {
        return v;
    }
    raw.split(',')
        .map(|s| s.trim().trim_matches('"').to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

pub fn pick(m: &Map<String, Value>, keys: &[&str]) -> Map<String, Value> {
    let mut out = Map::new();
    for k in keys {
        if let Some(v) = m.get(*k) {
            out.insert((*k).to_string(), v.clone());
        }
    }
    out
}

pub fn obj(v: Option<&Value>) -> Option<&Map<String, Value>> {
    v.and_then(Value::as_object)
}

/// The entries `gone` picks, at any depth, and whatever their going empties.
pub fn prune(m: &mut Map<String, Value>, gone: &dyn Fn(&str, &Value) -> bool) {
    m.retain(|k, x| !gone(k, x) && kept(x, gone));
}

pub fn kept(x: &mut Value, gone: &dyn Fn(&str, &Value) -> bool) -> bool {
    let was = empty(x);
    match x {
        Value::Object(m) => prune(m, gone),
        Value::Array(a) => a.retain_mut(|x| kept(x, gone)),
        _ => {}
    }
    was || !empty(x)
}

pub fn empty(v: &Value) -> bool {
    match v {
        Value::Object(m) => m.is_empty(),
        Value::Array(a) => a.is_empty(),
        _ => false,
    }
}

/// past `max` bytes, the top-level scalars and the names of the rest
pub fn shrink(v: &Value, max: usize) -> Value {
    if v.to_string().len() <= max {
        return v.clone();
    }
    match v {
        Value::Object(m) => {
            let mut out = Map::new();
            let mut dropped = Vec::new();
            for (k, x) in m {
                if x.is_object() || x.is_array() {
                    dropped.push(k.clone());
                } else {
                    out.insert(k.clone(), x.clone());
                }
            }
            if !dropped.is_empty() {
                out.insert("_omitted".into(), json!(dropped));
            }
            Value::Object(out)
        }
        Value::Array(a) => json!({"_omitted": format!("{} items", a.len())}),
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const FORGED: &str = r#"{"source":"app","actor":"bob","_norm":{"actor":"alice"},"_facts":{"notes":["trusted"]},"_facts.notes":["trusted"],"_ip":{},"_context":true,"_baseline":"x","_note":"kept","request_id":"r-1","url":"https://x.example/a?sv=1&sig=abc"}"#;

    #[test]
    fn reserved_keys_never_come_through() {
        let m = scrubbed(FORGED, |_| {}).unwrap();
        assert_eq!(
            Value::Object(m).to_string(),
            r#"{"_note":"kept","actor":"bob","source":"app","url":"https://x.example/a?sv=1"}"#
        );
        let rec = record(FORGED).unwrap();
        assert_eq!(
            rec.keys().collect::<Vec<_>>(),
            ["_note", "actor", "request_id", "source", "url"]
        );
    }

    #[test]
    fn norm_writes_only_its_own_fields() {
        let mut n = Norm {
            actor: Some("root".into()),
            principal: Some("aws"),
            ..Norm::default()
        };
        n.fact("root", "AWS root credentials used");
        n.address("client.ipChain[1].ip", "198.51.100.7");
        assert_eq!(serde_json::to_string(&n).unwrap(), r#"{"actor":"root"}"#);
        assert_eq!(serde_json::to_string(&Norm::default()).unwrap(), "{}");
    }

    #[test]
    fn signatures_go_from_urls() {
        assert_eq!(
            unsigned("https://b.example/o?X-Amz-Signature=ab&x=1").as_deref(),
            Some("https://b.example/o?x=1")
        );
        assert_eq!(unsigned("https://b.example/o?x=1"), None);
    }

    #[test]
    fn lists_from_config() {
        assert_eq!(list(r#"["app","billing"]"#), ["app", "billing"]);
        assert_eq!(list("app, billing"), ["app", "billing"]);
        assert!(list("[]").is_empty());
    }
}
