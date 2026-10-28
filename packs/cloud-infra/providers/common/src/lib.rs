//! What the provider plugins share: the normalized shape every source is
//! put into, dotted lookups into a record, and the trimming helpers.

use serde::Serialize;
use serde_json::{json, Map, Value};

/// The same fields for every source: who, which org, what, from where, to
/// where, on what. Partitioning keys on `org`; the window scopes on `actor`
/// and `org`; the rest is for the models.
#[derive(Debug, Default, Serialize)]
pub struct Norm {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub actor: Option<String>,
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
}

/// `a.b[0].c` into a record, as text (numbers and booleans as they print).
pub fn get(v: &Value, path: &str) -> Option<String> {
    let mut cur = v;
    for seg in path.split('.') {
        let (name, idx) = match seg.find('[') {
            Some(i) => (&seg[..i], seg[i + 1..].trim_end_matches(']').parse::<usize>().ok()),
            None => (seg, None),
        };
        if !name.is_empty() {
            cur = cur.get(name)?;
        }
        if let Some(i) = idx {
            cur = cur.get(i)?;
        }
    }
    match cur {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// `keys` of `m`, in that order, when present.
pub fn pick(m: &Map<String, Value>, keys: &[&str]) -> Map<String, Value> {
    let mut out = Map::new();
    for k in keys {
        if let Some(v) = m.get(*k) {
            out.insert((*k).to_string(), v.clone());
        }
    }
    out
}

pub fn obj<'a>(v: Option<&'a Value>) -> Option<&'a Map<String, Value>> {
    v.and_then(Value::as_object)
}

/// A value kept whole when small; past `max` bytes, its top-level scalars
/// and the names of what was left out.
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

/// Fields other plugins attached before this one (`_...`), carried over.
pub fn carry_attached(m: &Map<String, Value>, out: &mut Map<String, Value>) {
    for (k, v) in m {
        if k.starts_with('_') && !out.contains_key(k) {
            out.insert(k.clone(), v.clone());
        }
    }
}

/// The company's name from `profile.yaml` in the plugin's data directory,
/// when there is one. Its own infrastructure's records all belong to it.
pub fn org_from_profile() -> Option<String> {
    let text = std::fs::read_to_string("/data/profile.yaml").ok()?;
    let f: Value = serde_yaml::from_str(&text).ok()?;
    f["org"].as_str().map(str::to_string)
}

/// The trimmed record with `_norm` beside it, as one NDJSON line.
pub fn emit(out: &mut Vec<u8>, mut rec: Map<String, Value>, n: &Norm) -> Result<(), String> {
    rec.insert("_norm".into(), serde_json::to_value(n).map_err(|e| e.to_string())?);
    out.extend(serde_json::to_vec(&Value::Object(rec)).map_err(|e| e.to_string())?);
    out.push(b'\n');
    Ok(())
}
