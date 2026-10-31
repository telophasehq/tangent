//! A `document` table: a YAML or JSON file (JSON by a `.json` name). The key
//! is a dotted path into it, `[n]` or `.n` for an item of a list, and "" for
//! the whole document; a key that holds dots is found whole first. The answer
//! is the value there as JSON, none for a missing path or a null.

use std::path::Path;

use anyhow::Result;
use serde_json::{Map, Value};

pub struct Document(Value);

impl Document {
    pub fn parse(bytes: &[u8], path: &Path) -> Result<Self> {
        if path
            .extension()
            .is_some_and(|e| e.eq_ignore_ascii_case("json"))
        {
            return Ok(Self(serde_json::from_slice(bytes)?));
        }
        let yaml: serde_yaml::Value = serde_yaml::from_slice(bytes)?;
        Ok(Self(json(yaml)))
    }

    pub fn lookup(&self, path: &str) -> Option<String> {
        match at(&self.0, path)? {
            Value::Null => None,
            v => serde_json::to_string(v).ok(),
        }
    }
}

fn at<'a>(v: &'a Value, path: &str) -> Option<&'a Value> {
    if path.is_empty() {
        return Some(v);
    }
    // A key may hold dots: the longest key that starts the path goes first.
    if let Value::Object(m) = v {
        let ends = std::iter::once(path.len()).chain(path.match_indices('.').map(|(i, _)| i).rev());
        for end in ends {
            let found = m
                .get(&path[..end])
                .and_then(|next| at(next, path.get(end + 1..).unwrap_or("")));
            if found.is_some() {
                return found;
            }
        }
    }
    let (head, rest) = path.split_once('.').unwrap_or((path, ""));
    let (name, indexes) = head.split_once('[').unwrap_or((head, ""));
    let mut cur = match v {
        _ if name.is_empty() => v,
        Value::Object(m) if !indexes.is_empty() => m.get(name)?,
        Value::Array(items) => items.get(name.parse::<usize>().ok()?)?,
        _ => return None,
    };
    if !indexes.is_empty() {
        // `0][1]`: what follows the first `[`
        for i in indexes.trim_end_matches(']').split("][") {
            cur = cur.as_array()?.get(i.parse::<usize>().ok()?)?;
        }
    }
    at(cur, rest)
}

/// YAML as JSON: a key that is not a string becomes its text (an account id
/// written bare is a number to YAML), and a tag is dropped.
fn json(v: serde_yaml::Value) -> Value {
    use serde_yaml::Value as Y;
    match v {
        Y::Null => Value::Null,
        Y::Bool(b) => Value::Bool(b),
        Y::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::from(i)
            } else if let Some(u) = n.as_u64() {
                Value::from(u)
            } else {
                n.as_f64()
                    .and_then(serde_json::Number::from_f64)
                    .map_or(Value::Null, Value::Number)
            }
        }
        Y::String(s) => Value::String(s),
        Y::Sequence(items) => Value::Array(items.into_iter().map(json).collect()),
        Y::Mapping(m) => {
            let mut out = Map::with_capacity(m.len());
            for (k, v) in m {
                let k = match json(k) {
                    Value::String(s) => s,
                    other => other.to_string(),
                };
                out.insert(k, json(v));
            }
            Value::Object(out)
        }
        Y::Tagged(t) => json(t.value),
    }
}
