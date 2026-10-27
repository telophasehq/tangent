//! Which partition of the stream a record belongs to: the values of the
//! configured fields, joined. Computed from the record itself wherever it is
//! needed, so it survives plugins that rewrite records as long as they keep
//! the fields.

use std::sync::Arc;
use tangent_shared::runtime::PartitionConfig;

use crate::wasm::host::tangent::logs::log::Scalar;
use crate::wasm::host::JsonLogView;

fn scalar_text(s: Scalar) -> String {
    match s {
        Scalar::Str(s) => s,
        Scalar::Int(i) => i.to_string(),
        Scalar::Float(f) => f.to_string(),
        Scalar::Boolean(b) => b.to_string(),
        Scalar::Bytes(b) => format!("<{} bytes>", b.len()),
    }
}

/// None when the record has none of the key fields.
pub fn key_of_view(view: &JsonLogView, cfg: &PartitionConfig) -> Option<Arc<str>> {
    let parts: Vec<String> = cfg
        .key
        .iter()
        .map(|path| {
            view.lookup(path)
                .and_then(JsonLogView::to_scalar)
                .map(scalar_text)
                .unwrap_or_default()
        })
        .collect();
    join(parts, &cfg.separator)
}

/// The same, for an already-parsed `serde_json` record.
pub fn key_of_value(v: &serde_json::Value, cfg: &PartitionConfig) -> Option<Arc<str>> {
    let parts: Vec<String> = cfg
        .key
        .iter()
        .map(|path| {
            let mut cur = Some(v);
            if v.get(path).is_none() {
                for seg in path.split('.') {
                    cur = cur.and_then(|c| c.get(seg));
                }
            } else {
                cur = v.get(path);
            }
            match cur {
                Some(serde_json::Value::String(s)) => s.clone(),
                Some(serde_json::Value::Number(n)) => n.to_string(),
                Some(serde_json::Value::Bool(b)) => b.to_string(),
                _ => String::new(),
            }
        })
        .collect();
    join(parts, &cfg.separator)
}

fn join(parts: Vec<String>, separator: &str) -> Option<Arc<str>> {
    if parts.iter().all(String::is_empty) {
        return None;
    }
    Some(Arc::from(parts.join(separator)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    fn cfg(keys: &[&str]) -> PartitionConfig {
        PartitionConfig {
            key: keys.iter().map(|s| s.to_string()).collect(),
            separator: "|".into(),
        }
    }

    #[test]
    fn keys_from_views_and_values() {
        let raw =
            r#"{"org_id": "acme", "region": "us-west-2", "n": 7, "nested": {"tenant": "t1"}}"#;
        let view = JsonLogView::from_bytes(BytesMut::from(raw)).unwrap();
        let v: serde_json::Value = serde_json::from_str(raw).unwrap();

        assert_eq!(
            key_of_view(&view, &cfg(&["org_id"])).as_deref(),
            Some("acme")
        );
        assert_eq!(key_of_value(&v, &cfg(&["org_id"])).as_deref(), Some("acme"));
        assert_eq!(
            key_of_view(&view, &cfg(&["org_id", "region"])).as_deref(),
            Some("acme|us-west-2")
        );
        assert_eq!(key_of_value(&v, &cfg(&["n"])).as_deref(), Some("7"));
        assert_eq!(
            key_of_value(&v, &cfg(&["nested.tenant"])).as_deref(),
            Some("t1")
        );
        assert_eq!(
            key_of_view(&view, &cfg(&["org_id", "missing"])).as_deref(),
            Some("acme|"),
            "a missing field leaves its slot empty"
        );
        assert_eq!(key_of_view(&view, &cfg(&["missing"])), None);
        assert_eq!(key_of_value(&v, &cfg(&["missing"])), None);
    }
}
