use std::sync::Arc;

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

/// A record's scope for partition `name`: "name:" and its fields' values
/// joined with "|", a missing one empty.
pub fn scope(view: &JsonLogView, name: &str, fields: &[String]) -> Arc<str> {
    let parts: Vec<String> = fields
        .iter()
        .map(|path| {
            view.lookup(path)
                .and_then(JsonLogView::to_scalar)
                .map(scalar_text)
                .unwrap_or_default()
        })
        .collect();
    Arc::from(format!("{name}:{}", parts.join("|")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::BytesMut;

    #[test]
    fn record_scope() {
        let view = JsonLogView::from_bytes(BytesMut::from(
            r#"{"org_id":"acme","n":7,"user":{"id":"u1"}}"#,
        ))
        .unwrap();
        let of = |fields: &[&str]| {
            let fields: Vec<String> = fields.iter().map(|f| f.to_string()).collect();
            scope(&view, "org", &fields)
        };
        assert_eq!(&*of(&["org_id"]), "org:acme");
        assert_eq!(&*of(&["n"]), "org:7");
        assert_eq!(&*of(&["org_id", "user.id"]), "org:acme|u1");
        assert_eq!(&*of(&["org_id", "missing"]), "org:acme|");
        assert_eq!(&*of(&["missing"]), "org:");
    }
}
