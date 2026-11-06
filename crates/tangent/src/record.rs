//! A record's fields read as text, and its own time, for the sqlite sink's
//! columns.

use crate::wasm::host::tangent::logs::log::Scalar;
use crate::wasm::host::JsonLogView;

pub(crate) fn scalar_text(s: Scalar) -> String {
    match s {
        Scalar::Str(s) => s,
        Scalar::Int(i) => i.to_string(),
        Scalar::Float(f) => f.to_string(),
        Scalar::Boolean(b) => b.to_string(),
        Scalar::Bytes(b) => format!("<{} bytes>", b.len()),
    }
}

/// A record's own time as epoch milliseconds, from `field`: RFC 3339, or
/// epoch seconds or milliseconds. None without the field or when unreadable.
pub fn time_of(view: &JsonLogView, field: Option<&str>) -> Option<i64> {
    match view.lookup(field?).and_then(JsonLogView::to_scalar)? {
        Scalar::Str(s) => chrono::DateTime::parse_from_rfc3339(s.trim())
            .ok()
            .map(|t| t.timestamp_millis()),
        Scalar::Int(i) => Some(if i.abs() > 100_000_000_000 {
            i
        } else {
            i * 1000
        }),
        Scalar::Float(f) => Some(if f.abs() > 1e11 {
            f as i64
        } else {
            (f * 1000.0) as i64
        }),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_time() {
        let v = JsonLogView::from_bytes(
            r#"{"ts":"2026-10-20T13:00:00Z","epoch":1792501200,"ms":1792501200000,"bad":"noon"}"#
                .into(),
        )
        .unwrap();
        assert_eq!(time_of(&v, Some("ts")), Some(1_792_501_200_000));
        assert_eq!(time_of(&v, Some("epoch")), Some(1_792_501_200_000));
        assert_eq!(time_of(&v, Some("ms")), Some(1_792_501_200_000));
        assert_eq!(time_of(&v, Some("bad")), None);
        assert_eq!(time_of(&v, Some("missing")), None);
        assert_eq!(time_of(&v, None), None);
    }
}
