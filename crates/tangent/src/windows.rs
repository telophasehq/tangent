use std::sync::Arc;

use anyhow::Result;
use serde_json::Value;

use crate::inference::lookup;
use crate::sources::poll::parse_time;
use crate::state::{StateStore, StateValue};

pub const EVENTS_NS: &str = "events";
pub const INDEX_NS: &str = "recent:events";
pub const TIME_FIELD: &str = "ts";

pub const WINDOWS_NS: &str = "windows";
pub const WATERMARK: &str = "watermark";
pub const LAST_ARRIVAL: &str = "last_arrival";
pub const READ_THROUGH: &str = "read_through";
pub const CLOSED_THROUGH: &str = "closed_through";
pub const LATE_PREFIX: &str = "late:";
pub const INDEXED: &str = "indexed";

/// `<scope>\x1f<value>\x1f`, then the record's key.
pub fn index_prefix(scope: &str, value: &str) -> String {
    format!("{scope}\u{1f}{value}\u{1f}")
}

pub fn scope_value(v: &Value) -> Option<String> {
    match v {
        Value::String(s) if !s.is_empty() => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

pub fn ev_key(ts: i64, id: &str) -> String {
    format!("ev:{ts:013}:{id}")
}

/// `[ev_bound(a), ev_bound(b))` holds the records with `a <= time < b`.
pub fn ev_bound(ts: i64) -> String {
    format!("ev:{:013}:", ts.max(0))
}

pub fn ts_of_key(key: &str) -> Option<i64> {
    key.strip_prefix("ev:")?.get(..13)?.parse().ok()
}

pub async fn get_int(
    store: &Arc<dyn StateStore>,
    partition: &str,
    key: &str,
) -> Result<Option<i64>> {
    Ok(match store.get(WINDOWS_NS, partition, key).await? {
        Some(StateValue::Int(i)) => Some(i),
        _ => None,
    })
}

pub async fn set_int(
    store: &Arc<dyn StateStore>,
    partition: &str,
    key: &str,
    value: i64,
) -> Result<()> {
    store
        .set(WINDOWS_NS, partition, key, &StateValue::Int(value), Some(0))
        .await
}

pub fn time_of_value(v: &Value, field: &str) -> Option<i64> {
    lookup(v, field).and_then(parse_time).map(|t| t.timestamp_millis())
}

pub fn iso(ms: i64) -> String {
    chrono::DateTime::from_timestamp_millis(ms)
        .map(|t| t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true))
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_sort_by_time_and_carry_it() {
        let a = ev_key(1_793_106_000_000, "01J");
        let b = ev_key(1_793_106_000_001, "00A");
        assert!(a < b);
        assert!(ev_bound(1_793_106_000_000) <= a && a < ev_bound(1_793_106_000_001));
        assert_eq!(ts_of_key(&a), Some(1_793_106_000_000));
        let v = serde_json::json!({"ts": "2026-10-27T13:00:00Z", "n": {"t": 1793106000}});
        assert_eq!(time_of_value(&v, "ts"), Some(1_793_106_000_000));
        assert_eq!(time_of_value(&v, "n.t"), Some(1_793_106_000_000));
        assert_eq!(iso(1_793_106_000_000), "2026-10-27T13:00:00Z");
    }
}
