//! What the `store` sink and the `store` source share: how a record is kept
//! and what each partition's window bookkeeping is called.
//!
//! Records live in the sink's namespace under their partition, keyed
//! `ev:<epoch ms, 13 digits>:<ulid>`, so a partition's history is a key
//! range in time order. Per partition, the namespace `windows` holds:
//!
//! - `watermark`: the latest record time stored;
//! - `last_arrival`: the clock when a record last arrived;
//! - `first`: the earliest record time stored;
//! - `read_through`: the end of the latest window the source has read;
//! - `closed_through`: the end of the latest window whose records reached
//!   their sinks, every window before it done too;
//! - `late:<record key>`: a record stored after its window was read.
//!
//! Nothing about a window lives only in memory: a restart reads these and
//! carries on.

use std::sync::Arc;

use anyhow::Result;
use serde_json::Value;

use crate::state::{StateStore, StateValue};

pub const WINDOWS_NS: &str = "windows";
pub const WATERMARK: &str = "watermark";
pub const LAST_ARRIVAL: &str = "last_arrival";
pub const FIRST: &str = "first";
pub const READ_THROUGH: &str = "read_through";
pub const CLOSED_THROUGH: &str = "closed_through";
pub const LATE_PREFIX: &str = "late:";

/// A stored record's key.
pub fn ev_key(ts: i64, id: &str) -> String {
    format!("ev:{ts:013}:{id}")
}

/// The first key of millisecond `ts`: ranges `[ev_bound(a), ev_bound(b))`
/// hold the records with `a <= time < b`.
pub fn ev_bound(ts: i64) -> String {
    format!("ev:{:013}:", ts.max(0))
}

/// The record time in a stored key.
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

/// A record's own time from `field`: RFC 3339, or epoch seconds or
/// milliseconds, as a number or a string.
pub fn time_of_value(v: &Value, field: &str) -> Option<i64> {
    let x = v.get(field).or_else(|| {
        let mut cur = v;
        for seg in field.split('.') {
            cur = cur.get(seg)?;
        }
        Some(cur)
    })?;
    let epoch = |n: f64| if n.abs() > 1e11 { n as i64 } else { (n * 1000.0) as i64 };
    match x {
        Value::String(s) => chrono::DateTime::parse_from_rfc3339(s.trim())
            .map(|t| t.timestamp_millis())
            .ok()
            .or_else(|| s.trim().parse::<f64>().ok().map(epoch)),
        Value::Number(n) => n.as_f64().map(epoch),
        _ => None,
    }
}

/// Epoch milliseconds as RFC 3339, to the second.
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
