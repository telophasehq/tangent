//! What the polling sources share. A poller keeps a cursor and a set of
//! recently seen ids in the state store (namespace `sources`, partition the
//! source's name), so a restart resumes where it stopped and an overlapping
//! poll does not emit an event twice. Records go out stamped with `source`
//! and `ts`, one frame each, in time order.
//!
//! Each API reports events some time after they happen (CloudTrail by up to
//! fifteen minutes, say). A source names that lateness; every poll reads
//! from the cursor less the lateness, and the cursor ends a poll at the
//! newest event seen or at `now - lateness`, whichever is later. An id is
//! remembered for a few times the lateness, which is as long as a poll can
//! find it again.

use std::sync::Arc;

use anyhow::Result;
use bytes::BytesMut;
use chrono::{DateTime, Duration, Utc};
use serde_json::Value;
use tangent_shared::dag::NodeRef;

use crate::router::Router;
use crate::state::{StateStore, StateValue};

pub const SOURCES_NS: &str = "sources";

pub struct Poller {
    pub name: Arc<str>,
    pub from: NodeRef,
    store: Arc<dyn StateStore>,
    router: Arc<Router>,
    late: Duration,
}

impl Poller {
    /// `late`: how long after it happens an event may still show up.
    pub fn new(
        name: Arc<str>,
        store: Arc<dyn StateStore>,
        router: Arc<Router>,
        late: Duration,
    ) -> Self {
        Self {
            from: NodeRef::Source {
                name: Arc::clone(&name),
            },
            name,
            store,
            router,
            late,
        }
    }

    /// Where the last poll stopped, if there was one.
    pub async fn cursor(&self) -> Option<DateTime<Utc>> {
        match self.store.get(SOURCES_NS, &self.name, "cursor").await {
            Ok(Some(StateValue::Str(s))) => s.parse().ok(),
            _ => None,
        }
    }

    /// Where this poll starts: the cursor less the lateness window, or
    /// `since` ago (`15m`, `1h`, `7d`) on a first start.
    pub async fn start(&self, since: &str) -> DateTime<Utc> {
        match self.cursor().await {
            Some(c) => c - self.late,
            None => Utc::now() - Duration::milliseconds(duration_ms(since).unwrap_or(3_600_000)),
        }
    }

    /// A poll is over: the cursor moves to the newest event seen, or to
    /// `end` less the lateness window if that is later (so a quiet source
    /// does not re-read its past forever). It never moves back.
    pub async fn finish(&self, newest: Option<DateTime<Utc>>, end: DateTime<Utc>) {
        let floor = end - self.late;
        let mut at = newest.map_or(floor, |n| n.max(floor));
        if let Some(prev) = self.cursor().await {
            at = at.max(prev);
        }
        if let Err(e) = self
            .store
            .set(
                SOURCES_NS,
                &self.name,
                "cursor",
                &StateValue::Str(at.to_rfc3339()),
                Some(0),
            )
            .await
        {
            tracing::warn!(source = %self.name, "saving cursor: {e}");
        }
    }

    /// True the first time `id` is seen. It is remembered for as long as a
    /// later poll could find it again.
    pub async fn first_sight(&self, id: &str) -> bool {
        let key = format!("seen:{id}");
        match self.store.get(SOURCES_NS, &self.name, &key).await {
            Ok(Some(_)) => false,
            _ => {
                let ttl = (self.late.num_milliseconds() * 3).max(60_000) as u64;
                let _ = self
                    .store
                    .set(
                        SOURCES_NS,
                        &self.name,
                        &key,
                        &StateValue::Bool(true),
                        Some(ttl),
                    )
                    .await;
                true
            }
        }
    }

    /// Stamp each record with `source` and `ts` and forward it, in time order.
    pub async fn emit(
        &self,
        source: &str,
        mut records: Vec<(DateTime<Utc>, Value)>,
    ) -> Result<usize> {
        if records.is_empty() {
            return Ok(0);
        }
        records.sort_by_key(|(t, _)| *t);
        let mut frames = Vec::with_capacity(records.len());
        for (t, mut v) in records {
            stamp(source, t, &mut v);
            frames.push(frame(&v)?);
        }
        let n = frames.len();
        self.router.forward(&self.from, frames, Vec::new()).await?;
        Ok(n)
    }
}

/// `source` and `ts` (the latter only when the record has none) on a record.
pub fn stamp(source: &str, t: DateTime<Utc>, v: &mut Value) {
    if let Some(obj) = v.as_object_mut() {
        obj.insert("source".into(), Value::String(source.to_string()));
        obj.entry("ts")
            .or_insert_with(|| Value::String(t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true)));
    }
}

/// One NDJSON line.
pub fn frame(v: &Value) -> Result<BytesMut> {
    let mut buf = BytesMut::from(serde_json::to_vec(v)?.as_slice());
    buf.extend_from_slice(b"\n");
    Ok(buf)
}

/// `30s`, `15m`, `1h`, `7d` (a bare number is seconds) as milliseconds.
pub fn duration_ms(s: &str) -> Option<i64> {
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

/// The `rel="next"` URL of a `Link` header, if any.
pub fn next_link(header: &str) -> Option<String> {
    header.split(',').find_map(|part| {
        let (url, rel) = part.split_once(';')?;
        rel.contains("rel=\"next\"").then(|| {
            url.trim()
                .trim_start_matches('<')
                .trim_end_matches('>')
                .to_string()
        })
    })
}

/// A time in any of the shapes the APIs use: RFC 3339, epoch seconds or
/// milliseconds (as a number or a string).
pub fn parse_time(v: &Value) -> Option<DateTime<Utc>> {
    match v {
        Value::String(s) => s
            .parse::<DateTime<Utc>>()
            .ok()
            .or_else(|| s.parse::<f64>().ok().and_then(epoch)),
        Value::Number(n) => n.as_f64().and_then(epoch),
        _ => None,
    }
}

fn epoch(n: f64) -> Option<DateTime<Utc>> {
    let ms = if n.abs() > 1e11 {
        n as i64
    } else {
        (n * 1000.0) as i64
    };
    DateTime::from_timestamp_millis(ms)
}

/// Percent-encoding for a query value.
pub fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// A credential that is not there: say so once, and stay idle.
pub fn idle(name: &str, what: &str) {
    tracing::warn!(source = %name, "{what} is empty; the source stays idle");
}

/// A config string from `${VAR}`: empty means unset.
pub fn opt(s: &Option<String>) -> Option<&str> {
    s.as_deref().map(str::trim).filter(|s| !s.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_and_links() {
        assert_eq!(duration_ms("15m"), Some(900_000));
        assert_eq!(duration_ms("1h"), Some(3_600_000));
        assert_eq!(duration_ms("7d"), Some(604_800_000));
        assert_eq!(duration_ms("30"), Some(30_000));
        assert_eq!(duration_ms("soon"), None);
        let link = r#"<https://api.github.com/orgs/acme/audit-log?after=abc>; rel="next", <https://api.github.com/orgs/acme/audit-log?before=xyz>; rel="prev""#;
        assert_eq!(
            next_link(link).as_deref(),
            Some("https://api.github.com/orgs/acme/audit-log?after=abc")
        );
        assert_eq!(next_link(r#"<https://x>; rel="prev""#), None);
        assert_eq!(
            urlencode("created:>=2026-10-27T13:00:00+00:00"),
            "created%3A%3E%3D2026-10-27T13%3A00%3A00%2B00%3A00"
        );
    }

    #[test]
    fn times_in_three_shapes() {
        let a = parse_time(&Value::String("2026-10-27T13:00:00Z".into())).unwrap();
        let b = parse_time(&Value::Number(1_793_106_000_000u64.into())).unwrap();
        let c = parse_time(&Value::String("1793106000".into())).unwrap();
        assert_eq!(a, b);
        assert_eq!(a, c);
    }

    #[test]
    fn stamping_keeps_a_records_own_ts() {
        let t = parse_time(&Value::String("2026-10-27T13:00:00Z".into())).unwrap();
        let mut v = serde_json::json!({"eventName": "ConsoleLogin"});
        stamp("cloudtrail", t, &mut v);
        assert_eq!(v["source"], "cloudtrail");
        assert_eq!(v["ts"], "2026-10-27T13:00:00Z");
        let mut w = serde_json::json!({"ts": "2026-01-01T00:00:00Z"});
        stamp("okta", t, &mut w);
        assert_eq!(w["ts"], "2026-01-01T00:00:00Z");
        assert!(frame(&w).unwrap().ends_with(b"\n"));
    }
}
