use anyhow::{bail, Result};
use bytes::BytesMut;
use chrono::{DateTime, Utc};
use prometheus::IntCounter;
use simd_json::base::ValueAsScalar;
use simd_json::value::tape::Value;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::sinks::template::Template;
use crate::SINK_RECORDS_UNTIMED_TOTAL;

/// A `timestamp_field`, found in a record as a node's `group_by` finds a
/// field: a key holding the whole of it first, as a flattened record has
/// (`"event.time"`), and then each key between the `.`s in the object at
/// the one before, `[n]` after a key being the nth item of its list
/// (`events[0].time`).
pub struct FieldPath {
    whole: Box<str>,
    steps: Box<[Step]>,
}

enum Step {
    Key(Box<str>),
    Item(usize),
}

impl FieldPath {
    pub fn parse(field: &str) -> Result<Self> {
        let Some(steps) = steps(field) else {
            bail!("timestamp_field {field:?}: keys joined by `.`, none empty, each followed by any `[n]`");
        };
        Ok(Self {
            whole: field.into(),
            steps: steps.into(),
        })
    }

    fn find<'t, 'i>(&self, record: Value<'t, 'i>) -> Option<Value<'t, 'i>> {
        if self.steps.len() > 1 {
            if let Some(v) = record.get(&*self.whole) {
                return Some(v);
            }
        }
        self.steps.iter().try_fold(record, |v, step| match step {
            Step::Key(k) => v.get(&**k),
            Step::Item(n) => v.get_idx(*n),
        })
    }
}

/// `a.b[0][1]` is key `a`, key `b`, item 0, item 1; none for an empty key
/// or anything but `[n]`s after a key.
fn steps(field: &str) -> Option<Vec<Step>> {
    let mut steps = Vec::new();
    for segment in field.split('.') {
        let (key, mut items) = segment.split_at(segment.find('[').unwrap_or(segment.len()));
        if key.is_empty() {
            return None;
        }
        steps.push(Step::Key(key.into()));
        while !items.is_empty() {
            let (n, rest) = items.strip_prefix('[')?.split_once(']')?;
            steps.push(Step::Item(n.parse().ok()?));
            items = rest;
        }
    }
    Some(steps)
}

/// A WAL sink's reader of each record's own time, for its prefix to render:
/// the RFC 3339 string in the field its `timestamp_field` names.
pub struct EventTime {
    sink: Arc<str>,
    field: FieldPath,
    untimed: IntCounter,
    warned: AtomicBool,
}

impl EventTime {
    pub fn new(sink: &Arc<str>, field: FieldPath) -> Self {
        Self {
            sink: Arc::clone(sink),
            field,
            untimed: SINK_RECORDS_UNTIMED_TOTAL.with_label_values(&[&**sink]),
            warned: AtomicBool::new(false),
        }
    }

    /// The frame in parts, one for each prefix `template` renders for its
    /// records' times, in the order each prefix first appears; a part's
    /// records are in the frame's order. A record without a time (the field
    /// missing, not a string or not RFC 3339, or the line not JSON) goes
    /// under `now`'s prefix and is counted. A blank line goes with the
    /// record before it. A frame whose records all render one prefix, as a
    /// live one's do, is not copied.
    pub fn split(
        &self,
        frame: BytesMut,
        template: &Template,
        now: DateTime<Utc>,
    ) -> Vec<(Arc<str>, BytesMut)> {
        let mut reader = Reader::default();
        let mut rendered = String::new();
        let mut prefixes: Vec<Arc<str>> = Vec::new();
        // (start, end, prefix): lines in a row under one prefix
        let mut runs: Vec<(usize, usize, usize)> = Vec::new();
        let mut untimed = 0;
        let mut start = 0;
        while start < frame.len() {
            let end = memchr::memchr(b'\n', &frame[start..]).map_or(frame.len(), |i| start + i + 1);
            let line = &frame[start..end];
            if line.trim_ascii().is_empty() {
                if let Some(run) = runs.last_mut() {
                    run.1 = end;
                }
                start = end;
                continue;
            }
            let at = reader.time(line, &self.field).unwrap_or_else(|| {
                untimed += 1;
                now
            });
            rendered.clear();
            template.render_into(at, &mut rendered);
            let ix = match runs.last() {
                Some(&(_, _, ix)) if *prefixes[ix] == *rendered => ix,
                _ => match prefixes.iter().position(|p| **p == *rendered) {
                    Some(ix) => ix,
                    None => {
                        prefixes.push(Arc::from(rendered.as_str()));
                        prefixes.len() - 1
                    }
                },
            };
            match runs.last_mut() {
                Some(run) if run.2 == ix => run.1 = end,
                Some(_) => runs.push((start, end, ix)),
                // blank lines before the first record go with it
                None => runs.push((0, end, ix)),
            }
            start = end;
        }
        if untimed > 0 {
            self.untimed.inc_by(untimed);
            if !self.warned.swap(true, Ordering::Relaxed) {
                tracing::warn!(
                    sink = %self.sink,
                    timestamp_field = %self.field.whole,
                    "a record without an RFC 3339 time in its timestamp_field goes under the time it reaches the sink; tangent_sink_records_untimed_total counts them"
                );
            }
        }
        if prefixes.len() <= 1 {
            let prefix = prefixes.pop().unwrap_or_else(|| template.render(now));
            return vec![(prefix, frame)];
        }
        let mut sizes = vec![0; prefixes.len()];
        for &(s, e, ix) in &runs {
            sizes[ix] += e - s;
        }
        let mut parts: Vec<BytesMut> = sizes.into_iter().map(BytesMut::with_capacity).collect();
        for (s, e, ix) in runs {
            parts[ix].extend_from_slice(&frame[s..e]);
        }
        prefixes.into_iter().zip(parts).collect()
    }
}

/// Reads one field of a line: into a copy, since simd-json unescapes
/// strings in place, kept with simd-json's buffers for the frame's next
/// line.
#[derive(Default)]
struct Reader {
    line: Vec<u8>,
    buffers: simd_json::Buffers,
}

impl Reader {
    fn time(&mut self, line: &[u8], field: &FieldPath) -> Option<DateTime<Utc>> {
        self.line.clear();
        self.line.extend_from_slice(line);
        let tape = simd_json::to_tape_with_buffers(&mut self.line, &mut self.buffers).ok()?;
        let v = field.find(tape.as_value())?;
        // trimmed, as a sqlite sink's timestamp_key is
        let t = DateTime::parse_from_rfc3339(v.as_str()?.trim()).ok()?;
        Some(t.with_timezone(&Utc))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const T1: &str = "2026-11-01T23:59:59Z";
    const T2: &str = "2026-11-02T00:00:00Z";

    fn time_of(record: &str, field: &str) -> Option<String> {
        let field = FieldPath::parse(field).unwrap();
        let t = Reader::default().time(record.as_bytes(), &field)?;
        Some(t.format("%FT%TZ").to_string())
    }

    /// A `timestamp_field` is found as a node's `group_by` finds a field:
    /// a key holding all of it first, then the objects and lists below.
    #[test]
    fn a_field_is_found_as_group_by_finds_one() {
        let nested = format!(r#"{{"event":{{"time":"{T1}"}}}}"#);
        assert_eq!(time_of(&nested, "event.time").as_deref(), Some(T1));
        let flat = format!(r#"{{"event.time":"{T1}"}}"#);
        assert_eq!(time_of(&flat, "event.time").as_deref(), Some(T1));
        let both = format!(r#"{{"event":{{"time":"{T2}"}},"event.time":"{T1}"}}"#);
        assert_eq!(time_of(&both, "event.time").as_deref(), Some(T1));
        let items = format!(r#"{{"events":[{{"time":"noon"}},{{"time":"{T1}"}}]}}"#);
        assert_eq!(time_of(&items, "events[1].time").as_deref(), Some(T1));
        let grid = format!(r#"{{"m":[[0,"{T1}"]]}}"#);
        assert_eq!(time_of(&grid, "m[0][1]").as_deref(), Some(T1));
        // trimmed, and in UTC
        let padded = r#"{"ts":" 2026-11-01T15:59:59-08:00 "}"#;
        assert_eq!(time_of(padded, "ts").as_deref(), Some(T1));
        for (record, field) in [
            (r#"{"ts":1792501200}"#, "ts"),
            (r#"{"ts":"noon"}"#, "ts"),
            (r#"{"event":"x"}"#, "event.time"),
            (r#"{"events":{"0":{"time":"x"}}}"#, "events[0].time"),
            ("not json", "ts"),
        ] {
            assert_eq!(time_of(record, field), None, "{record} {field}");
        }
    }

    #[test]
    fn a_field_is_keys_joined_by_dots_each_with_any_items() {
        for good in ["ts", "event.time", "events[0].time", "m[0][1]", "a.b.c"] {
            assert!(FieldPath::parse(good).is_ok(), "{good}");
        }
        for bad in [
            "", "a..b", ".ts", "ts.", "[0]", "a.[0]", "a[", "a[x]", "a[0]x", "a[-1]",
        ] {
            assert!(FieldPath::parse(bad).is_err(), "{bad}");
        }
    }
}
