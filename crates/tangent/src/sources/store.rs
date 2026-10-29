//! The `store` source: what a `store` sink kept, read back from the state
//! store a buffer at a time as each closes. See `tangent_shared::sources::store` for
//! the rules and `crate::windows` for the bookkeeping.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use bytes::BytesMut;
use parking_lot::Mutex;
use serde_json::{json, Value};
use tangent_shared::dag::NodeRef;
use tangent_shared::sources::store::StoreSourceConfig;
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::router::Router;
use crate::sources::poll::frame;
use crate::state::{now_ms, StateStore, StateValue};
use crate::windows::{
    ev_bound, get_int, iso, set_int, ts_of_key, CLOSED_THROUGH, EVENTS_NS, LAST_ARRIVAL,
    LATE_PREFIX, READ_THROUGH, WATERMARK, WINDOWS_NS,
};
use crate::worker::Ack;

struct Rules {
    span: i64,
    late: i64,
    idle: i64,
    /// When set, only buffers overlapping one of these are judged.
    ranges: Vec<(i64, i64)>,
}

impl Rules {
    fn wanted(&self, start: i64, end: i64) -> bool {
        self.ranges.is_empty() || self.ranges.iter().any(|&(a, b)| start < b && a < end)
    }
}

/// One partition, as the source knows it while running. `closed` and the
/// windows before it are durable; `next` and `pending` are rebuilt from
/// `closed` at a start, so windows read but not done before a stop are read
/// again (at least once).
struct Part {
    closed: i64,
    next: i64,
    /// Windows read and not yet closed, by start: done or not.
    pending: BTreeMap<i64, bool>,
    late_in_flight: HashSet<String>,
}

enum Done {
    Window {
        part: String,
        start: i64,
        _permit: OwnedSemaphorePermit,
    },
    Late {
        part: String,
        markers: Vec<String>,
        _permit: OwnedSemaphorePermit,
    },
}

/// Fires when every record of a window has reached its sinks.
struct DoneAck {
    tx: mpsc::UnboundedSender<Done>,
    done: Mutex<Option<Done>>,
}

#[async_trait]
impl Ack for DoneAck {
    async fn ack(&self) -> Result<()> {
        if let Some(d) = self.done.lock().take() {
            let _ = self.tx.send(d);
        }
        Ok(())
    }
}

pub async fn run_consumer(
    name: Arc<str>,
    cfg: StoreSourceConfig,
    router: Arc<Router>,
    store: Arc<dyn StateStore>,
    shutdown: CancellationToken,
) -> Result<()> {
    if cfg.buffer_secs == 0 {
        bail!("store source: buffer_secs must be positive");
    }
    let at = |s: &str| {
        chrono::DateTime::parse_from_rfc3339(s.trim())
            .map(|t| t.timestamp_millis())
            .map_err(|e| anyhow!("store source: range time {s:?}: {e}"))
    };
    let mut ranges = Vec::with_capacity(cfg.ranges.len());
    for r in &cfg.ranges {
        ranges.push((at(&r.from)?, r.to.as_deref().map_or(Ok(i64::MAX), at)?));
    }
    let rules = Rules {
        span: cfg.buffer_secs as i64 * 1000,
        late: cfg.late_secs as i64 * 1000,
        idle: cfg.idle_secs.unwrap_or(cfg.late_secs) as i64 * 1000,
        ranges,
    };
    let grace = cfg.grace_secs.unwrap_or(cfg.late_secs);
    let from = NodeRef::Source {
        name: Arc::clone(&name),
    };
    tracing::info!(source = %name, buffer_secs = cfg.buffer_secs, late_secs = cfg.late_secs, "store source");
    tokio::select! {
        () = shutdown.cancelled() => return Ok(()),
        () = tokio::time::sleep(Duration::from_secs(grace)) => {}
    }

    let (tx, mut rx) = mpsc::unbounded_channel::<Done>();
    let sem = Arc::new(Semaphore::new(cfg.in_flight.max(1)));
    let mut parts: HashMap<String, Part> = HashMap::new();
    let mut ticker = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            Some(done) = rx.recv() => {
                if let Err(e) = finish(&store, &mut parts, done, &rules).await {
                    tracing::warn!(source = %name, "store source: recording a finished window: {e:#}");
                }
            }
            _ = ticker.tick() => {
                if let Err(e) = tick(&name, &rules, &store, &router, &from, &sem, &tx, &mut parts).await {
                    tracing::warn!(source = %name, "store source: {e:#}");
                }
            }
        }
    }
    Ok(())
}

/// Close what is due in every partition, and send what arrived late.
#[allow(clippy::too_many_arguments)]
async fn tick(
    name: &Arc<str>,
    rules: &Rules,
    store: &Arc<dyn StateStore>,
    router: &Arc<Router>,
    from: &NodeRef,
    sem: &Arc<Semaphore>,
    tx: &mpsc::UnboundedSender<Done>,
    parts: &mut HashMap<String, Part>,
) -> Result<()> {
    let now = now_ms() as i64;
    let marks = store.scan(Some(WINDOWS_NS), None, WATERMARK, 100_000).await?;
    for mark in marks.into_iter().filter(|e| e.key == WATERMARK) {
        let p = mark.partition;
        let StateValue::Int(watermark) = mark.value else { continue };
        let arrived = get_int(store, &p, LAST_ARRIVAL).await?.unwrap_or(now);
        // A quiet partition's watermark follows the clock.
        let wm = if now - arrived >= rules.idle {
            watermark.max(now - rules.late)
        } else {
            watermark
        };
        if !parts.contains_key(&p) {
            let closed = match get_int(store, &p, CLOSED_THROUGH).await? {
                Some(c) => c,
                None => {
                    let first = store.range(EVENTS_NS, &p, "ev:", "ev:~", 1).await?;
                    match first.first().and_then(|e| ts_of_key(&e.key)) {
                        Some(f) => f.div_euclid(rules.span) * rules.span,
                        None => continue,
                    }
                }
            };
            parts.insert(
                p.clone(),
                Part {
                    closed,
                    next: closed,
                    pending: BTreeMap::new(),
                    late_in_flight: HashSet::new(),
                },
            );
        }
        let state = parts.get_mut(&p).expect("just inserted");

        // The windows that may close end at or before this.
        let due_end = (wm - rules.late).div_euclid(rules.span) * rules.span;
        // Skip a stretch with nothing in it in one step.
        if state.pending.is_empty() && state.next < due_end {
            let first = store
                .range(EVENTS_NS, &p, &ev_bound(state.next), &ev_bound(due_end), 1)
                .await?;
            let target = match first.first().and_then(|e| ts_of_key(&e.key)) {
                Some(t) => t.div_euclid(rules.span) * rules.span,
                None => due_end,
            };
            if target > state.next {
                state.next = target;
                state.closed = target;
                set_int(store, &p, CLOSED_THROUGH, target).await?;
            }
        }

        while state.next + rules.span <= due_end {
            let start = state.next;
            let end = start + rules.span;
            if !rules.wanted(start, end) {
                state.next = end;
                state.pending.insert(start, true);
                continue;
            }
            let Ok(permit) = Arc::clone(sem).try_acquire_owned() else { break };
            state.next = end;
            // Recorded before the read, so a record stored from now on for
            // this window is marked late by the sink rather than lost.
            set_int(store, &p, READ_THROUGH, end).await?;
            let rows: Vec<String> = store
                .range(EVENTS_NS, &p, &ev_bound(start), &ev_bound(end), i64::MAX as usize)
                .await?
                .into_iter()
                .filter_map(|e| match e.value {
                    StateValue::Str(s) => Some(s),
                    _ => None,
                })
                .collect();
            if rows.is_empty() {
                state.pending.insert(start, true);
                drop(permit);
                continue;
            }
            state.pending.insert(start, false);
            let buffer = json!({"from": iso(start), "to": iso(end)});
            let frames = frames(&rows, |m| {
                m.insert("_buffer".into(), buffer.clone());
            });
            tracing::debug!(source = %name, partition = %p, from = %iso(start), records = frames.len(), "buffer closed");
            let ack: Arc<dyn Ack> = Arc::new(DoneAck {
                tx: tx.clone(),
                done: Mutex::new(Some(Done::Window {
                    part: p.clone(),
                    start,
                    _permit: permit,
                })),
            });
            router.forward(from, frames, vec![ack]).await?;
        }
        advance(store, &p, state, rules).await?;

        // Records stored after their window was read.
        let markers = store
            .scan(Some(WINDOWS_NS), Some(&p), LATE_PREFIX, 500)
            .await?
            .into_iter()
            .filter(|e| !state.late_in_flight.contains(&e.key))
            .collect::<Vec<_>>();
        if markers.is_empty() {
            continue;
        }
        let Ok(permit) = Arc::clone(sem).try_acquire_owned() else { continue };
        let mut rows = Vec::new();
        let mut keys = Vec::new();
        for m in &markers {
            keys.push(m.key.clone());
            let StateValue::Str(ev) = &m.value else { continue };
            if let Some(StateValue::Str(line)) = store.get(EVENTS_NS, &p, ev).await? {
                rows.push(line);
            }
        }
        if rows.is_empty() {
            for k in &keys {
                store.del(WINDOWS_NS, &p, k).await?;
            }
            continue;
        }
        state.late_in_flight.extend(keys.iter().cloned());
        tracing::info!(source = %name, partition = %p, records = rows.len(), "late records");
        let frames = frames(&rows, |m| {
            m.insert("_late".into(), Value::Bool(true));
        });
        let ack: Arc<dyn Ack> = Arc::new(DoneAck {
            tx: tx.clone(),
            done: Mutex::new(Some(Done::Late {
                part: p.clone(),
                markers: keys,
                _permit: permit,
            })),
        });
        router.forward(from, frames, vec![ack]).await?;
    }
    Ok(())
}

/// One frame per record, each with `mark` applied.
fn frames(rows: &[String], mark: impl Fn(&mut serde_json::Map<String, Value>)) -> Vec<BytesMut> {
    rows.iter()
        .filter_map(|line| {
            let mut v: Value = serde_json::from_str(line).ok()?;
            if let Some(m) = v.as_object_mut() {
                mark(m);
            }
            frame(&v).ok()
        })
        .collect()
}

/// `closed` moves over the done windows at the front of `pending`.
async fn advance(
    store: &Arc<dyn StateStore>,
    part: &str,
    state: &mut Part,
    rules: &Rules,
) -> Result<()> {
    let before = state.closed;
    while let Some((&start, &done)) = state.pending.first_key_value() {
        if start != state.closed || !done {
            break;
        }
        state.pending.pop_first();
        state.closed = start + rules.span;
    }
    if state.closed != before {
        set_int(store, part, CLOSED_THROUGH, state.closed).await?;
    }
    Ok(())
}

async fn finish(
    store: &Arc<dyn StateStore>,
    parts: &mut HashMap<String, Part>,
    done: Done,
    rules: &Rules,
) -> Result<()> {
    match done {
        Done::Window { part, start, .. } => {
            let Some(state) = parts.get_mut(&part) else { return Ok(()) };
            state.pending.insert(start, true);
            advance(store, &part, state, rules).await
        }
        Done::Late { part, markers, .. } => {
            for k in &markers {
                store.del(WINDOWS_NS, &part, k).await?;
            }
            if let Some(state) = parts.get_mut(&part) {
                for k in &markers {
                    state.late_in_flight.remove(k);
                }
            }
            Ok(())
        }
    }
}
