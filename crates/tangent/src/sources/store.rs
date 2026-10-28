//! The `store` source: what a `store` sink kept, read back from the state
//! store a buffer at a time as each closes. See `tangent_shared::sources::store` for
//! the rules and `crate::windows` for the bookkeeping.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use bytes::BytesMut;
use parking_lot::Mutex;
use serde_json::{json, Value};
use tangent_shared::dag::NodeRef;
use tangent_shared::sources::store::StoreSourceConfig;
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio_util::sync::CancellationToken;

use crate::router::Router;
use crate::sources::poll::duration_ms;
use crate::state::{now_ms, StateStore, StateValue};
use crate::windows::{
    ev_bound, get_int, iso, set_int, ts_of_key, CLOSED_THROUGH, FIRST, LAST_ARRIVAL, LATE_PREFIX,
    READ_THROUGH, WATERMARK, WINDOWS_NS,
};
use crate::worker::Ack;

/// Rows read per range call.
const PAGE: usize = 5_000;

struct Rules {
    span: i64,
    late: i64,
    idle: i64,
    /// Nothing before this is judged, on a partition's first sight.
    since: Option<i64>,
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

fn ms(s: &str, what: &str) -> Result<i64> {
    duration_ms(s).ok_or_else(|| anyhow!("store source: {what} {s:?} is not a duration like 15m"))
}

pub async fn run_consumer(
    name: Arc<str>,
    cfg: StoreSourceConfig,
    router: Arc<Router>,
    store: Arc<dyn StateStore>,
    shutdown: CancellationToken,
) -> Result<()> {
    let span = ms(&cfg.buffer, "buffer")?;
    if span <= 0 {
        return Err(anyhow!("store source: buffer must be positive"));
    }
    let late = ms(&cfg.late, "late")?;
    let idle = cfg.idle.as_deref().map(|s| ms(s, "idle")).transpose()?.unwrap_or(late);
    let grace = cfg.grace.as_deref().map(|s| ms(s, "grace")).transpose()?.unwrap_or(late);
    let since = match cfg.since.as_deref().map(str::trim).filter(|s| !s.is_empty()) {
        Some(s) => Some(
            chrono::DateTime::parse_from_rfc3339(s)
                .map_err(|e| anyhow!("store source: since {s:?}: {e}"))?
                .timestamp_millis(),
        ),
        None => None,
    };
    let rules = Rules { span, late, idle, since };
    let from = NodeRef::Source {
        name: Arc::clone(&name),
    };
    tracing::info!(
        source = %name, buffer = %cfg.buffer, late = %cfg.late, idle_ms = idle, grace_ms = grace,
        "store source: the stored records, a buffer at a time"
    );
    tokio::select! {
        () = shutdown.cancelled() => return Ok(()),
        () = tokio::time::sleep(Duration::from_millis(grace.max(0) as u64)) => {}
    }

    let (tx, mut rx) = mpsc::unbounded_channel::<Done>();
    let sem = Arc::new(Semaphore::new(cfg.in_flight.max(1)));
    let mut parts: HashMap<String, Part> = HashMap::new();
    let mut ticker = tokio::time::interval(Duration::from_secs(cfg.interval_secs.max(1)));
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            Some(done) = rx.recv() => {
                if let Err(e) = finish(&store, &mut parts, done, &rules).await {
                    tracing::warn!(source = %name, "store source: recording a finished window: {e:#}");
                }
            }
            _ = ticker.tick() => {
                if let Err(e) = tick(&name, &cfg, &rules, &store, &router, &from, &sem, &tx, &mut parts).await {
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
    cfg: &StoreSourceConfig,
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
                None => match get_int(store, &p, FIRST).await? {
                    Some(f) => f.max(rules.since.unwrap_or(i64::MIN)).div_euclid(rules.span) * rules.span,
                    None => continue,
                },
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
                .range(&cfg.namespace, &p, &ev_bound(state.next), &ev_bound(due_end), 1)
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
            let Ok(permit) = Arc::clone(sem).try_acquire_owned() else { break };
            let start = state.next;
            let end = start + rules.span;
            state.next = end;
            // Recorded before the read, so a record stored from now on for
            // this window is marked late by the sink rather than lost.
            set_int(store, &p, READ_THROUGH, end).await?;
            let rows = read(store, &cfg.namespace, &p, start, end).await?;
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
            if let Some(StateValue::Str(line)) = store.get(&cfg.namespace, &p, ev).await? {
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

/// A window's records, in time order.
async fn read(
    store: &Arc<dyn StateStore>,
    ns: &str,
    part: &str,
    start: i64,
    end: i64,
) -> Result<Vec<String>> {
    let end_key = ev_bound(end);
    let mut from = ev_bound(start);
    let mut out = Vec::new();
    loop {
        let page = store.range(ns, part, &from, &end_key, PAGE).await?;
        let n = page.len();
        if let Some(last) = page.last() {
            // The next key after `last`: every key has the same shape, so
            // anything greater differs at a character `!` sorts below.
            from = format!("{}!", last.key);
        }
        out.extend(page.into_iter().filter_map(|e| match e.value {
            StateValue::Str(s) => Some(s),
            _ => None,
        }));
        if n < PAGE {
            return Ok(out);
        }
    }
}

/// One frame per record, each with `mark` applied.
fn frames(rows: &[String], mark: impl Fn(&mut serde_json::Map<String, Value>)) -> Vec<BytesMut> {
    rows.iter()
        .filter_map(|line| {
            let mut v: Value = serde_json::from_str(line).ok()?;
            if let Some(m) = v.as_object_mut() {
                mark(m);
            }
            let mut b = BytesMut::from(serde_json::to_vec(&v).ok()?.as_slice());
            b.extend_from_slice(b"\n");
            Some(b)
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
