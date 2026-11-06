//! Grouping in front of a plugin, as Vector's `reduce` groups, without the
//! merge: records reaching the plugin wait in a group per value of its
//! `group_by` fields, and the plugin gets a group at a time. One task per
//! grouped plugin owns its groups, so the workers that hand it records share
//! nothing.
//!
//! A group closes when it holds `max_events` records, when no record has
//! joined it for `expire_after_ms` of wall-clock time, or `max_age_ms` after
//! it opened, however often records join it; at the end of input every open
//! group closes. A closed group goes whole to the plugin's own
//! workers, and one group per key is out at a time: the next waits until the
//! last is through the DAG. With `history`, a key's latest records are kept
//! in the state store and go again, marked `"_context": true`, ahead of its
//! next group.
//!
//! Groups live in memory: records are acked once they are in one, and a
//! crash loses the groups still open, as with Vector's `reduce`.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use parking_lot::Mutex;
use serde_json::Value;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::record;
use crate::state::{StateStore, StateValue};
use crate::wasm::host::JsonLogView;
use crate::worker::Ack;

/// The namespace a grouped plugin's history is kept in, under
/// `<plugin>:<group key>`.
pub const GROUPS_NS: &str = "groups";

/// How a plugin groups what reaches it.
#[derive(Clone, Debug)]
pub struct GroupConfig {
    pub group_by: Vec<String>,
    pub max_events: Option<usize>,
    pub expire_after: Duration,
    pub max_age: Option<Duration>,
    pub history: usize,
}

/// A closed group on its way to the grouped plugin's workers.
pub struct Group {
    /// The grouped plugin, where the group's walk of the DAG starts.
    pub node: Arc<str>,
    /// The key's history first, marked `"_context": true`, then the group.
    pub records: Vec<JsonLogView>,
    /// Fires once the group is through the DAG.
    pub done: Arc<dyn Ack>,
}

/// Where closed groups go: the grouped plugin's worker pool.
#[async_trait]
pub trait Dispatch: Send + Sync {
    async fn dispatch(&self, group: Group) -> Result<()>;
}

enum Msg {
    Records(Vec<JsonLogView>),
    /// Close every open group; answer once none is open, waiting or out.
    Flush(oneshot::Sender<()>),
}

/// A grouped plugin's grouping task, as the workers hand it records.
#[derive(Clone)]
pub struct Grouper {
    tx: mpsc::Sender<Msg>,
}

impl Grouper {
    pub async fn add(&self, records: Vec<JsonLogView>) -> Result<()> {
        self.tx
            .send(Msg::Records(records))
            .await
            .map_err(|_| anyhow!("the grouping task is gone"))
    }

    /// Closes every open group and waits until each, and any group waiting
    /// behind one, is through the DAG: the end of input.
    pub async fn flush(&self) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.tx
            .send(Msg::Flush(tx))
            .await
            .map_err(|_| anyhow!("the grouping task is gone"))?;
        rx.await.map_err(|_| anyhow!("the grouping task is gone"))
    }
}

/// Starts the grouping task for plugin `node`: closed groups go to `out`,
/// history to `store`.
pub fn spawn(
    node: Arc<str>,
    cfg: GroupConfig,
    out: Arc<dyn Dispatch>,
    store: Arc<dyn StateStore>,
) -> (Grouper, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(1024);
    let (done_tx, done_rx) = mpsc::unbounded_channel();
    let task = Task {
        node,
        cfg,
        out,
        store,
        keys: HashMap::new(),
        done_tx,
        flushing: Vec::new(),
    };
    (Grouper { tx }, tokio::spawn(task.run(rx, done_rx)))
}

/// A group still taking records.
struct Open {
    records: Vec<JsonLogView>,
    /// When its first record joined: `max_age` counts from here.
    opened: Instant,
    /// When its latest record joined: `expire_after` counts from here.
    last: Instant,
}

#[derive(Default)]
struct Key {
    open: Option<Open>,
    /// Closed groups waiting for the one out to be through.
    waiting: VecDeque<Vec<JsonLogView>>,
    /// A group of this key is on its way through the DAG.
    out: bool,
}

struct Task {
    node: Arc<str>,
    cfg: GroupConfig,
    out: Arc<dyn Dispatch>,
    store: Arc<dyn StateStore>,
    keys: HashMap<Arc<str>, Key>,
    /// Where a group's walk reports it is through, with the group's records.
    done_tx: mpsc::UnboundedSender<(Arc<str>, Vec<JsonLogView>)>,
    flushing: Vec<oneshot::Sender<()>>,
}

impl Task {
    async fn run(
        mut self,
        mut rx: mpsc::Receiver<Msg>,
        mut done_rx: mpsc::UnboundedReceiver<(Arc<str>, Vec<JsonLogView>)>,
    ) {
        let mut every = self.cfg.expire_after.min(Duration::from_secs(1));
        if let Some(age) = self.cfg.max_age {
            every = every.min(age);
        }
        let mut tick = tokio::time::interval(every);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                msg = rx.recv() => match msg {
                    Some(Msg::Records(records)) => {
                        for r in records {
                            self.add(r).await;
                        }
                    }
                    Some(Msg::Flush(answer)) => {
                        let open: Vec<Arc<str>> = self
                            .keys
                            .iter()
                            .filter(|(_, k)| k.open.is_some())
                            .map(|(key, _)| Arc::clone(key))
                            .collect();
                        for key in open {
                            self.close(key).await;
                        }
                        self.flushing.push(answer);
                        self.answer_if_idle();
                    }
                    None => break,
                },
                Some((key, records)) = done_rx.recv() => self.done(key, records).await,
                _ = tick.tick() => self.expire().await,
            }
        }
    }

    /// The record's key: its `group_by` values as text.
    fn key_of(&self, r: &JsonLogView) -> Arc<str> {
        let parts: Vec<String> = self
            .cfg
            .group_by
            .iter()
            .map(|path| {
                r.lookup(path)
                    .and_then(JsonLogView::to_scalar)
                    .map(record::scalar_text)
                    .unwrap_or_default()
            })
            .collect();
        Arc::from(parts.join("\u{1f}"))
    }

    async fn add(&mut self, r: JsonLogView) {
        let key = self.key_of(&r);
        let k = self.keys.entry(Arc::clone(&key)).or_default();
        let now = Instant::now();
        let open = k.open.get_or_insert_with(|| Open {
            records: Vec::new(),
            opened: now,
            last: now,
        });
        open.records.push(r);
        open.last = now;
        if self
            .cfg
            .max_events
            .is_some_and(|max| open.records.len() >= max)
        {
            self.close(key).await;
        }
    }

    /// Closes each group quiet for `expire_after`, or open for `max_age`.
    async fn expire(&mut self) {
        let cfg = &self.cfg;
        let due: Vec<Arc<str>> = self
            .keys
            .iter()
            .filter(|(_, k)| {
                k.open.as_ref().is_some_and(|o| {
                    o.last.elapsed() >= cfg.expire_after
                        || cfg.max_age.is_some_and(|age| o.opened.elapsed() >= age)
                })
            })
            .map(|(key, _)| Arc::clone(key))
            .collect();
        for key in due {
            self.close(key).await;
        }
    }

    async fn close(&mut self, key: Arc<str>) {
        let Some(k) = self.keys.get_mut(&key) else {
            return;
        };
        let Some(Open { records, .. }) = k.open.take() else {
            return;
        };
        if k.out {
            k.waiting.push_back(records);
        } else {
            self.send(key, records).await;
        }
    }

    async fn send(&mut self, key: Arc<str>, records: Vec<JsonLogView>) {
        let mut all = self.history(&key).await;
        all.extend(records.iter().cloned());
        let done = Arc::new(Through {
            tx: self.done_tx.clone(),
            group: Mutex::new(Some((Arc::clone(&key), records))),
        });
        let group = Group {
            node: Arc::clone(&self.node),
            records: all,
            done,
        };
        // Out from here: the group's ack, fired or dropped, frees the key.
        if let Some(k) = self.keys.get_mut(&key) {
            k.out = true;
        }
        if let Err(e) = self.out.dispatch(group).await {
            tracing::warn!(plugin = %self.node, error = %e, "a group could not reach its workers; dropped");
        }
    }

    /// A group of `key` is through the DAG, or was dropped on the way (no
    /// records then, and nothing to remember).
    async fn done(&mut self, key: Arc<str>, records: Vec<JsonLogView>) {
        if !records.is_empty() {
            self.remember(&key, &records).await;
        }
        let next = match self.keys.get_mut(&key) {
            Some(k) => {
                k.out = false;
                k.waiting.pop_front()
            }
            None => None,
        };
        match next {
            Some(records) => self.send(Arc::clone(&key), records).await,
            None => {
                if self
                    .keys
                    .get(&key)
                    .is_some_and(|k| k.open.is_none() && !k.out)
                {
                    self.keys.remove(&key);
                }
            }
        }
        self.answer_if_idle();
    }

    fn answer_if_idle(&mut self) {
        if self.flushing.is_empty() || !self.keys.is_empty() {
            return;
        }
        for answer in self.flushing.drain(..) {
            let _ = answer.send(());
        }
    }

    fn history_key(&self, key: &str) -> String {
        format!("{}:{key}", self.node)
    }

    /// The key's history, each record marked `"_context": true`.
    async fn history(&self, key: &str) -> Vec<JsonLogView> {
        if self.cfg.history == 0 {
            return Vec::new();
        }
        let kept = match self.store.get(GROUPS_NS, &self.history_key(key)).await {
            Ok(Some(StateValue::Json(Value::Array(kept)))) => kept,
            Ok(_) => return Vec::new(),
            Err(e) => {
                tracing::warn!(plugin = %self.node, error = %e, "reading a group's history");
                return Vec::new();
            }
        };
        kept.into_iter()
            .filter_map(|mut v| {
                v.as_object_mut()?
                    .insert("_context".into(), Value::Bool(true));
                let bytes = serde_json::to_vec(&v).ok()?;
                JsonLogView::from_bytes(bytes.as_slice().into()).ok()
            })
            .collect()
    }

    /// The key's latest `history` records, the group's added: only this task
    /// writes them, so the read and the write need no transaction.
    async fn remember(&self, key: &str, records: &[JsonLogView]) {
        let n = self.cfg.history;
        if n == 0 {
            return;
        }
        let hk = self.history_key(key);
        let mut kept = match self.store.get(GROUPS_NS, &hk).await {
            Ok(Some(StateValue::Json(Value::Array(kept)))) => kept,
            _ => Vec::new(),
        };
        kept.extend(
            records
                .iter()
                .filter_map(|r| serde_json::from_str::<Value>(&r.to_json()).ok()),
        );
        if kept.len() > n {
            kept.drain(..kept.len() - n);
        }
        if let Err(e) = self
            .store
            .set(
                GROUPS_NS,
                &hk,
                &StateValue::Json(Value::Array(kept)),
                Some(0),
            )
            .await
        {
            tracing::warn!(plugin = %self.node, error = %e, "keeping a group's history");
        }
    }
}

/// A group's ack: once it is through the DAG, its key may send the next.
/// Dropped without firing (its walk was stopped by a signal, or failed),
/// it frees the key all the same, remembering nothing.
struct Through {
    tx: mpsc::UnboundedSender<(Arc<str>, Vec<JsonLogView>)>,
    group: Mutex<Option<(Arc<str>, Vec<JsonLogView>)>>,
}

#[async_trait]
impl Ack for Through {
    async fn ack(&self) -> Result<()> {
        if let Some(group) = self.group.lock().take() {
            let _ = self.tx.send(group);
        }
        Ok(())
    }
}

impl Drop for Through {
    fn drop(&mut self) {
        if let Some((key, _)) = self.group.get_mut().take() {
            let _ = self.tx.send((key, Vec::new()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::state::lru::LruStore;
    use tangent_shared::runtime::StateConfig;

    /// Each group handed over: its records, and its ack until it is fired
    /// or dropped.
    type Handed = Vec<(Vec<Value>, Option<Arc<dyn Ack>>)>;

    /// Records the groups it is handed; acks them when told to.
    #[derive(Default)]
    struct Out {
        groups: Mutex<Handed>,
    }

    #[async_trait]
    impl Dispatch for Out {
        async fn dispatch(&self, g: Group) -> Result<()> {
            let records = g
                .records
                .iter()
                .map(|r| serde_json::from_str(&r.to_json()).unwrap())
                .collect();
            self.groups.lock().push((records, Some(g.done)));
            Ok(())
        }
    }

    impl Out {
        fn ids(&self) -> Vec<Vec<i64>> {
            self.groups
                .lock()
                .iter()
                .map(|(rs, _)| rs.iter().map(|r| r["id"].as_i64().unwrap()).collect())
                .collect()
        }
        async fn ack(&self, i: usize) {
            let done = self.groups.lock()[i].1.take().expect("not fired yet");
            done.ack().await.unwrap();
        }
        /// As a walk a signal stopped: the ack goes, unfired.
        fn drop_ack(&self, i: usize) {
            self.groups.lock()[i].1.take();
        }
    }

    fn rec(actor: &str, id: i64) -> JsonLogView {
        let line = format!("{{\"actor\":\"{actor}\",\"id\":{id}}}");
        JsonLogView::from_bytes(line.as_str().into()).unwrap()
    }

    fn start(max: Option<usize>, expire_ms: u64, history: usize) -> (Grouper, Arc<Out>) {
        start_with(GroupConfig {
            group_by: vec!["actor".into()],
            max_events: Some(max.unwrap_or(1000)),
            expire_after: Duration::from_millis(expire_ms),
            max_age: None,
            history,
        })
    }

    fn start_with(cfg: GroupConfig) -> (Grouper, Arc<Out>) {
        let out = Arc::new(Out::default());
        let store: Arc<dyn StateStore> = Arc::new(LruStore::new(StateConfig::default()).unwrap());
        let (g, _task) = spawn(
            Arc::from("p"),
            cfg,
            Arc::clone(&out) as Arc<dyn Dispatch>,
            store,
        );
        (g, out)
    }

    async fn settle() {
        tokio::time::sleep(Duration::from_millis(30)).await;
    }

    /// A group closes at `max_events`, per key.
    #[tokio::test]
    async fn a_group_closes_when_full() {
        let (g, out) = start(Some(2), 60_000, 0);
        g.add(vec![rec("a", 1), rec("b", 2), rec("a", 3)])
            .await
            .unwrap();
        settle().await;
        assert_eq!(out.ids(), [vec![1, 3]], "b's one record waits");
    }

    /// A group closes once nothing has joined it for `expire_after_ms`.
    #[tokio::test]
    async fn a_quiet_group_closes() {
        let (g, out) = start(None, 50, 0);
        g.add(vec![rec("a", 1)]).await.unwrap();
        settle().await;
        assert!(out.ids().is_empty(), "not yet");
        tokio::time::sleep(Duration::from_millis(120)).await;
        assert_eq!(out.ids(), [vec![1]]);
    }

    /// A group closes `max_age_ms` after it opened, though its key never goes
    /// quiet.
    #[tokio::test]
    async fn a_busy_group_closes_at_max_age() {
        let (g, out) = start_with(GroupConfig {
            group_by: vec!["actor".into()],
            max_events: None,
            expire_after: Duration::from_secs(60),
            max_age: Some(Duration::from_millis(50)),
            history: 0,
        });
        for id in 1..=20 {
            g.add(vec![rec("a", id)]).await.unwrap();
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let ids = out.ids();
        assert_eq!(ids.len(), 1, "the next waits for the first to be through");
        assert!(
            ids[0].len() < 20,
            "closed while records kept joining: {ids:?}"
        );
    }

    /// A key's next group waits until the last is through.
    #[tokio::test]
    async fn one_group_per_key_is_out() {
        let (g, out) = start(Some(1), 60_000, 0);
        g.add(vec![rec("a", 1), rec("a", 2), rec("b", 3)])
            .await
            .unwrap();
        settle().await;
        assert_eq!(out.ids(), [vec![1], vec![3]], "a's second waits");
        out.ack(0).await;
        settle().await;
        assert_eq!(out.ids(), [vec![1], vec![3], vec![2]]);
    }

    /// A key's history goes ahead of its next group, marked as context, and
    /// holds only the latest `history` records.
    #[tokio::test]
    async fn history_comes_back_as_context() {
        let (g, out) = start(Some(2), 60_000, 3);
        g.add(vec![rec("a", 1), rec("a", 2)]).await.unwrap();
        settle().await;
        out.ack(0).await;
        g.add(vec![rec("a", 3), rec("a", 4)]).await.unwrap();
        settle().await;
        out.ack(1).await;
        g.add(vec![rec("a", 5), rec("a", 6)]).await.unwrap();
        settle().await;
        assert_eq!(
            out.ids(),
            [vec![1, 2], vec![1, 2, 3, 4], vec![2, 3, 4, 5, 6]]
        );
        let groups = out.groups.lock();
        let last = &groups[2].0;
        assert!(last[..3].iter().all(|r| r["_context"] == true), "{last:?}");
        assert!(last[3..].iter().all(|r| r.get("_context").is_none()));
    }

    /// A group whose walk ended without firing its ack (a signal, a failed
    /// worker) frees its key: the next goes, and a flush can answer.
    #[tokio::test]
    async fn a_group_dropped_unfired_frees_its_key() {
        let (g, out) = start(Some(1), 60_000, 2);
        g.add(vec![rec("a", 1), rec("a", 2)]).await.unwrap();
        settle().await;
        assert_eq!(out.ids(), [vec![1]], "a's second waits");
        out.drop_ack(0);
        settle().await;
        assert_eq!(
            out.ids(),
            [vec![1], vec![2]],
            "no history from a dropped group"
        );
        let flushed = {
            let g = g.clone();
            tokio::spawn(async move { g.flush().await })
        };
        out.ack(1).await;
        flushed.await.unwrap().unwrap();
    }

    /// A flush closes every open group and answers once each is through.
    #[tokio::test]
    async fn a_flush_closes_everything_and_waits() {
        let (g, out) = start(None, 60_000, 0);
        g.add(vec![rec("a", 1), rec("b", 2)]).await.unwrap();
        let flushed = {
            let g = g.clone();
            tokio::spawn(async move { g.flush().await })
        };
        settle().await;
        assert_eq!(out.ids().len(), 2);
        assert!(!flushed.is_finished(), "waits for the groups to be through");
        out.ack(0).await;
        out.ack(1).await;
        flushed.await.unwrap().unwrap();
    }
}
