use ahash::AHashMap as HashMap;
use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use std::collections::{BTreeMap, VecDeque};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::Path;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use tangent_shared::dag::NodeRef;
use tangent_shared::inference::InferenceConfig;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::task::JoinHandle;
use tokio::time::{self, Instant as TokioInstant};
use wasmtime::component::{Component, Resource};

use crate::inference::{lines, InferenceNode};
use crate::router::{RefCountAck, Router};
use crate::wasm::host::JsonLogView;
use crate::wasm::{self, mapper::Mappers, probe::eval_selector};
use crate::{
    CONSUMER_BYTES_TOTAL, CONSUMER_OBJECTS_TOTAL, GUEST_BYTES_TOTAL, GUEST_LATENCY,
    PLUGIN_TRAPS_TOTAL,
};

#[async_trait]
pub trait Ack: Send + Sync {
    async fn ack(&self) -> Result<()>;
}

/// A record on its way to a worker: raw from a source, parsed when handed over.
pub enum Payload {
    Raw(BytesMut),
    Parsed(JsonLogView),
}

impl Payload {
    pub fn size(&self) -> usize {
        match self {
            Payload::Raw(b) => b.len(),
            Payload::Parsed(view) => view.byte_len(),
        }
    }

    /// The record as bytes, serialized again if it was parsed.
    pub fn into_bytes(self) -> BytesMut {
        match self {
            Payload::Raw(b) => b,
            Payload::Parsed(view) => BytesMut::from(view.to_json().as_bytes()),
        }
    }
}

pub struct Record {
    pub payload: Payload,
    pub ack: Option<Arc<dyn Ack>>,
    /// Its source, or the partitioned node it was handed over to run.
    pub at: NodeRef,
}

/// Takes `n` records off the DAG's in-flight count when dropped: after the
/// batch has been through the DAG, or lost to an error.
struct Done(Arc<AtomicUsize>, usize);

impl Drop for Done {
    fn drop(&mut self) {
        self.0.fetch_sub(self.1, Ordering::AcqRel);
    }
}

/// The worker that owns a scope: a stable function of it, so one guest sees
/// a scope's records in order and nothing races on its state.
fn worker_for(scope: &str, workers: usize) -> usize {
    let mut h = DefaultHasher::new();
    scope.hash(&mut h);
    (h.finish() % workers as u64) as usize
}

/// The records as one NDJSON frame, for a sink.
fn frame_of(views: &[JsonLogView]) -> BytesMut {
    let mut frame = BytesMut::new();
    for lv in views {
        frame.extend_from_slice(lv.to_json().as_bytes());
        frame.extend_from_slice(b"\n");
    }
    frame
}

/// The records in a node's output frames; output that is not JSON is dropped.
fn parse(frames: &[BytesMut]) -> Vec<JsonLogView> {
    let mut views = Vec::new();
    for frame in frames {
        for line in lines(frame) {
            let mut b = BytesMut::with_capacity(line.len());
            b.extend_from_slice(line);
            match JsonLogView::from_bytes(b) {
                Ok(lv) => views.push(lv),
                Err(e) => tracing::warn!(error = %e, "node output is not JSON; dropping"),
            }
        }
    }
    views
}

/// A node's output on its way to the node's outputs. A sink takes the frames
/// as they are; a plugin or a model needs the records, so frames are parsed
/// only then, and once.
enum Hop {
    Frames(Vec<BytesMut>),
    Views(Vec<JsonLogView>),
}

/// The frames as one, for a sink; a single frame moves.
fn join_frames(mut frames: Vec<BytesMut>) -> BytesMut {
    if frames.len() == 1 {
        return frames.pop().expect("one frame");
    }
    let mut joined = BytesMut::with_capacity(frames.iter().map(|f| f.len() + 1).sum());
    for f in &frames {
        joined.extend_from_slice(f);
        if !f.ends_with(b"\n") {
            joined.extend_from_slice(b"\n");
        }
    }
    joined
}

/// A frame for a sink: its name, key prefix, and the bytes.
type Delivery = (Arc<str>, Option<Arc<str>>, BytesMut);

/// One frame to each of `sinks`: the last takes it, the others a copy.
fn deliver(
    deliveries: &mut Vec<Delivery>,
    sinks: Vec<(Arc<str>, Option<Arc<str>>)>,
    frame: BytesMut,
) {
    let mut frame = Some(frame);
    let last = sinks.len().saturating_sub(1);
    for (i, (name, key_prefix)) in sinks.into_iter().enumerate() {
        let f = if i == last {
            frame.take().expect("frame")
        } else {
            frame.as_ref().expect("frame").clone()
        };
        deliveries.push((name, key_prefix, f));
    }
}

/// A worker hosts every plugin and every inference node, and walks the DAG
/// for each batch it is handed: a record enters at its source and goes to
/// each of the node's outputs in turn, a plugin's or a model's output being
/// the next node's input, until the sinks. A partitioned node runs a record
/// on the worker that owns its scope; this one hands it there.
pub struct Worker {
    id: usize,
    rx: mpsc::Receiver<Record>,
    mappers: Mappers,
    /// What the plugins were instantiated from, to instantiate one again
    /// after it traps.
    engine: wasm::engine::WasmEngine,
    components: Vec<(Arc<str>, Component)>,
    inference: HashMap<Arc<str>, InferenceNode>,
    batch_max_size: usize,
    batch_max_age: Duration,
    router: Arc<Router>,
    /// Records anywhere between a source and a sink, shared with the router.
    in_flight: Arc<AtomicUsize>,
    /// Records another worker handed this one. Unbounded, so a worker never
    /// blocks on a hand-off and two workers cannot wait on each other.
    handoff_rx: mpsc::UnboundedReceiver<Record>,
    /// Every worker's hand-off queue, by worker index.
    handoffs: Vec<mpsc::UnboundedSender<Record>>,
}

impl Worker {
    pub async fn run(mut self) -> Result<()> {
        let mut batch = Vec::<Record>::new();
        let mut acks: Vec<Arc<dyn Ack>> = Vec::with_capacity(1024);
        let mut total_size = 0usize;

        let mut deadline = TokioInstant::now() + self.batch_max_age;
        let sleeper = time::sleep_until(deadline);
        tokio::pin!(sleeper);

        enum Next {
            Record(Record),
            Closed,
            Tick,
        }

        loop {
            let next = tokio::select! {
                maybe = self.rx.recv() => match maybe {
                    Some(rec) => Next::Record(rec),
                    None => Next::Closed,
                },
                // Never None: this worker holds a sender to its own queue.
                maybe = self.handoff_rx.recv() => match maybe {
                    Some(rec) => Next::Record(rec),
                    None => Next::Tick,
                },
                () = &mut sleeper => Next::Tick,
            };
            match next {
                Next::Closed => {
                    if !batch.is_empty() {
                        let _ = self
                            .flush_batch(&mut batch, &mut acks, &mut total_size)
                            .await;
                    }
                    break;
                }
                Next::Tick => {
                    if !batch.is_empty() {
                        self.flush_batch(&mut batch, &mut acks, &mut total_size)
                            .await?;
                    }
                    deadline = TokioInstant::now() + self.batch_max_age;
                    sleeper.as_mut().reset(deadline);
                }
                Next::Record(mut rec) => {
                    if batch.is_empty() {
                        deadline = TokioInstant::now() + self.batch_max_age;
                        sleeper.as_mut().reset(deadline);
                    }

                    let payload_len = rec.payload.size();

                    if total_size + payload_len > self.batch_max_size {
                        self.flush_batch(&mut batch, &mut acks, &mut total_size)
                            .await?;
                        deadline = TokioInstant::now() + self.batch_max_age;
                        sleeper.as_mut().reset(deadline);
                    }

                    if payload_len > self.batch_max_size && batch.is_empty() {
                        let mut single_ack: Vec<Arc<dyn Ack>> =
                            rec.ack.take().into_iter().collect();
                        let mut single = vec![rec];
                        self.flush_batch(&mut single, &mut single_ack, &mut total_size)
                            .await?;
                        deadline = TokioInstant::now() + self.batch_max_age;
                        sleeper.as_mut().reset(deadline);
                    } else {
                        total_size += payload_len;
                        if let Some(a) = rec.ack.take() {
                            acks.push(a);
                        }
                        batch.push(rec);
                    }
                }
            }
        }

        Ok(())
    }

    /// A batch through the DAG. Each node's records go to each of its
    /// outputs: a plugin or a model turns them into the next node's records,
    /// a sink's share is collected and delivered when the walk is done, so
    /// the batch's acks cover every delivery.
    pub async fn flush_batch(
        &mut self,
        batch: &mut Vec<Record>,
        acks: &mut Vec<Arc<dyn Ack>>,
        total_size: &mut usize,
    ) -> Result<()> {
        if batch.is_empty() {
            tracing::warn!("flushed empty batch");
            return Ok(());
        }
        let _done = Done(Arc::clone(&self.in_flight), batch.len());

        // Parsed, and grouped by the node each record is at.
        let mut entries: Vec<(NodeRef, Vec<JsonLogView>)> = Vec::new();
        for Record { payload, at, .. } in batch.drain(..) {
            let lv = match payload {
                Payload::Parsed(lv) => lv,
                Payload::Raw(b) => match JsonLogView::from_bytes(b) {
                    Ok(lv) => lv,
                    Err(e) => {
                        tracing::warn!(error = %e, "record is not valid JSON; dropping");
                        continue;
                    }
                },
            };
            match entries.iter_mut().find(|(node, _)| *node == at) {
                Some((_, views)) => views.push(lv),
                None => entries.push((at, vec![lv])),
            }
        }

        let router = Arc::clone(&self.router);
        let workers = self.handoffs.len();
        let mut queue: VecDeque<(NodeRef, Hop)> = VecDeque::new();
        for (node, views) in entries {
            if matches!(node, NodeRef::Source { .. }) {
                queue.push_back((node, Hop::Views(views)));
                continue;
            }
            // Handed over: this worker owns their scope at `node`.
            let out = self.run_node(&node, &views).await?;
            if !out.is_empty() {
                queue.push_back((node, Hop::Frames(out)));
            }
        }

        let mut deliveries: Vec<Delivery> = Vec::new();
        let mut handoffs: Vec<(usize, NodeRef, JsonLogView)> = Vec::new();
        while let Some((node, hop)) = queue.pop_front() {
            let outs = router.outs(&node);
            let sinks: Vec<(Arc<str>, Option<Arc<str>>)> = outs
                .iter()
                .filter_map(|to| match to {
                    NodeRef::Sink { name, key_prefix } => {
                        Some((Arc::clone(name), key_prefix.clone()))
                    }
                    _ => None,
                })
                .collect();
            let nodes: Vec<&NodeRef> = outs
                .iter()
                .filter(|to| matches!(to, NodeRef::Plugin { .. } | NodeRef::Inference { .. }))
                .collect();
            if let Some(bad) = outs.iter().find(|to| matches!(to, NodeRef::Source { .. })) {
                tracing::warn!(from = ?node, to = ?bad, "a source is not an output; dropping");
            }

            // Sinks take the node's output as it is: no parse, one copy at most.
            if nodes.is_empty() {
                if !sinks.is_empty() {
                    let frame = match hop {
                        Hop::Frames(frames) => join_frames(frames),
                        Hop::Views(views) => frame_of(&views),
                    };
                    deliver(&mut deliveries, sinks, frame);
                }
                continue;
            }
            if !sinks.is_empty() {
                let frame = match &hop {
                    Hop::Frames(frames) => join_frames(frames.clone()),
                    Hop::Views(views) => frame_of(views),
                };
                deliver(&mut deliveries, sinks, frame);
            }

            // A plugin or a model needs the records. A partitioned node runs
            // here the records this worker owns; the rest go to their owners.
            let views = match hop {
                Hop::Views(views) => views,
                Hop::Frames(frames) => parse(&frames),
            };
            if views.is_empty() {
                continue;
            }
            for to in nodes {
                let out = if workers <= 1 || !router.partitioned(to) {
                    self.run_node(to, &views).await?
                } else {
                    let mut mine = Vec::new();
                    for lv in &views {
                        let owner = worker_for(&router.scope(to, lv), workers);
                        if owner == self.id {
                            mine.push(lv.clone());
                        } else {
                            handoffs.push((owner, to.clone(), lv.clone()));
                        }
                    }
                    if mine.is_empty() {
                        continue;
                    }
                    self.run_node(to, &mine).await?
                };
                if !out.is_empty() {
                    queue.push_back((to.clone(), Hop::Frames(out)));
                }
            }
        }

        // The batch's acks cover every sink delivery and every record handed
        // to another worker, whose own walk acks it in turn.
        let upstream = std::mem::take(acks);
        let outstanding = deliveries.len() + handoffs.len();
        if outstanding == 0 {
            for a in upstream {
                let _ = a.ack().await;
            }
        } else {
            let shared: Arc<dyn Ack> = Arc::new(RefCountAck::new(upstream, outstanding));
            for (name, key_prefix, frame) in deliveries {
                router
                    .sinks()
                    .enqueue(name, key_prefix, frame, vec![Arc::clone(&shared)])
                    .await?;
            }
            for (owner, at, view) in handoffs {
                // Counted until the owner's walk is done with it.
                self.in_flight.fetch_add(1, Ordering::AcqRel);
                let rec = Record {
                    payload: Payload::Parsed(view),
                    ack: Some(Arc::clone(&shared)),
                    at,
                };
                if self.handoffs[owner].send(rec).is_err() {
                    self.in_flight.fetch_sub(1, Ordering::AcqRel);
                    tracing::warn!(worker = owner, "worker is gone; record not acked");
                }
            }
        }

        batch.clear();
        *total_size = 0;
        Ok(())
    }

    async fn run_node(&mut self, node: &NodeRef, views: &[JsonLogView]) -> Result<Vec<BytesMut>> {
        match node {
            NodeRef::Plugin { name } => {
                let scopes: Vec<Arc<str>> =
                    views.iter().map(|lv| self.router.scope(node, lv)).collect();
                self.run_plugin(name, views, &scopes).await
            }
            NodeRef::Inference { name } => run_inference(&self.inference, name, views).await,
            _ => unreachable!("only plugins and inference nodes run"),
        }
    }

    /// `views` through plugin `name`: one guest call per scope, with the
    /// records whose probe matched and the plugin's state scoped to that
    /// scope. The output frames, scopes in first-seen order.
    async fn run_plugin(
        &mut self,
        name: &Arc<str>,
        views: &[JsonLogView],
        scopes: &[Arc<str>],
    ) -> Result<Vec<BytesMut>> {
        let Some(idx) = self
            .mappers
            .mappers
            .iter()
            .position(|m| m.cfg_name == *name)
        else {
            bail!("unknown plugin {name}");
        };

        let mut groups: Vec<(Arc<str>, Vec<JsonLogView>, usize)> = Vec::new();
        {
            let m = &self.mappers.mappers[idx];
            for (lv, scope) in views.iter().zip(scopes) {
                if !m.selectors.iter().any(|s| eval_selector(s, lv)) {
                    continue;
                }
                match groups.iter_mut().find(|(sc, _, _)| sc == scope) {
                    Some((_, g, bytes)) => {
                        g.push(lv.clone());
                        *bytes += lv.byte_len();
                    }
                    None => groups.push((Arc::clone(scope), vec![lv.clone()], lv.byte_len())),
                }
            }
        }
        if groups.is_empty() {
            tracing::debug!(plugin = %name, "no record matched the probe");
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        for (scope, group, bytes) in groups {
            let m = &mut self.mappers.mappers[idx];
            m.store.data_mut().partition = Arc::clone(&scope);

            let mut owned: Vec<Resource<JsonLogView>> = Vec::with_capacity(group.len());
            for lv in group {
                owned.push(m.store.data_mut().table.push(lv)?);
            }

            let start = Instant::now();
            let res = m
                .proc
                .tangent_logs_mapper()
                .call_process_logs(&mut m.store, &owned)
                .await;

            GUEST_LATENCY
                .with_label_values(&[&self.id.to_string()])
                .observe(start.elapsed().as_secs_f64());
            GUEST_BYTES_TOTAL.inc_by(bytes as u64);

            let frame = match res {
                Err(trap) => {
                    // The guest trapped (a panic, an out-of-bounds access, a
                    // host failure under it). Its memory is not to be trusted
                    // any more, so it is instantiated again from the component
                    // and this batch is skipped; the worker goes on. Without
                    // this one bad record took the worker down for good.
                    tracing::error!(
                        error = ?trap,
                        mapper = %m.name,
                        scope = %scope,
                        records = owned.len(),
                        "plugin trapped; reinstantiating it and skipping the batch"
                    );
                    PLUGIN_TRAPS_TOTAL
                        .with_label_values(&[m.cfg_name.as_ref()])
                        .inc();
                    let component = self
                        .components
                        .iter()
                        .find(|(n, _)| *n == *name)
                        .map(|(_, c)| c)
                        .ok_or_else(|| anyhow::anyhow!("no component for plugin {name}"))?;
                    self.mappers.mappers[idx] = Mappers::load_one(&self.engine, name, component)
                        .await
                        .with_context(|| format!("reinstantiating plugin {name} after a trap"))?;
                    continue;
                }
                Ok(Ok(frame)) => frame,
                Ok(Err(guest_err)) => {
                    tracing::warn!(mapper = %m.name, error = ?guest_err, "guest error; skipping");
                    continue;
                }
            };
            if frame.is_empty() {
                // A plugin that filters emits nothing for most batches.
                tracing::debug!(mapper = %m.name, "mapper produced empty output");
                continue;
            }
            // The guest's buffer becomes the frame without a copy.
            let frame = Bytes::from(frame)
                .try_into_mut()
                .unwrap_or_else(|b| BytesMut::from(&b[..]));
            out.push(frame);
        }
        Ok(out)
    }
}

/// `views` through inference node `name`, `concurrency` at a time: every
/// record comes back annotated, in the order given. A free function rather
/// than a method: the worker's guest stores are not `Sync`, so a `&Worker`
/// cannot be held across the model's await.
async fn run_inference(
    nodes: &HashMap<Arc<str>, InferenceNode>,
    name: &Arc<str>,
    views: &[JsonLogView],
) -> Result<Vec<BytesMut>> {
    let Some(node) = nodes.get(name) else {
        bail!("unknown inference node {name}");
    };
    let records: Vec<serde_json::Value> = views
        .iter()
        .map(|lv| serde_json::from_str(&lv.to_json()).unwrap_or(serde_json::Value::Null))
        .collect();
    let annotated = node.annotate_all(records).await;
    let mut frame = BytesMut::new();
    for v in annotated {
        frame.extend_from_slice(&serde_json::to_vec(&v)?);
        frame.extend_from_slice(b"\n");
    }
    Ok(vec![frame])
}

pub struct WorkerPool {
    senders: Vec<mpsc::Sender<Record>>,
    rr: AtomicUsize,
    handles: Vec<JoinHandle<()>>,
    in_flight: Arc<AtomicUsize>,
}

impl WorkerPool {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        size: usize,
        engines: Vec<wasm::engine::WasmEngine>,
        components: Vec<Vec<(Arc<str>, Component)>>,
        inference: &BTreeMap<Arc<str>, InferenceConfig>,
        config_dir: &Path,
        batch_max_size: usize,
        batch_max_age: Duration,
        router: Arc<Router>,
    ) -> anyhow::Result<Self> {
        let mut senders = Vec::with_capacity(size);
        let mut handles = Vec::with_capacity(size);
        let in_flight = router.in_flight_counter();

        let mut handoff_tx = Vec::with_capacity(size);
        let mut handoff_rx = VecDeque::with_capacity(size);
        for _ in 0..size {
            let (tx, rx) = mpsc::unbounded_channel::<Record>();
            handoff_tx.push(tx);
            handoff_rx.push_back(rx);
        }

        let ch_capacity = 4096;
        for (i, (engine, comps)) in engines.into_iter().zip(components).enumerate() {
            let (tx, rx) = mpsc::channel::<Record>(ch_capacity);
            senders.push(tx);

            let mut mappers = Mappers::load_all(&engine, &comps).await?;
            if let Some(first) = mappers.mappers.first_mut() {
                let start = Instant::now();
                match first
                    .proc
                    .tangent_logs_mapper()
                    .call_metadata(&mut first.store)
                    .await
                {
                    Ok(_) => tracing::info!(
                        target:"sidecar",
                        "worker {i} warmup in {} µs",
                        start.elapsed().as_micros()
                    ),
                    Err(e) => tracing::warn!(
                        target:"sidecar",
                        "worker {i} warmup failed after {} µs: {e}",
                        start.elapsed().as_micros()
                    ),
                }
            }

            let mut nodes = HashMap::default();
            for (name, cfg) in inference {
                nodes.insert(
                    Arc::clone(name),
                    InferenceNode::new(Arc::clone(name), cfg.clone(), config_dir)?,
                );
            }

            let worker = Worker {
                id: i,
                rx,
                mappers,
                engine,
                components: comps,
                inference: nodes,
                batch_max_size,
                batch_max_age,
                router: Arc::clone(&router),
                in_flight: Arc::clone(&in_flight),
                handoff_rx: handoff_rx
                    .pop_front()
                    .expect("one hand-off queue per worker"),
                handoffs: handoff_tx.clone(),
            };
            let h = tokio::spawn(async move {
                if let Err(e) = worker.run().await {
                    tracing::error!("worker {i} exited: {e:#}");
                }
            });
            handles.push(h);
        }

        Ok(Self {
            senders,
            rr: AtomicUsize::new(0),
            handles,
            in_flight,
        })
    }

    pub async fn dispatch(&self, mut job: Record) -> Result<()> {
        let n = self.senders.len();
        if n == 0 {
            anyhow::bail!("worker pool is closed")
        }
        let start = self.rr.fetch_add(1, Ordering::Relaxed) % n;

        CONSUMER_BYTES_TOTAL.inc_by(job.payload.size() as u64);
        CONSUMER_OBJECTS_TOTAL.inc();

        // Counted from here until a worker has walked it through the DAG (`Done`).
        self.in_flight.fetch_add(1, Ordering::AcqRel);
        for i in 0..n {
            let idx = (start + i) % n;
            match self.senders[idx].try_send(job) {
                Ok(()) => return Ok(()),
                Err(TrySendError::Full(j)) | Err(TrySendError::Closed(j)) => {
                    job = j;
                }
            }
        }

        let idx = start;
        if let Err(_e) = self.senders[idx].send(job).await {
            self.in_flight.fetch_sub(1, Ordering::AcqRel);
            tracing::warn!("all workers unavailable; dropping job");
            anyhow::bail!("all workers unavailable")
        }

        Ok(())
    }

    pub async fn join(self) {
        let WorkerPool {
            senders,
            rr: _,
            mut handles,
            in_flight: _,
        } = self;
        drop(senders);

        for h in handles.drain(..) {
            let _ = h.await;
        }
    }
}

#[cfg(test)]
impl WorkerPool {
    pub fn new_for_test(handles: Vec<JoinHandle<()>>) -> Self {
        Self {
            senders: Vec::new(),
            rr: AtomicUsize::new(0),
            handles,
            in_flight: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inference::stub;
    use crate::sinks::manager::{Sink, SinkManager, SinkWrite};
    use crate::state::{lru::LruStore, StateStore};
    use crate::wasm::engine::WasmEngine;
    use serde_json::Value;
    use tangent_shared::runtime::StateConfig;
    use tokio::sync::Mutex;

    #[test]
    fn a_partition_always_lands_on_the_same_worker() {
        for key in ["acme", "globex", "acme|us-west-2", ""] {
            assert_eq!(worker_for(key, 14), worker_for(key, 14));
            assert!(worker_for(key, 14) < 14);
        }
        let used: std::collections::HashSet<usize> = (0..64)
            .map(|i| worker_for(&format!("org-{i}"), 4))
            .collect();
        assert_eq!(used.len(), 4, "partitions spread over the workers");
    }

    #[derive(Default)]
    struct RecordingSink {
        writes: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl Sink for RecordingSink {
        async fn write(&self, req: SinkWrite) -> Result<()> {
            self.writes
                .lock()
                .await
                .push(String::from_utf8_lossy(&req.payload).into_owned());
            Ok(())
        }
    }

    #[derive(Default)]
    struct CountingAck(AtomicUsize);

    #[async_trait]
    impl Ack for CountingAck {
        async fn ack(&self) -> Result<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    /// A worker walks source → judge → sink: every record reaches the sink
    /// in order, annotated under the node's name, and its ack fires once the
    /// sink has it.
    #[tokio::test]
    async fn a_worker_walks_the_dag_from_the_source() {
        let (url, _seen) = stub::model().await;
        let dir = stub::dir("walk");
        let source = NodeRef::Source {
            name: Arc::from("events"),
        };
        let judge: Arc<str> = Arc::from("judge");
        let sink_name: Arc<str> = Arc::from("out");
        let sink = Arc::new(RecordingSink::default());
        let sink_manager = Arc::new(SinkManager::for_test(
            vec![(sink_name.clone(), sink.clone() as Arc<dyn Sink>)],
            1,
        ));
        let mut outs: HashMap<NodeRef, Vec<NodeRef>> = HashMap::default();
        outs.insert(
            source.clone(),
            vec![NodeRef::Inference {
                name: judge.clone(),
            }],
        );
        outs.insert(
            NodeRef::Inference {
                name: judge.clone(),
            },
            vec![NodeRef::Sink {
                name: sink_name,
                key_prefix: None,
            }],
        );
        let router = Arc::new(Router::new(
            outs,
            HashMap::default(),
            Arc::clone(&sink_manager),
        ));
        let in_flight = router.in_flight_counter();
        let mut inference = HashMap::default();
        inference.insert(
            judge.clone(),
            InferenceNode::new(judge.clone(), stub::config(&url), &dir).unwrap(),
        );
        let (tx, rx) = mpsc::channel(16);
        let handoff = mpsc::unbounded_channel();
        let store: Arc<dyn StateStore> = Arc::new(LruStore::new(StateConfig::default()).unwrap());
        let worker = Worker {
            id: 0,
            rx,
            mappers: Mappers {
                mappers: Vec::new(),
            },
            engine: WasmEngine::new(Arc::clone(&store), true).unwrap(),
            components: Vec::new(),
            inference,
            batch_max_size: 1 << 20,
            batch_max_age: Duration::from_millis(20),
            router: Arc::clone(&router),
            in_flight: Arc::clone(&in_flight),
            handoff_rx: handoff.1,
            handoffs: vec![handoff.0],
        };
        let handle = tokio::spawn(worker.run());

        let ack = Arc::new(CountingAck::default());
        for (i, msg) in ["fine", "bad login", "fine"].iter().enumerate() {
            in_flight.fetch_add(1, Ordering::AcqRel);
            tx.send(Record {
                payload: Payload::Raw(BytesMut::from(
                    format!("{{\"id\":{},\"msg\":\"{msg}\"}}\n", i + 1).as_str(),
                )),
                ack: Some(ack.clone() as Arc<dyn Ack>),
                at: source.clone(),
            })
            .await
            .unwrap();
        }
        drop(tx);
        handle.await.unwrap().unwrap();
        assert_eq!(
            router.in_flight(),
            0,
            "the walk is the whole in-flight span"
        );
        drop(router);
        Arc::try_unwrap(sink_manager)
            .ok()
            .expect("sink manager refs")
            .join()
            .await
            .unwrap();

        let writes = sink.writes.lock().await.clone();
        let lines: Vec<Value> = writes
            .iter()
            .flat_map(|w| w.lines())
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(
            lines
                .iter()
                .map(|v| v["id"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            [1, 2, 3],
            "in order past the model"
        );
        assert_eq!(lines[1]["judge"]["flag"], true);
        assert_eq!(lines[0]["judge"]["flag"], false);
        assert_eq!(lines[2]["judge"]["flag"], false);
        assert_eq!(
            ack.0.load(Ordering::SeqCst),
            3,
            "every record acked once the sink had it"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Two workers and a judge partitioned by org: the record reaches worker 0
    /// from the source, the judge runs it on worker 1, its owner, and it
    /// reaches the sink once.
    #[tokio::test]
    async fn hands_off_to_owner() {
        let (url, seen) = stub::model().await;
        let dir = stub::dir("handoff");
        let source = NodeRef::Source {
            name: Arc::from("events"),
        };
        let judge = NodeRef::Inference {
            name: Arc::from("judge"),
        };
        let sink_name: Arc<str> = Arc::from("out");
        let sink = Arc::new(RecordingSink::default());
        let sink_manager = Arc::new(SinkManager::for_test(
            vec![(sink_name.clone(), sink.clone() as Arc<dyn Sink>)],
            1,
        ));
        let mut outs: HashMap<NodeRef, Vec<NodeRef>> = HashMap::default();
        outs.insert(source.clone(), vec![judge.clone()]);
        outs.insert(
            judge.clone(),
            vec![NodeRef::Sink {
                name: sink_name,
                key_prefix: None,
            }],
        );
        let mut partitions = HashMap::default();
        partitions.insert(judge, ("org".to_string(), vec!["org_id".to_string()]));
        let router = Arc::new(Router::new(outs, partitions, Arc::clone(&sink_manager)));
        let store: Arc<dyn StateStore> = Arc::new(LruStore::new(StateConfig::default()).unwrap());
        let engines = (0..2)
            .map(|_| WasmEngine::new(Arc::clone(&store), true).unwrap())
            .collect();
        let mut inference = BTreeMap::new();
        inference.insert(Arc::from("judge"), stub::config(&url));
        let pool = Arc::new(
            WorkerPool::new(
                2,
                engines,
                vec![Vec::new(), Vec::new()],
                &inference,
                &dir,
                1 << 20,
                Duration::from_millis(20),
                Arc::clone(&router),
            )
            .await
            .unwrap(),
        );
        router.set_pool(&pool);

        let org = (0..)
            .map(|i| format!("org-{i}"))
            .find(|o| worker_for(&format!("org:{o}"), 2) == 1)
            .unwrap();
        let ack = Arc::new(CountingAck::default());
        let frame = BytesMut::from(format!("{{\"id\":1,\"org_id\":\"{org}\"}}\n").as_str());
        router
            .forward(&source, vec![frame], vec![ack.clone() as Arc<dyn Ack>])
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while router.in_flight() > 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the DAG goes quiet");
        drop(router);
        Arc::try_unwrap(pool).ok().expect("pool refs").join().await;
        Arc::try_unwrap(sink_manager)
            .ok()
            .expect("sink manager refs")
            .join()
            .await
            .unwrap();

        let writes = sink.writes.lock().await.clone();
        let lines: Vec<Value> = writes
            .iter()
            .flat_map(|w| w.lines())
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(lines.len(), 1);
        assert_eq!(lines[0]["org_id"], org.as_str());
        assert!(lines[0]["judge"]["flag"].is_boolean());
        assert_eq!(seen.lock().await.len(), 1);
        assert_eq!(ack.0.load(Ordering::SeqCst), 1);
        let _ = std::fs::remove_dir_all(dir);
    }
}
