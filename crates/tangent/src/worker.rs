use ahash::AHashMap as HashMap;
use anyhow::{bail, Result};
use async_trait::async_trait;
use bytes::BytesMut;
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
use tangent_shared::runtime::PartitionConfig;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::task::JoinHandle;
use tokio::time::{self, Instant as TokioInstant};
use wasmtime::component::{Component, Resource};

use crate::inference::{lines, InferenceNode};
use crate::router::{RefCountAck, Router};
use crate::wasm::host::JsonLogView;
use crate::{
    partition,
    wasm::{self, mapper::Mappers, probe::eval_selector},
};
use crate::{CONSUMER_BYTES_TOTAL, CONSUMER_OBJECTS_TOTAL, GUEST_BYTES_TOTAL, GUEST_LATENCY};

#[async_trait]
pub trait Ack: Send + Sync {
    async fn ack(&self) -> Result<()>;
}

/// A record on its way to a worker: raw bytes, or already parsed when the
/// pool needed its partition key to pick the worker.
pub enum Payload {
    Raw(BytesMut),
    Parsed {
        view: JsonLogView,
        partition: Arc<str>,
    },
}

impl Payload {
    pub fn size(&self) -> usize {
        match self {
            Payload::Raw(b) => b.len(),
            Payload::Parsed { view, .. } => view.byte_len(),
        }
    }

    /// The record as bytes, serialized again if it was parsed.
    pub fn into_bytes(self) -> BytesMut {
        match self {
            Payload::Raw(b) => b,
            Payload::Parsed { view, .. } => BytesMut::from(view.to_json().as_bytes()),
        }
    }
}

pub struct Record {
    pub payload: Payload,
    pub ack: Option<Arc<dyn Ack>>,
    /// The source the record came in from: where its walk of the DAG starts.
    pub from: NodeRef,
}

/// Records at one node of the walk, each with the partition it belongs to.
type Views = Vec<(JsonLogView, Arc<str>)>;

/// Takes `n` records off the DAG's in-flight count when dropped: after the
/// batch has been through the DAG, or lost to an error.
struct Done(Arc<AtomicUsize>, usize);

impl Drop for Done {
    fn drop(&mut self) {
        self.0.fetch_sub(self.1, Ordering::AcqRel);
    }
}

/// The worker that owns a partition: a stable function of the key, so one
/// guest sees a partition's records in order and nothing races on its state.
fn worker_for(partition: &str, workers: usize) -> usize {
    let mut h = DefaultHasher::new();
    partition.hash(&mut h);
    (h.finish() % workers as u64) as usize
}

/// The records as one NDJSON frame, for a sink.
fn frame_of(views: &Views) -> BytesMut {
    let mut frame = BytesMut::new();
    for (lv, _) in views {
        frame.extend_from_slice(lv.to_json().as_bytes());
        frame.extend_from_slice(b"\n");
    }
    frame
}

/// A worker hosts every plugin and every inference node, and walks the DAG
/// for each batch it is handed: a record enters at its source and goes to
/// each of the node's outputs in turn, a plugin's or a model's output being
/// the next node's input, until the sinks. Records between a source and a
/// sink never leave the worker, so a partition's records stay in order.
pub struct Worker {
    id: usize,
    rx: mpsc::Receiver<Record>,
    mappers: Mappers,
    inference: HashMap<Arc<str>, InferenceNode>,
    batch_max_size: usize,
    batch_max_age: Duration,
    router: Arc<Router>,
    /// With a partition key configured, a batch is handed to each plugin one
    /// partition at a time, so the guest's state is scoped to that partition.
    partition: Option<Arc<PartitionConfig>>,
    /// Records anywhere between a source and a sink, shared with the router.
    in_flight: Arc<AtomicUsize>,
}

impl Worker {
    pub async fn run(mut self) -> Result<()> {
        let mut batch = Vec::<Record>::new();
        let mut acks: Vec<Arc<dyn Ack>> = Vec::with_capacity(1024);
        let mut total_size = 0usize;

        let mut deadline = TokioInstant::now() + self.batch_max_age;
        let sleeper = time::sleep_until(deadline);
        tokio::pin!(sleeper);

        loop {
            tokio::select! {
                maybe_job = self.rx.recv() => {
                    match maybe_job {
                        None => {
                            if !batch.is_empty() {
                                let _ = self.flush_batch(&mut batch, &mut acks, &mut total_size).await;
                            }
                            break;
                        }
                        Some(mut rec) => {
                            if batch.is_empty() {
                                deadline = TokioInstant::now() + self.batch_max_age;
                                sleeper.as_mut().reset(deadline);
                            }

                            let payload_len = rec.payload.size();

                            if total_size + payload_len > self.batch_max_size {
                                self.flush_batch(&mut batch, &mut acks, &mut total_size).await?;
                                deadline = TokioInstant::now() + self.batch_max_age;
                                sleeper.as_mut().reset(deadline);
                            }

                            if payload_len > self.batch_max_size && batch.is_empty() {
                                let mut single_ack: Vec<Arc<dyn Ack>> = rec.ack.take().into_iter().collect();
                                let mut single = vec![rec];
                                self.flush_batch(&mut single, &mut single_ack, &mut total_size).await?;
                                deadline = TokioInstant::now() + self.batch_max_age;
                                sleeper.as_mut().reset(deadline);
                            } else {
                                total_size += payload_len;
                                if let Some(a) = rec.ack.take() { acks.push(a); }
                                batch.push(rec);
                            }
                        }
                    }
                }
                () = &mut sleeper => {
                    if !batch.is_empty() {
                        self.flush_batch(&mut batch, &mut acks, &mut total_size).await?;
                    }
                    deadline = TokioInstant::now() + self.batch_max_age;
                    sleeper.as_mut().reset(deadline);
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

        // Parsed, and grouped by the source each record came in from.
        let unpartitioned: Arc<str> = Arc::from("");
        let mut queue: VecDeque<(NodeRef, Views)> = VecDeque::new();
        for Record { payload, from, .. } in batch.drain(..) {
            let (lv, part) = match payload {
                Payload::Parsed { view, partition } => (view, partition),
                Payload::Raw(b) => {
                    let lv = JsonLogView::from_bytes(b)?;
                    let part = self
                        .partition
                        .as_ref()
                        .and_then(|cfg| partition::key_of_view(&lv, cfg))
                        .unwrap_or_else(|| Arc::clone(&unpartitioned));
                    (lv, part)
                }
            };
            match queue.iter_mut().find(|(node, _)| *node == from) {
                Some((_, views)) => views.push((lv, part)),
                None => queue.push_back((from, vec![(lv, part)])),
            }
        }

        let router = Arc::clone(&self.router);
        let mut deliveries: Vec<(Arc<str>, Option<Arc<str>>, BytesMut)> = Vec::new();
        while let Some((node, views)) = queue.pop_front() {
            for to in router.outs(&node) {
                match to {
                    NodeRef::Plugin { name } => {
                        let out = self.run_plugin(name, &views).await?;
                        if !out.is_empty() {
                            queue.push_back((to.clone(), out));
                        }
                    }
                    NodeRef::Inference { name } => {
                        let out = run_inference(&self.inference, name, &views).await?;
                        if !out.is_empty() {
                            queue.push_back((to.clone(), out));
                        }
                    }
                    NodeRef::Sink { name, key_prefix } => {
                        deliveries.push((Arc::clone(name), key_prefix.clone(), frame_of(&views)));
                    }
                    NodeRef::Source { .. } => {
                        tracing::warn!(from = ?node, to = ?to, "a source is not an output; dropping");
                    }
                }
            }
        }

        let upstream = std::mem::take(acks);
        if deliveries.is_empty() {
            for a in upstream {
                let _ = a.ack().await;
            }
        } else {
            let shared: Arc<dyn Ack> = Arc::new(RefCountAck::new(upstream, deliveries.len()));
            for (name, key_prefix, frame) in deliveries {
                router
                    .sinks()
                    .enqueue(name, key_prefix, frame, vec![Arc::clone(&shared)])
                    .await?;
            }
        }

        batch.clear();
        *total_size = 0;
        Ok(())
    }

    /// `views` through plugin `name`: one guest call per partition, with the
    /// records whose probe matched, the plugin's state scoped to that
    /// partition. The plugin's output records, partitions in first-seen
    /// order, each under the partition it came out of.
    async fn run_plugin(&mut self, name: &Arc<str>, views: &Views) -> Result<Views> {
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
            for (lv, part) in views {
                if !m.selectors.iter().any(|s| eval_selector(s, lv)) {
                    continue;
                }
                match groups.iter_mut().find(|(p, _, _)| p == part) {
                    Some((_, g, bytes)) => {
                        g.push(lv.clone());
                        *bytes += lv.byte_len();
                    }
                    None => groups.push((Arc::clone(part), vec![lv.clone()], lv.byte_len())),
                }
            }
        }
        if groups.is_empty() {
            tracing::debug!(plugin = %name, "no record matched the probe");
            return Ok(Vec::new());
        }

        let mut out: Views = Vec::new();
        for (part, group, bytes) in groups {
            let m = &mut self.mappers.mappers[idx];
            m.store.data_mut().partition = Arc::clone(&part);

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
                Err(host_err) => {
                    tracing::error!(error = ?host_err, mapper = %m.name, "host error in process_log");
                    return Err(host_err);
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
            for line in lines(&frame) {
                let mut b = BytesMut::with_capacity(line.len());
                b.extend_from_slice(line);
                match JsonLogView::from_bytes(b) {
                    Ok(lv) => out.push((lv, Arc::clone(&part))),
                    Err(e) => {
                        tracing::warn!(mapper = %m.name, error = %e, "plugin output is not JSON; dropping")
                    }
                }
            }
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
    views: &Views,
) -> Result<Views> {
    let Some(node) = nodes.get(name) else {
        bail!("unknown inference node {name}");
    };
    let records: Vec<serde_json::Value> = views
        .iter()
        .map(|(lv, _)| serde_json::from_str(&lv.to_json()).unwrap_or(serde_json::Value::Null))
        .collect();
    let annotated = node.annotate_all(records).await;
    let mut out: Views = Vec::with_capacity(annotated.len());
    for (v, (_, part)) in annotated.into_iter().zip(views) {
        let bytes = serde_json::to_vec(&v)?;
        out.push((
            JsonLogView::from_bytes(BytesMut::from(&bytes[..]))?,
            Arc::clone(part),
        ));
    }
    Ok(out)
}

pub struct WorkerPool {
    senders: Vec<mpsc::Sender<Record>>,
    rr: AtomicUsize,
    handles: Vec<JoinHandle<()>>,
    partition: Option<Arc<PartitionConfig>>,
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
        partition: Option<PartitionConfig>,
    ) -> anyhow::Result<Self> {
        let mut senders = Vec::with_capacity(size);
        let mut handles = Vec::with_capacity(size);
        let partition = partition.map(Arc::new);
        let in_flight = router.in_flight_counter();

        let ch_capacity = 4096;
        for i in 0..size {
            let (tx, rx) = mpsc::channel::<Record>(ch_capacity);
            senders.push(tx);

            let mut mappers = Mappers::load_all(&engines[i], &components[i]).await?;
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
                inference: nodes,
                batch_max_size,
                batch_max_age,
                router: Arc::clone(&router),
                partition: partition.clone(),
                in_flight: Arc::clone(&in_flight),
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
            partition,
            in_flight,
        })
    }

    /// With a partition key configured, parse the record here and pin it to
    /// its partition's worker; the worker reuses the parse. None when there is
    /// no key to pin by (unpartitioned, or the record lacks the key fields).
    fn pin(&self, job: &mut Record, workers: usize) -> Result<Option<usize>> {
        let Some(cfg) = &self.partition else {
            return Ok(None);
        };
        let raw = match std::mem::replace(&mut job.payload, Payload::Raw(BytesMut::new())) {
            Payload::Raw(raw) => raw,
            Payload::Parsed { view, partition } => {
                let idx = worker_for(&partition, workers);
                job.payload = Payload::Parsed { view, partition };
                return Ok(Some(idx));
            }
        };
        let view = JsonLogView::from_bytes(raw)?;
        Ok(match partition::key_of_view(&view, cfg) {
            Some(partition) => {
                let idx = worker_for(&partition, workers);
                job.payload = Payload::Parsed { view, partition };
                Some(idx)
            }
            None => {
                job.payload = Payload::Parsed {
                    view,
                    partition: Arc::from(""),
                };
                None
            }
        })
    }

    pub async fn dispatch(&self, mut job: Record) -> Result<()> {
        let n = self.senders.len();
        if n == 0 {
            anyhow::bail!("worker pool is closed")
        }

        CONSUMER_BYTES_TOTAL.inc_by(job.payload.size() as u64);
        CONSUMER_OBJECTS_TOTAL.inc();

        let pinned = match self.pin(&mut job, n) {
            Ok(p) => p,
            Err(e) => {
                // The worker would have failed on it too; a bad record is
                // not worth a worker.
                tracing::warn!(error = %e, "record is not valid JSON; dropping");
                if let Some(a) = job.ack {
                    let _ = a.ack().await;
                }
                return Ok(());
            }
        };
        // Counted from here until a worker has walked it through the DAG (`Done`).
        self.in_flight.fetch_add(1, Ordering::AcqRel);
        if let Some(idx) = pinned {
            // Pinned records wait for their worker rather than spilling to a
            // neighbor: that is what keeps a partition on one guest.
            if self.senders[idx].send(job).await.is_err() {
                self.in_flight.fetch_sub(1, Ordering::AcqRel);
                anyhow::bail!("worker {idx} unavailable")
            }
            return Ok(());
        }

        let start = self.rr.fetch_add(1, Ordering::Relaxed) % n;
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
            partition: _,
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
            partition: None,
            in_flight: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inference::stub;
    use crate::sinks::manager::{Sink, SinkManager, SinkWrite};
    use serde_json::Value;
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
        let router = Arc::new(Router::new(outs, Arc::clone(&sink_manager)));
        let in_flight = router.in_flight_counter();
        let mut inference = HashMap::default();
        inference.insert(
            judge.clone(),
            InferenceNode::new(judge.clone(), stub::config(&url), &dir).unwrap(),
        );
        let (tx, rx) = mpsc::channel(16);
        let worker = Worker {
            id: 0,
            rx,
            mappers: Mappers {
                mappers: Vec::new(),
            },
            inference,
            batch_max_size: 1 << 20,
            batch_max_age: Duration::from_millis(20),
            router: Arc::clone(&router),
            partition: None,
            in_flight: Arc::clone(&in_flight),
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
                from: source.clone(),
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
}
