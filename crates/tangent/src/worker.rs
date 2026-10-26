use ahash::{HashMap, HashMapExt};
use anyhow::Result;
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use tangent_shared::dag::NodeRef;
use tangent_shared::runtime::PartitionConfig;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::task::JoinHandle;
use tokio::time::{self, Instant as TokioInstant};
use wasmtime::component::{Component, Resource};

use crate::wasm::host::JsonLogView;
use crate::{
    partition,
    router::Router,
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
}

pub struct Record {
    pub payload: Payload,
    pub ack: Option<Arc<dyn Ack>>,
}

/// The worker that owns a partition: a stable function of the key, so one
/// guest sees a partition's records in order and nothing races on its state.
fn worker_for(partition: &str, workers: usize) -> usize {
    let mut h = DefaultHasher::new();
    partition.hash(&mut h);
    (h.finish() % workers as u64) as usize
}

pub struct Worker {
    id: usize,
    rx: mpsc::Receiver<Record>,
    mappers: Mappers,
    batch_max_size: usize,
    batch_max_age: Duration,
    router: Arc<Router>,
    /// With a partition key configured, a batch is handed to each plugin one
    /// partition at a time, so the guest's state is scoped to that partition.
    partition: Option<Arc<PartitionConfig>>,
}

impl Worker {
    pub async fn run(mut self) -> Result<()> {
        let mut batch = Vec::<Payload>::new();
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
                        Some(rec) => {
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
                                let mut single = vec![rec.payload];
                                let mut single_ack = rec.ack.as_slice().to_owned();
                                self.flush_batch(&mut single, &mut single_ack, &mut total_size).await?;
                                deadline = TokioInstant::now() + self.batch_max_age;
                                sleeper.as_mut().reset(deadline);
                            } else {
                                total_size += payload_len;
                                batch.push(rec.payload);
                                if let Some(a) = rec.ack { acks.push(a); }
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

    pub async fn flush_batch(
        &mut self,
        batch: &mut Vec<Payload>,
        acks: &mut Vec<Arc<dyn Ack>>,
        total_size: &mut usize,
    ) -> Result<()> {
        if batch.is_empty() {
            tracing::warn!("flushed empty batch");
            return Ok(());
        }

        // One guest call per (plugin, partition): the plugin sees records of
        // one partition at a time, and its state calls land in that partition.
        // Groups keep the order the partitions first appeared in, so output is
        // deterministic and an unpartitioned batch keeps its input order.
        struct Group {
            idx: usize,
            part: Arc<str>,
            views: Vec<JsonLogView>,
            bytes: usize,
        }
        let unpartitioned: Arc<str> = Arc::from("");
        let mut groups: Vec<Group> = Vec::new();
        let mut group_of: HashMap<(usize, Arc<str>), usize> = HashMap::default();
        for p in batch.drain(..) {
            let sz = p.size();
            let (lv, part) = match p {
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
            let mut matched = false;
            for (idx, m) in self.mappers.mappers.iter_mut().enumerate() {
                if m.selectors.iter().any(|s| eval_selector(s, &lv)) {
                    let g = *group_of.entry((idx, Arc::clone(&part))).or_insert_with(|| {
                        groups.push(Group {
                            idx,
                            part: Arc::clone(&part),
                            views: Vec::new(),
                            bytes: 0,
                        });
                        groups.len() - 1
                    });
                    groups[g].views.push(lv.clone());
                    groups[g].bytes += sz;
                    matched = true;
                }
            }

            if !matched {
                tracing::debug!("log did not match any mappers");
            }
        }

        let mut plugin_outputs: HashMap<Arc<str>, Vec<BytesMut>> =
            HashMap::with_capacity(batch.len());

        for Group {
            idx,
            part,
            views,
            bytes,
        } in groups
        {
            let m = &mut self.mappers.mappers[idx];
            m.store.data_mut().partition = part;

            let mut owned: Vec<Resource<JsonLogView>> = Vec::new();
            for lv in views {
                let h = m.store.data_mut().table.push(lv)?;
                owned.push(h);
            }

            let start = Instant::now();
            let res = m
                .proc
                .tangent_logs_mapper()
                .call_process_logs(&mut m.store, &owned)
                .await;

            let secs = start.elapsed().as_secs_f64();
            GUEST_LATENCY
                .with_label_values(&[&self.id.to_string()])
                .observe(secs);
            GUEST_BYTES_TOTAL.inc_by(bytes as u64);

            let out = match res {
                Err(host_err) => {
                    tracing::error!(error = ?host_err, mapper=%m.name, "host error in process_log");
                    return Err(host_err);
                }
                Ok(Ok(frames)) => frames,
                Ok(Err(guest_err)) => {
                    tracing::warn!(mapper=%m.name, error = ?guest_err, "guest error; skipping");
                    continue;
                }
            };

            if out.is_empty() {
                tracing::warn!(mapper=%m.name, "mapper produced empty output");
                continue;
            }

            plugin_outputs
                .entry(m.cfg_name.clone())
                .or_default()
                .push(Bytes::from(out).try_into_mut().unwrap())
        }

        let upstream_acks = std::mem::take(acks);
        let mut remaining = upstream_acks;

        for (plugin_name, mut frames) in plugin_outputs {
            // One frame per plugin per flush, as without partitioning: the
            // batch took several guest calls but is still one unit downstream,
            // with the partitions' output in first-seen order.
            if frames.len() > 1 {
                let mut joined = BytesMut::with_capacity(frames.iter().map(|f| f.len() + 1).sum());
                for f in &frames {
                    joined.extend_from_slice(f);
                    if !f.ends_with(b"\n") {
                        joined.extend_from_slice(b"\n");
                    }
                }
                frames = vec![joined];
            }
            self.router
                .forward(
                    &NodeRef::Plugin { name: plugin_name },
                    frames,
                    std::mem::take(&mut remaining),
                )
                .await?;
        }

        batch.clear();
        *total_size = 0;
        Ok(())
    }
}

pub struct WorkerPool {
    senders: Vec<mpsc::Sender<Record>>,
    rr: AtomicUsize,
    handles: Vec<JoinHandle<()>>,
    partition: Option<Arc<PartitionConfig>>,
}

impl WorkerPool {
    pub async fn new(
        size: usize,
        engines: Vec<wasm::engine::WasmEngine>,
        components: Vec<Vec<(Arc<str>, Component)>>,
        batch_max_size: usize,
        batch_max_age: Duration,
        router: Arc<Router>,
        partition: Option<PartitionConfig>,
    ) -> anyhow::Result<Self> {
        let mut senders = Vec::with_capacity(size);
        let mut handles = Vec::with_capacity(size);
        let partition = partition.map(Arc::new);

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

            let worker = Worker {
                id: i,
                rx,
                mappers,
                batch_max_size,
                batch_max_age,
                router: Arc::clone(&router),
                partition: partition.clone(),
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
        if let Some(idx) = pinned {
            // Pinned records wait for their worker rather than spilling to a
            // neighbor: that is what keeps a partition on one guest.
            if self.senders[idx].send(job).await.is_err() {
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
