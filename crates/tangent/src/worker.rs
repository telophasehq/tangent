use ahash::{HashMap, HashMapExt};
use anyhow::Result;
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::{Duration, Instant};
use tangent_shared::dag::NodeRef;
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::task::JoinHandle;
use tokio::time::{self, Instant as TokioInstant};
use wasmtime::component::{Component, Resource};

use crate::wasm::host::JsonLogView;
use crate::{
    router::Router,
    wasm::{self, mapper::Mappers, probe::eval_selector},
};
use crate::{CONSUMER_BYTES_TOTAL, CONSUMER_OBJECTS_TOTAL, GUEST_BYTES_TOTAL, GUEST_LATENCY};

#[async_trait]
pub trait Ack: Send + Sync {
    async fn ack(&self) -> Result<()>;
}

pub struct Record {
    pub payload: BytesMut,
    pub ack: Option<Arc<dyn Ack>>,
}

pub struct Worker {
    id: usize,
    rx: mpsc::Receiver<Record>,
    mappers: Mappers,
    batch_max_size: usize,
    batch_max_age: Duration,
    router: Arc<Router>,
}

impl Worker {
    pub async fn run(mut self) -> Result<()> {
        let mut batch = Vec::<BytesMut>::new();
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

                            let payload_len = rec.payload.len();

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
        batch: &mut Vec<BytesMut>,
        acks: &mut Vec<Arc<dyn Ack>>,
        total_size: &mut usize,
    ) -> Result<()> {
        if batch.is_empty() {
            tracing::warn!("flushed empty batch");
            return Ok(());
        }

        let mut groups: HashMap<usize, Vec<JsonLogView>> = HashMap::default();
        let mut sizes: HashMap<usize, usize> = HashMap::default();
        for b in batch.drain(..) {
            let sz = b.len();
            let lv = JsonLogView::from_bytes(b)?;
            let mut matched = false;
            for (idx, m) in self.mappers.mappers.iter_mut().enumerate() {
                if m.selectors.iter().any(|s| eval_selector(s, &lv)) {
                    groups.entry(idx).or_default().push(lv.clone());
                    *sizes.entry(idx).or_default() += sz;
                    matched = true;
                }
            }

            if !matched {
                tracing::warn!("log did not match any mappers");
            }
        }

        let mut plugin_outputs: HashMap<Arc<str>, Vec<BytesMut>> =
            HashMap::with_capacity(batch.len());

        for (idx, lvs) in groups {
            let m = &mut self.mappers.mappers[idx];

            let mut owned: Vec<Resource<JsonLogView>> = Vec::new();
            for lv in lvs {
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
            GUEST_BYTES_TOTAL.inc_by(*sizes.get(&idx).unwrap() as u64);

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

        for (plugin_name, frames) in plugin_outputs {
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
}

impl WorkerPool {
    pub async fn new(
        size: usize,
        engines: Vec<wasm::engine::WasmEngine>,
        components: Vec<Vec<(Arc<str>, Component)>>,
        batch_max_size: usize,
        batch_max_age: Duration,
        router: Arc<Router>,
    ) -> anyhow::Result<Self> {
        let mut senders = Vec::with_capacity(size);
        let mut handles = Vec::with_capacity(size);

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
        })
    }

    pub async fn dispatch(&self, mut job: Record) -> Result<()> {
        let n = self.senders.len();
        if n == 0 {
            anyhow::bail!("worker pool is closed")
        }
        let start = self.rr.fetch_add(1, Ordering::Relaxed) % n;

        CONSUMER_BYTES_TOTAL.inc_by(job.payload.len() as u64);
        CONSUMER_OBJECTS_TOTAL.inc();

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

    pub fn close(&mut self) {
        self.senders.clear();
    }

    pub async fn join(self) {
        for h in self.handles {
            let _ = h.await;
        }
    }
}
