use anyhow::Result;
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use std::time::{Duration, Instant};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::sync::mpsc;
use tokio::sync::mpsc::error::TrySendError;
use tokio::task::JoinHandle;
use tokio::time::{self, Instant as TokioInstant};
use wasmtime::Store;

use crate::sinks::{manager::SinkManager, wal::RouteKey};
use crate::wasm::{self, Host, Processor, Sink};
use crate::{CONSUMER_BYTES_TOTAL, CONSUMER_OBJECTS_TOTAL, GUEST_BYTES_TOTAL, GUEST_LATENCY};

#[async_trait]
pub trait Ack: Send + Sync {
    async fn ack(&self) -> Result<()>;
}

#[derive(Clone)]
struct RefCountAck {
    remaining: Arc<AtomicUsize>,
    inners: Arc<Vec<Arc<dyn Ack>>>,
}

impl RefCountAck {
    fn new(inner: Vec<Arc<dyn Ack>>, n: usize) -> Self {
        Self {
            remaining: Arc::new(AtomicUsize::new(n)),
            inners: Arc::new(inner),
        }
    }
}

#[async_trait]
impl Ack for RefCountAck {
    async fn ack(&self) -> Result<()> {
        if self.remaining.fetch_sub(1, Ordering::AcqRel) == 1 {
            for a in self.inners.iter() {
                let _ = a.ack().await;
            }
        }
        Ok(())
    }
}

pub struct Record {
    pub payload: Bytes,
    pub ack: Option<Arc<dyn Ack>>,
}

pub struct Worker {
    id: usize,
    rx: mpsc::Receiver<Record>,
    store: Store<Host>,
    processor: Processor,
    batch_max_size: usize,
    batch_max_age: Duration,
    sink_manager: Arc<SinkManager>,
    default_sink: Option<Sink>,
}

impl Worker {
    pub async fn run(mut self) -> Result<()> {
        let mut batch = BytesMut::with_capacity(self.batch_max_size);
        let mut acks: Vec<Arc<dyn Ack>> = Vec::with_capacity(1024);
        let mut events = 0usize;

        let mut deadline = TokioInstant::now() + self.batch_max_age;
        let sleeper = time::sleep_until(deadline);
        tokio::pin!(sleeper);

        loop {
            tokio::select! {
                maybe_job = self.rx.recv() => {
                    match maybe_job {
                        None => {
                            let _ = self.flush_batch(&mut batch, &mut acks, &mut events).await;
                            break;
                        }
                        Some(rec) => {
                            if batch.is_empty() {
                                deadline = TokioInstant::now() + self.batch_max_age;
                                sleeper.as_mut().reset(deadline);
                            }

                            let need = rec.payload.len();

                            if batch.len() + need > self.batch_max_size {
                                self.flush_batch(&mut batch, &mut acks, &mut events).await?;
                                deadline = TokioInstant::now() + self.batch_max_age;
                                sleeper.as_mut().reset(deadline);
                            }

                            if need > self.batch_max_size && batch.is_empty() {
                                let mut single = BytesMut::from(rec.payload.as_ref());
                                let mut one = 1usize;
                                self.flush_batch(&mut single, &mut acks, &mut one).await?;
                                deadline = TokioInstant::now() + self.batch_max_age;
                                sleeper.as_mut().reset(deadline);
                                if let Some(a) = rec.ack { acks.push(a); }
                            } else {
                                batch.extend_from_slice(&rec.payload);
                                if let Some(a) = rec.ack { acks.push(a); }
                                events += 1;
                            }
                        }
                    }
                }
                _ = &mut sleeper => {
                    if !batch.is_empty() {
                        self.flush_batch(&mut batch, &mut acks, &mut events).await?;
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
        batch: &mut BytesMut,
        acks: &mut Vec<Arc<dyn Ack>>,
        events_in_batch: &mut usize,
    ) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let s: &mut Store<Host> = &mut self.store;
        let start = Instant::now();

        let outs = match self.processor.call_process_logs(s, &batch).await {
            Err(host) => {
                return Err(anyhow::anyhow!("process_logs host error: {host}"));
            }
            Ok(Ok(outs)) => outs,
            Ok(Err(guest)) => {
                tracing::warn!(target: "sidecar",
                    %guest,
                    batch_bytes = batch.len(),
                    "process_logs guest error; skipping batch");
                batch.clear();
                *events_in_batch = 0;
                return Ok(());
            }
        };

        let secs = start.elapsed().as_secs_f64();
        GUEST_LATENCY
            .with_label_values(&[&self.id.to_string()])
            .observe(secs);
        GUEST_BYTES_TOTAL.inc_by(batch.len() as u64);

        let mut sink_routes: HashMap<RouteKey, BytesMut> = HashMap::new();

        if outs.len() == 0 {
            tracing::warn!("no output from process_logs");
        }
        for o in outs {
            let data = &o.data;

            match o.sink {
                Sink::Default(_) => {
                    if self.default_sink.is_none() {
                        tracing::warn!(
                                "No sink specified for log and no default sink configured. Dropping log."
                            );
                    } else {
                        let key = sink_key(self.default_sink.clone().unwrap());
                        let buf = sink_routes
                            .entry(key)
                            .or_insert_with(|| BytesMut::with_capacity(data.len()));
                        buf.extend_from_slice(data);
                    }
                }
                _ => {
                    let key = sink_key(o.sink);
                    let buf = sink_routes
                        .entry(key)
                        .or_insert_with(|| BytesMut::with_capacity(data.len()));
                    buf.extend_from_slice(data);
                }
            }
        }

        let total_writes = sink_routes.len().max(1);
        let rc = {
            let batch_acks = std::mem::take(acks);
            RefCountAck::new(batch_acks, total_writes)
        };

        for (rk, buf) in sink_routes {
            let payload = buf.freeze();

            self.sink_manager
                .enqueue(
                    rk.sink_name.clone(),
                    rk.prefix.clone(),
                    payload,
                    vec![Arc::new(rc.clone()) as Arc<dyn Ack>],
                )
                .await
                .map_err(|e| anyhow::anyhow!("sink queue full: {e}"))?;
        }
        tracing::debug!(
            target: "sidecar",
            worker = self.id,
            events = *events_in_batch,
            bytes = batch.len(),
            took_us = start.elapsed().as_micros(),
            "processed batch"
        );

        batch.clear();
        *events_in_batch = 0;
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
        engine: wasm::WasmEngine,
        sink_manager: Arc<SinkManager>,
        batch_max_size: usize,
        batch_max_age: Duration,
        default_sink: Option<Sink>,
    ) -> anyhow::Result<Self> {
        let mut senders = Vec::with_capacity(size);
        let mut handles = Vec::with_capacity(size);

        let ch_capacity = 4096;
        for i in 0..size {
            let (tx, rx) = mpsc::channel::<Record>(ch_capacity);
            senders.push(tx);

            let mut store = engine.make_store();
            let processor = engine.make_processor(&mut store).await?;

            let start = Instant::now();
            match processor.call_process_logs(&mut store, b"").await? {
                Ok(_) => {
                    tracing::info!(target:"sidecar",
                        "worker {i} warmup in {} µs", start.elapsed().as_micros());
                }
                Err(e) => {
                    tracing::error!(target:"sidecar",
                        "worker {i} warmup failed after {} µs: {e}",
                        start.elapsed().as_micros());
                }
            }

            let worker = Worker {
                id: i,
                sink_manager: Arc::clone(&sink_manager),
                rx,
                store,
                processor,
                batch_max_size,
                batch_max_age,
                default_sink: default_sink.clone(),
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
            handles: handles,
        })
    }

    pub async fn dispatch(&self, mut job: Record) {
        let n = self.senders.len();
        if n == 0 {
            tracing::warn!("worker pool is closed; dropping job");
            return;
        }
        let start = self.rr.fetch_add(1, Ordering::Relaxed) % n;

        CONSUMER_BYTES_TOTAL.inc_by(job.payload.len() as u64);
        CONSUMER_OBJECTS_TOTAL.inc();

        for i in 0..n {
            let idx = (start + i) % n;
            match self.senders[idx].try_send(job) {
                Ok(()) => return,
                Err(TrySendError::Full(j)) => {
                    job = j;
                }
                Err(TrySendError::Closed(j)) => {
                    job = j;
                }
            }
        }

        let idx = start;
        if let Err(_e) = self.senders[idx].send(job).await {
            tracing::warn!("all workers unavailable; dropping job");
        }
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

fn sink_key(s: Sink) -> RouteKey {
    match s {
        Sink::S3(s3c) => {
            return RouteKey {
                sink_name: s3c.name.clone(),
                prefix: s3c.key_prefix.clone(),
            };
        }
        Sink::File(fc) => {
            return RouteKey {
                sink_name: fc.name.clone(),
                prefix: None,
            };
        }
        Sink::Blackhole(bc) => {
            return RouteKey {
                sink_name: bc.name.clone(),
                prefix: None,
            };
        }
        Sink::Default(_) => unreachable!(),
    }
}
