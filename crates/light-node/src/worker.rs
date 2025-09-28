use anyhow::Result;
use bytes::{Bytes, BytesMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time::{self, Instant as TokioInstant};
use wasmtime::Store;

use crate::wasm::{self, Host, Processor};
use crate::{BATCH_EVENTS, BATCH_LATENCY};

pub enum Incoming {
    Record(Bytes),
}

pub struct Worker {
    id: usize,
    rx: mpsc::Receiver<Incoming>,
    store: Store<Host>,
    processor: Processor,
    batch_max_size: usize,
    batch_max_age: Duration,
}

impl Worker {
    pub async fn run(mut self) -> Result<()> {
        let mut batch = BytesMut::with_capacity(self.batch_max_size);
        let mut events = 0usize;

        let mut deadline = TokioInstant::now() + self.batch_max_age;
        let sleeper = time::sleep_until(deadline);
        tokio::pin!(sleeper);

        loop {
            tokio::select! {
                maybe_job = self.rx.recv() => {
                    match maybe_job {
                        None => {
                            let _ = self.flush_batch(&mut batch, &mut events).await;
                            break;
                        }
                        Some(Incoming::Record(rec)) => {
                            if batch.is_empty() {
                                deadline = TokioInstant::now() + self.batch_max_age;
                                sleeper.as_mut().reset(deadline);
                            }

                            let need = rec.len();

                            if batch.len() + need > self.batch_max_size {
                                self.flush_batch(&mut batch, &mut events).await?;
                                deadline = TokioInstant::now() + self.batch_max_age;
                                sleeper.as_mut().reset(deadline);
                            }

                            if need > self.batch_max_size && batch.is_empty() {
                                let mut single = BytesMut::from(rec.as_ref());
                                let mut one = 1usize;
                                self.flush_batch(&mut single, &mut one).await?;
                                deadline = TokioInstant::now() + self.batch_max_age;
                                sleeper.as_mut().reset(deadline);
                            } else {
                                batch.extend_from_slice(&rec);
                                events += 1;
                            }
                        }
                    }
                }
                _ = &mut sleeper => {
                    if !batch.is_empty() {
                        self.flush_batch(&mut batch, &mut events).await?;
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
        events_in_batch: &mut usize,
    ) -> Result<()> {
        if batch.is_empty() {
            return Ok(());
        }

        let start = Instant::now();
        let s: &mut Store<Host> = &mut self.store;

        // TODO: handle output
        let _ = self.processor.call_process_logs(s, &batch).await?;

        let secs = start.elapsed().as_secs_f64();
        BATCH_LATENCY.observe(secs);
        BATCH_EVENTS.inc_by(*events_in_batch as u64);

        tracing::info!(target: "sidecar", "processed batch of {} in {} µs",
                                events_in_batch, start.elapsed().as_micros());
        batch.clear();
        *events_in_batch = 0;
        Ok(())
    }
}

pub struct WorkerPool {
    senders: Vec<mpsc::Sender<Incoming>>,
    rr: AtomicUsize,
}

impl WorkerPool {
    pub async fn new(
        size: usize,
        engine: wasm::WasmEngine,
        batch_max_size: usize,
        batch_max_age: Duration,
    ) -> anyhow::Result<Self> {
        let mut senders = Vec::with_capacity(size);

        // e.g., allow ~several batches worth per worker. Start simple:
        let ch_capacity = 4096;

        for i in 0..size {
            let (tx, rx) = mpsc::channel::<Incoming>(ch_capacity);
            senders.push(tx);

            let mut store = engine.make_store();
            let processor = engine.make_processor(&mut store).await?;

            let start = Instant::now();
            match processor.call_process_logs(&mut store, b"{}").await? {
                Ok(_) => {
                    BATCH_LATENCY.observe(start.elapsed().as_secs_f64());
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
                rx,
                store,
                processor,
                batch_max_size,
                batch_max_age,
            };
            tokio::spawn(async move { worker.run().await });
        }

        Ok(Self {
            senders,
            rr: AtomicUsize::new(0),
        })
    }

    pub async fn dispatch(&self, mut job: Incoming) {
        let n = self.senders.len();
        let mut idx = self.rr.fetch_add(1, Ordering::Relaxed) % n;

        for _ in 0..n {
            match self.senders[idx].send(job).await {
                Ok(()) => return,
                Err(e) => {
                    job = e.0;
                    idx = (idx + 1) % n;
                }
            }
        }
        tracing::warn!("all workers unavailable; dropping job");
    }
}
