use anyhow::Result;
use bytes::{Bytes, BytesMut};
use futures::{TryStream, TryStreamExt};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio::time::{self, Instant as TokioInstant};
use tokio_util::codec::{FramedRead, LinesCodec};
use wasmtime::Store;

use crate::wasm::{self, Host, Processor};
use crate::{BATCH_EVENTS, BATCH_LATENCY};

type RecordStream = Pin<
    Box<
        dyn TryStream<Ok = Bytes, Error = anyhow::Error, Item = Result<Bytes, anyhow::Error>>
            + Send
            + 'static,
    >,
>;

pub enum Incoming {
    UnixStream(tokio::net::UnixStream),
    Records(RecordStream),
}

fn unix_lines_stream(s: tokio::net::UnixStream) -> RecordStream {
    use futures::StreamExt;
    let st = FramedRead::new(s, LinesCodec::new()).map(|r| {
        r.map(|line| {
            let mut b = BytesMut::with_capacity(line.len() + 1);
            b.extend_from_slice(line.as_bytes());
            b.extend_from_slice(b"\n");
            b.freeze()
        })
        .map_err(|e| anyhow::Error::new(e))
    });
    Box::pin(st)
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
    pub async fn run(mut self) {
        while let Some(job) = self.rx.recv().await {
            let records: RecordStream = match job {
                Incoming::UnixStream(us) => unix_lines_stream(us),
                Incoming::Records(rs) => rs,
            };

            if let Err(e) = self.run_records(records).await {
                tracing::error!("worker error: {e}");
            }
        }
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

    pub async fn run_records<S>(&mut self, mut records: S) -> Result<()>
    where
        S: TryStream<Ok = Bytes, Error = anyhow::Error> + Unpin + Send + 'static,
    {
        let mut batch = BytesMut::with_capacity(self.batch_max_size);
        let mut events = 0usize;

        let mut deadline = TokioInstant::now() + self.batch_max_age;
        let sleeper = time::sleep_until(deadline);
        tokio::pin!(sleeper);

        loop {
            tokio::select! {
                item = records.try_next() => {
                    match item? {
                        None => { self.flush_batch(&mut batch, &mut events).await?; break; }
                        Some(rec) => {
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
    ) -> Result<Self> {
        let mut senders = Vec::with_capacity(size);

        let ch_capacity = batch_max_size * 2;
        for i in 0..size {
            let (tx, rx) = mpsc::channel::<Incoming>(ch_capacity);
            senders.push(tx);

            let mut store = engine.make_store();
            let processor = engine.make_processor(&mut store).await?;

            let start = Instant::now();
            match processor.call_process_logs(&mut store, b"{}").await? {
                Ok(_) => {
                    BATCH_LATENCY.observe(start.elapsed().as_secs_f64());
                    tracing::info!(target:"sidecar", "worker {i} warmup in {} µs", start.elapsed().as_micros());
                }
                Err(e) => {
                    tracing::error!(target:"sidecar", "worker {i} warmup failed after {} µs: {e}",
                                    start.elapsed().as_micros());
                }
            }

            let worker = Worker {
                id: i,
                rx,
                store: store,
                processor: processor,
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

        // TODO: wait...
        tracing::warn!("all workers unavailable; dropping job");
    }
}
