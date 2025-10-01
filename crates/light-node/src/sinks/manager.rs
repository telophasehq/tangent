use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use rand::{rng, Rng};
use std::{sync::Arc, time::Duration};
use tangent_shared::{SinkConfig, SinkKind};
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{sleep, Instant};
use tokio_util::sync::CancellationToken;

use crate::INFLIGHT;
use crate::{
    sinks::{s3, wal},
    worker::Ack,
};

#[async_trait]
pub trait Sink: Send + Sync {
    async fn write(&self, payload: Bytes) -> Result<()>;

    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}

pub struct SinkItem {
    pub payload: Bytes,
    pub acks: Vec<Arc<dyn Ack>>,
}

pub struct SinkManager {
    tx: mpsc::Sender<SinkItem>,
    handles: Vec<JoinHandle<()>>,
    sink: Arc<dyn Sink>,
    cancel: CancellationToken,
}

impl SinkManager {
    pub async fn new(
        name: &String,
        cfg: &SinkConfig,
        queue_capacity: usize,
        cancel: CancellationToken,
    ) -> Result<Self> {
        let mut handles = Vec::with_capacity(1usize);

        let sink: Arc<dyn Sink> = match &cfg.kind {
            SinkKind::S3(s3cfg) => {
                let remote = Arc::new(s3::S3Sink::new(name, s3cfg).await?);

                wal::DurableFileSink::new(
                    remote,
                    cfg.common.wal_path.clone(),
                    cfg.common.in_flight_limit,
                    cfg.common.object_max_bytes,
                    Duration::from_secs(cfg.common.max_file_age_seconds),
                    cancel.clone(),
                )
                .await?
            }
        };

        let (tx, mut rx) = mpsc::channel::<SinkItem>(queue_capacity);
        let sem = Arc::new(Semaphore::new(cfg.common.in_flight_limit));

        let h = tokio::spawn({
            let sink = sink.clone();
            let sem = sem.clone();
            let cancel = cancel.clone();
            async move {
                let mut js = JoinSet::new();

                loop {
                    tokio::select! {
                        _ = cancel.cancelled() => {
                            break;
                        }
                        maybe = rx.recv() => {
                            let mut item = match maybe {
                                Some(it) => it,
                                None => break,
                            };

                            let sink = sink.clone();
                            let permit = match sem.clone().acquire_owned().await {
                                Ok(p) => p,
                                Err(_) => break,
                            };
                            let token = cancel.child_token();

                            js.spawn(async move {
                                let _permit: OwnedSemaphorePermit = permit;
                                let start = Instant::now();
                                let mut delay = Duration::from_millis(50);
                                loop {
                                    if token.is_cancelled() {
                                        INFLIGHT.dec();
                                        break;
                                    }
                                    match sink.write(item.payload.clone()).await {
                                        Ok(()) => {
                                            for a in item.acks.drain(..) {
                                                if let Err(e) = a.ack().await {
                                                    tracing::warn!("ack failed: {e}");
                                                }
                                            }
                                            tracing::debug!(bytes = item.payload.len(),
                                                            took_us = start.elapsed().as_micros(),
                                                            "wrote batch");
                                            INFLIGHT.dec();
                                            break;
                                        }
                                        Err(e) => {
                                            if token.is_cancelled() {
                                                tracing::warn!("cancelling retries due to shutdown");
                                                INFLIGHT.dec();
                                                break;
                                            }
                                            tracing::warn!("sink write failed: {e}");
                                            let j = rng().random_range(0..=delay.as_millis() as u64 / 4);
                                            sleep(delay + Duration::from_millis(j)).await;
                                            delay = (delay * 2).min(Duration::from_secs(5));
                                        }
                                    }
                                }
                            });
                        }
                    }
                }

                while js.join_next().await.is_some() {}
            }
        });
        handles.push(h);
        Ok(Self {
            tx: tx,
            handles: handles,
            sink: sink,
            cancel: cancel,
        })
    }

    pub fn cancel(&self) {
        self.cancel.cancel();
    }

    pub async fn enqueue(&self, item: SinkItem) -> Result<(), mpsc::error::SendError<SinkItem>> {
        match self.tx.send(item).await {
            Ok(()) => {
                INFLIGHT.inc();
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub async fn join(self) -> Result<()> {
        for h in self.handles {
            h.await?;
        }
        if let Err(e) = self.sink.flush().await {
            tracing::warn!("sink flush failed during shutdown: {e}");
            return Err(e);
        }
        Ok(())
    }
}
