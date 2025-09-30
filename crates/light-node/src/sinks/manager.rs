use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use std::{sync::Arc, time::Duration};
use tangent_shared::{SinkConfig, SinkKind};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::INFLIGHT;
use crate::{
    sinks::{s3, wal},
    worker::Ack,
};

#[async_trait]
pub trait Sink: Send + Sync {
    async fn write(&self, payload: Bytes) -> anyhow::Result<()>;

    async fn flush(&self) -> anyhow::Result<()> {
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
}

impl SinkManager {
    pub async fn new(name: &String, cfg: &SinkConfig, queue_capacity: usize) -> Result<Self> {
        let mut handles = Vec::with_capacity(1usize);
        let sink: Arc<dyn Sink> = match &cfg.kind {
            SinkKind::S3(s3cfg) => {
                let remote = Arc::new(s3::S3Sink::new(name, s3cfg).await?);

                Arc::new(
                    wal::DurableFileSink::new(
                        remote,
                        cfg.common.wal_path.clone(),
                        cfg.common.object_max_bytes,
                    )
                    .await?,
                )
            }
        };

        let (tx, mut rx) = mpsc::channel::<SinkItem>(queue_capacity);
        let sem = Arc::new(tokio::sync::Semaphore::new(cfg.common.in_flight_limit));
        let h = tokio::spawn({
            let sem = sem.clone();
            let sink_for_worker = sink.clone();
            async move {
                while let Some(mut item) = rx.recv().await {
                    let sink = sink_for_worker.clone();
                    let sem = sem.clone();

                    let mut delay = Duration::from_millis(50);
                    let permit = sem.acquire_owned().await.unwrap();
                    tokio::spawn(async move {
                        let start = Instant::now();
                        for attempt in 0.. {
                            match sink.write(item.payload.clone()).await {
                                Ok(()) => {
                                    for a in item.acks.drain(..) {
                                        if let Err(e) = a.ack().await {
                                            tracing::warn!("ack failed: {e}");
                                        }
                                    }

                                    tracing::info!(
                                        target: "sidecar",
                                        bytes = item.payload.len(),
                                        took_us = start.elapsed().as_micros(),
                                        "wrote batch"
                                    );
                                    INFLIGHT.dec();
                                    break;
                                }
                                Err(e) => {
                                    tracing::warn!("sink write failed (attempt {attempt}): {e}");
                                    // TODO: cap attempts → DLQ
                                    tokio::time::sleep(delay).await;
                                    delay = delay.saturating_mul(2).min(Duration::from_secs(5));
                                }
                            }
                        }
                        drop(permit);
                    });
                }
            }
        });
        handles.push(h);
        Ok(Self {
            tx: tx,
            handles: handles,
            sink: sink,
        })
    }

    pub async fn enqueue(&self, item: SinkItem) -> Result<(), mpsc::error::SendError<SinkItem>> {
        INFLIGHT.inc();
        self.tx.send(item).await
    }

    pub async fn join(self) {
        for h in self.handles {
            let _ = h.await;
        }

        if let Err(e) = self.sink.flush().await {
            tracing::warn!("sink flush failed during shutdown: {e}");
        }
    }
}
