use ahash::AHasher;
use anyhow::Result;
use async_trait::async_trait;
use bytes::Bytes;
use rand::{rng, Rng};
use std::collections::HashMap;
use std::hash::Hasher;
use std::{sync::Arc, time::Duration};
use tangent_shared::sinks::common::{SinkConfig, SinkKind};
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{sleep, Instant};

use crate::sinks::file;
use crate::INFLIGHT;
use crate::{
    sinks::{s3, wal},
    worker::Ack,
};

pub struct SinkWrite {
    pub sink_name: String,
    pub payload: Bytes,
    pub s3: Option<s3::S3SinkItem>,
}

#[async_trait]
pub trait Sink: Send + Sync {
    async fn write(&self, req: SinkWrite) -> Result<()>;

    async fn flush(&self) -> Result<()> {
        Ok(())
    }
}

pub struct SinkItem {
    pub acks: Vec<Arc<dyn Ack>>,
    pub req: SinkWrite,
}

struct Shard {
    tx: mpsc::Sender<SinkItem>,
    handle: JoinHandle<()>,
}

pub struct SinkManager {
    shards: Vec<Shard>,
    sinks: HashMap<String, Arc<dyn Sink>>,
}

impl SinkManager {
    pub async fn new(name: &String, cfg: &SinkConfig, queue_capacity: usize) -> Result<Self> {
        let mut sinks = HashMap::new();

        match &cfg.kind {
            SinkKind::S3(s3cfg) => {
                let remote = Arc::new(s3::S3Sink::new(name, s3cfg).await?);

                let s3_sink = wal::DurableFileSink::new(
                    remote,
                    s3cfg.wal_path.clone(),
                    cfg.common.in_flight_limit,
                    cfg.common.object_max_bytes,
                    Duration::from_secs(s3cfg.max_file_age_seconds),
                    cfg.common.compression.clone(),
                    cfg.common.encoding.clone(),
                )
                .await?;
                sinks.insert(name.clone(), s3_sink as Arc<dyn Sink>);
            }
            SinkKind::File(filecfg) => {
                let file_sink = file::FileSink::new(filecfg.path.clone()).await?;
                sinks.insert(name.clone(), file_sink);
            }
        };

        let num_shards = 4usize;
        let mut shards = Vec::with_capacity(num_shards);
        let sem = Arc::new(Semaphore::new(cfg.common.in_flight_limit));

        for _ in 0..num_shards {
            let (tx, mut rx) = mpsc::channel::<SinkItem>(queue_capacity);
            let sinks_map = sinks.clone();
            let sem = sem.clone();

            let handle = tokio::spawn(async move {
                let mut js = JoinSet::new();

                loop {
                    tokio::select! {
                        maybe = rx.recv() => {
                            let mut item = match maybe { Some(it) => it, None => break };

                            let sink_name = item.req.sink_name;

                            let sink = match sinks_map.get(&sink_name) {
                                Some(s) => s.clone(),
                                None => {
                                    tracing::warn!("no sink named '{sink_name}'; dropping item");
                                    for a in item.acks.drain(..) { let _ = a.ack().await; }
                                    continue;
                                }
                            };

                            let permit = match sem.clone().acquire_owned().await {
                                Ok(p) => p,
                                Err(_) => break,
                            };

                            js.spawn(async move {
                                let _permit: OwnedSemaphorePermit = permit;
                                let start = Instant::now();
                                let mut delay = Duration::from_millis(50);
                                loop {
                                    match sink.write(SinkWrite {
                                        sink_name: sink_name.clone(),
                                        payload: item.req.payload.clone(),
                                        s3: item.req.s3.clone(),
                                    }).await {
                                        Ok(()) => {
                                            for a in item.acks.drain(..) {
                                                if let Err(e) = a.ack().await {
                                                    tracing::warn!("ack failed: {e}");
                                                }
                                            }
                                            tracing::debug!(
                                                bytes = item.req.payload.len(),
                                                took_us = start.elapsed().as_micros(),
                                                "wrote route chunk"
                                            );
                                            INFLIGHT.dec();
                                            break;
                                        }
                                        Err(e) => {
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
            });

            shards.push(Shard { tx, handle });
        }

        Ok(Self { shards, sinks })
    }

    pub async fn enqueue(
        &self,
        sink_name: String,
        key_prefix: Option<String>,
        payload: Bytes,
        acks: Vec<Arc<dyn Ack>>,
    ) -> Result<()> {
        let route_key = if let Some(prefix) = &key_prefix {
            format!("{}|{}", sink_name, prefix)
        } else {
            sink_name.clone()
        };

        let shard_ix = (hash_route(&route_key) as usize) % self.shards.len();

        let item: Option<SinkItem> = if let Some(_sink) = self.sinks.get(&sink_name) {
            let meta = s3::S3SinkItem {
                bucket_name: sink_name.clone(),
                key_prefix: key_prefix.clone(),
            };

            Some(SinkItem {
                acks,
                req: SinkWrite {
                    sink_name: sink_name.clone(),
                    payload: payload.clone(),
                    s3: Some(meta),
                },
            })
        } else {
            tracing::warn!("no sink named '{}'; dropping item", sink_name);
            None
        };

        match item {
            Some(sink_item) => self.shards[shard_ix]
                .tx
                .send(sink_item)
                .await
                .map(|_| {
                    INFLIGHT.inc();
                })
                .map_err(|e| anyhow::anyhow!("send to shard {shard_ix} failed: {e}")),
            None => anyhow::bail!("unknown sink: {sink_name}"),
        }
    }

    pub async fn join(self) -> Result<()> {
        let SinkManager {
            mut shards, sinks, ..
        } = self;

        for sh in shards.drain(..) {
            drop(sh.tx);
            if let Err(e) = sh.handle.await {
                tracing::warn!("shard join error: {e}");
            }
        }

        for (nm, s) in &sinks {
            if let Err(e) = s.flush().await {
                tracing::warn!("sink '{nm}' flush failed during shutdown: {e}");
                return Err(e);
            }
        }
        Ok(())
    }
}

fn hash_route(route_key: &str) -> u64 {
    let mut h = AHasher::default();
    h.write(route_key.as_bytes());
    h.finish()
}
