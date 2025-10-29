use ahash::AHasher;
use anyhow::Result;
use async_trait::async_trait;
use bytes::BytesMut;
use rand::{rng, Rng};
use std::collections::{BTreeMap, HashMap};
use std::hash::Hasher;
use std::{sync::Arc, time::Duration};
use tangent_shared::sinks::common::{SinkConfig, SinkKind};
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{sleep, Instant};

use crate::sinks::blackhole;
use crate::sinks::file;
use crate::sinks::s3::S3SinkItem;
use crate::INFLIGHT;
use crate::{
    sinks::{s3, wal},
    worker::Ack,
};

pub struct SinkWrite {
    pub sink_name: String,
    pub payload: BytesMut,
    pub s3: Option<S3SinkItem>,
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

#[derive(Clone)]
enum SinkEntry {
    S3 { sink: Arc<dyn Sink>, bucket: String },
    Other { sink: Arc<dyn Sink> },
}

pub struct SinkManager {
    shards: Vec<Shard>,
    sinks: HashMap<String, SinkEntry>,
}

impl SinkManager {
    pub async fn new(cfgs: &BTreeMap<String, SinkConfig>, queue_capacity: usize) -> Result<Self> {
        let mut sinks: HashMap<String, SinkEntry> = HashMap::with_capacity(cfgs.len());

        for (name, cfg) in cfgs {
            match &cfg.kind {
                SinkKind::S3(s3cfg) => {
                    let remote = Arc::new(s3::S3Sink::new(name, &s3cfg).await?);
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
                    sinks.insert(
                        name.to_owned(),
                        SinkEntry::S3 {
                            sink: s3_sink as Arc<dyn Sink>,
                            bucket: s3cfg.bucket_name.clone(),
                        },
                    );
                }
                SinkKind::File(filecfg) => {
                    let file_sink = file::FileSink::new(filecfg, &cfg.common).await?;
                    sinks.insert(name.to_owned(), SinkEntry::Other { sink: file_sink });
                }
                SinkKind::Blackhole(_) => {
                    let bh = blackhole::BlackholeSink::new();
                    sinks.insert(name.to_owned(), SinkEntry::Other { sink: bh });
                }
            }
        }

        let num_shards = 4usize;
        let mut shards = Vec::with_capacity(num_shards);

        let total_inflight: usize = cfgs.values().map(|c| c.common.in_flight_limit).sum();
        let sem = Arc::new(Semaphore::new(total_inflight.max(1)));

        for _ in 0..num_shards {
            let (tx, mut rx) = mpsc::channel::<SinkItem>(queue_capacity);
            let sinks_map = sinks.clone();
            let sem = sem.clone();

            let handle = tokio::spawn(async move {
                let mut js = JoinSet::new();
                loop {
                    tokio::select! {
                        maybe = rx.recv() => {
                            let Some(mut item) = maybe else { break };

                            let sink_name = item.req.sink_name.clone();

                            let entry = match sinks_map.get(&sink_name) {
                                Some(e) => e,
                                None => {
                                    tracing::warn!("no sink named '{sink_name}'; dropping item");
                                    for a in item.acks.drain(..) { let _ = a.ack().await; }
                                    continue;
                                }
                            };

                            if let SinkEntry::S3 { bucket, .. } = entry {
                                let prefix = item.req.s3.as_ref().and_then(|m| m.key_prefix.clone());
                                item.req.s3 = Some(s3::S3SinkItem {
                                    bucket_name: bucket.clone(),
                                    key_prefix: prefix,
                                });
                            } else {
                                item.req.s3 = None;
                            }

                            let sink: Arc<dyn Sink> = match entry {
                                SinkEntry::S3 { sink, .. } => sink.clone(),
                                SinkEntry::Other { sink } => sink.clone(),
                            };

                            let Ok(permit) = sem.clone().acquire_owned().await else { break };

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
                                                "wrote sink item"
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
        payload: BytesMut,
        acks: Vec<Arc<dyn Ack>>,
    ) -> Result<()> {
        let route_key = key_prefix.as_ref().map_or_else(
            || sink_name.clone(),
            |prefix| format!("{sink_name}|{prefix}"),
        );
        let shard_ix = (hash_route(&route_key) as usize) % self.shards.len();

        if !self.sinks.contains_key(&sink_name) {
            tracing::warn!("unknown sink '{}'; dropping item", sink_name);
            anyhow::bail!("unknown sink: {sink_name}");
        }

        let sink_item = SinkItem {
            acks,
            req: SinkWrite {
                sink_name: sink_name.clone(),
                payload: payload.clone(),
                s3: key_prefix.map(|kp| s3::S3SinkItem {
                    bucket_name: String::new(), // placeholder; filled in shard
                    key_prefix: Some(kp),
                }),
            },
        };

        self.shards[shard_ix]
            .tx
            .send(sink_item)
            .await
            .map(|()| {
                INFLIGHT.inc();
            })
            .map_err(|e| anyhow::anyhow!("send to shard {shard_ix} failed: {e}"))
    }

    /// Drain and flush everything.
    pub async fn join(self) -> Result<()> {
        let Self { shards, sinks, .. } = self;

        for sh in shards.into_iter() {
            drop(sh.tx);
            if let Err(e) = sh.handle.await {
                tracing::warn!("shard join error: {e}");
            }
        }

        for (nm, entry) in &sinks {
            let sink: &Arc<dyn Sink> = match entry {
                SinkEntry::S3 { sink, .. } => sink,
                SinkEntry::Other { sink } => sink,
            };
            if let Err(e) = sink.flush().await {
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
