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
    pub sink_name: Arc<str>,
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
    S3 {
        sink: Arc<dyn Sink>,
        bucket: Arc<str>,
    },
    Other {
        sink: Arc<dyn Sink>,
    },
}

pub struct SinkManager {
    shards: Vec<Shard>,
    sinks: Arc<HashMap<Arc<str>, SinkEntry>>,
}

impl SinkManager {
    pub async fn new(cfgs: &BTreeMap<Arc<str>, SinkConfig>) -> Result<Self> {
        let mut sinks: HashMap<Arc<str>, SinkEntry> = HashMap::with_capacity(cfgs.len());

        let total_inflight: usize = cfgs.values().map(|c| c.common.in_flight_limit).sum();

        for (name, cfg) in cfgs {
            match &cfg.kind {
                SinkKind::S3(s3cfg) => {
                    let bucket: Arc<str> = Arc::<str>::from(s3cfg.bucket_name.clone());
                    let remote = Arc::new(s3::S3Sink::new(Arc::clone(&name), bucket).await?);
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
                        Arc::clone(&name),
                        SinkEntry::S3 {
                            sink: s3_sink as Arc<dyn Sink>,
                            bucket: Arc::<str>::from(s3cfg.bucket_name.clone()),
                        },
                    );
                }
                SinkKind::File(filecfg) => {
                    let file_sink = file::FileSink::new(filecfg, &cfg.common).await?;
                    sinks.insert(Arc::clone(&name), SinkEntry::Other { sink: file_sink });
                }
                SinkKind::Blackhole(_) => {
                    let bh = blackhole::BlackholeSink::new();
                    sinks.insert(Arc::clone(&name), SinkEntry::Other { sink: bh });
                }
            }
        }

        Ok(Self::from_entries(sinks, total_inflight))
    }

    fn from_entries(sinks: HashMap<Arc<str>, SinkEntry>, total_inflight: usize) -> Self {
        let num_shards = 4usize;
        let mut shards = Vec::with_capacity(num_shards);

        let sem = Arc::new(Semaphore::new(total_inflight.max(1)));
        let sinks = Arc::new(sinks);

        for _ in 0..num_shards {
            let (tx, mut rx) = mpsc::channel::<SinkItem>(4096);
            let sinks_map = Arc::clone(&sinks);
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
                                // Move on first attempt; reconstruct from a frozen snapshot on retries
                                let frozen = item.req.payload.clone().freeze();
                                let mut first_attempt = true;
                                loop {
                                    let payload_to_send = if first_attempt {
                                        first_attempt = false;
                                        std::mem::take(&mut item.req.payload)
                                    } else {
                                        BytesMut::from(frozen.as_ref())
                                    };

                                    match sink.write(SinkWrite {
                                        sink_name: sink_name.clone(),
                                        payload: payload_to_send,
                                        s3: item.req.s3.clone(),
                                    }).await {
                                        Ok(()) => {
                                            for a in item.acks.drain(..) {
                                                if let Err(e) = a.ack().await {
                                                    tracing::warn!("ack failed: {e}");
                                                }
                                            }
                                            tracing::debug!(
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

        Self { shards, sinks }
    }

    #[cfg(test)]
    pub(crate) fn for_test(sinks: Vec<(Arc<str>, Arc<dyn Sink>)>, total_inflight: usize) -> Self {
        let entries = sinks
            .into_iter()
            .map(|(name, sink)| (name, SinkEntry::Other { sink }))
            .collect();
        Self::from_entries(entries, total_inflight)
    }

    pub async fn enqueue(
        &self,
        sink_name: Arc<str>,
        key_prefix: Option<Arc<str>>,
        payload: BytesMut,
        acks: Vec<Arc<dyn Ack>>,
    ) -> Result<()> {
        let shard_ix = {
            let mut h = AHasher::default();
            h.write(sink_name.as_bytes());
            if let Some(prefix) = key_prefix.clone() {
                h.write_u8(b'|');
                h.write(prefix.as_bytes());
            }
            (h.finish() as usize) % self.shards.len()
        };

        if !self.sinks.contains_key(&sink_name) {
            tracing::warn!("unknown sink '{}'; dropping item", sink_name);
            anyhow::bail!("unknown sink: {sink_name}");
        }

        let sink_item = SinkItem {
            acks,
            req: SinkWrite {
                sink_name: Arc::<str>::from(sink_name),
                payload: payload,
                s3: key_prefix.clone().map(|kp| s3::S3SinkItem {
                    bucket_name: Arc::<str>::from(""), // placeholder; filled in shard
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

        for (nm, entry) in sinks.iter() {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::Ack;
    use anyhow::Result;
    use async_trait::async_trait;
    use bytes::BytesMut;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Mutex;

    #[derive(Default)]
    struct RecordingSink {
        writes: Mutex<Vec<Vec<u8>>>,
    }

    impl RecordingSink {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                writes: Mutex::new(Vec::new()),
            })
        }

        async fn take(&self) -> Vec<Vec<u8>> {
            self.writes.lock().await.drain(..).collect()
        }
    }

    #[async_trait]
    impl Sink for RecordingSink {
        async fn write(&self, req: SinkWrite) -> Result<()> {
            self.writes.lock().await.push(req.payload.freeze().to_vec());
            Ok(())
        }
    }

    #[derive(Default)]
    struct TestAck {
        count: AtomicUsize,
    }

    impl TestAck {
        fn count(&self) -> usize {
            self.count.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl Ack for TestAck {
        async fn ack(&self) -> Result<()> {
            self.count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn join_drains_inflight_items() {
        let sink_name: Arc<str> = Arc::from("recorder");
        let recorder = RecordingSink::new();
        let manager = SinkManager::for_test(vec![(sink_name.clone(), recorder.clone())], 2);

        let ack = Arc::new(TestAck::default());
        let ack_dyn: Arc<dyn Ack> = ack.clone();
        manager
            .enqueue(
                sink_name.clone(),
                None,
                BytesMut::from("{\"msg\":1}\n"),
                vec![ack_dyn],
            )
            .await
            .unwrap();

        manager
            .enqueue(
                sink_name.clone(),
                None,
                BytesMut::from("{\"msg\":2}\n"),
                Vec::new(),
            )
            .await
            .unwrap();

        manager.join().await.unwrap();

        let writes = recorder.take().await;
        assert_eq!(writes.len(), 2);
        assert_eq!(ack.count(), 1);
    }
}
