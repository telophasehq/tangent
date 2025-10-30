use ahash::AHashMap as HashMap;
use anyhow::Result;
use async_trait::async_trait;
use bytes::BytesMut;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Weak,
};
use tangent_shared::dag::NodeRef;
use tokio::sync::OnceCell;

use crate::{
    sinks::manager::SinkManager,
    worker::{Ack, Record, WorkerPool},
};

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

pub struct Router {
    outs: HashMap<NodeRef, Vec<NodeRef>>,
    pool: OnceCell<Weak<WorkerPool>>,
    sink_manager: Arc<SinkManager>,
}

impl Router {
    pub fn new(outs: HashMap<NodeRef, Vec<NodeRef>>, sink_manager: Arc<SinkManager>) -> Self {
        Self {
            outs,
            pool: OnceCell::new(),
            sink_manager,
        }
    }

    pub fn set_pool(&self, pool: &Arc<WorkerPool>) {
        let _ = self.pool.set(Arc::downgrade(pool));
    }
    #[inline]
    fn pool(&self) -> Option<Arc<WorkerPool>> {
        self.pool.get().and_then(|w| w.upgrade())
    }

    pub async fn forward(
        &self,
        from: &NodeRef,
        mut frames: Vec<BytesMut>,
        acks: Vec<Arc<dyn Ack>>,
    ) -> Result<()> {
        let Some(tos) = self.outs.get(from) else {
            tracing::warn!("no output from node: {:?}", from);
            for a in acks {
                let _ = a.ack().await;
            }
            return Ok(());
        };

        let deliveries = frames.len() * tos.len();
        if deliveries == 0 {
            for a in acks {
                let _ = a.ack().await;
            }
            return Ok(());
        }

        let pool = self.pool();

        let shared = Arc::new(RefCountAck::new(acks, deliveries));
        if tos.len() == 1 {
            let to = &tos[0];
            for frame in frames.drain(..) {
                match to {
                    NodeRef::Plugin { .. } => {
                        if let Some(ref pool) = pool {
                            let rec = Record {
                                payload: frame,
                                ack: Some(shared.clone()),
                            };
                            pool.dispatch(rec).await?;
                        } else {
                            let _ = shared.ack().await;
                        }
                    }
                    NodeRef::Sink { name, key_prefix } => {
                        self.sink_manager
                            .enqueue(
                                name.clone(),
                                key_prefix.clone(),
                                frame,
                                vec![shared.clone()],
                            )
                            .await?;
                    }
                    NodeRef::Source { .. } => {
                        let _ = shared.ack().await;
                    }
                }
            }
            return Ok(());
        }

        for frame in frames.drain(..) {
            for to in tos {
                match to {
                    NodeRef::Plugin { .. } => {
                        if let Some(ref pool) = pool {
                            let rec = Record {
                                payload: frame.clone(),
                                ack: Some(shared.clone()),
                            };
                            pool.dispatch(rec).await?;
                        } else {
                            let _ = shared.ack().await;
                        }
                    }
                    NodeRef::Sink { name, key_prefix } => {
                        self.sink_manager
                            .enqueue(
                                name.clone(),
                                key_prefix.clone(),
                                frame.clone(),
                                vec![shared.clone()],
                            )
                            .await?;
                    }
                    NodeRef::Source { .. } => {
                        let _ = shared.ack().await;
                    }
                }
            }
        }
        Ok(())
    }
}
