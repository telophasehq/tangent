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
    inference::InferenceNodes,
    sinks::manager::SinkManager,
    worker::{Ack, Payload, Record, WorkerPool},
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
    inference: OnceCell<Weak<InferenceNodes>>,
    sink_manager: Arc<SinkManager>,
    /// Records handed to a worker or an inference node and not yet forwarded
    /// on. One count for both, so zero means the DAG is quiet: a record
    /// leaving a model for a plugin is counted in before it is counted out.
    in_flight: Arc<AtomicUsize>,
}

impl Router {
    pub fn new(outs: HashMap<NodeRef, Vec<NodeRef>>, sink_manager: Arc<SinkManager>) -> Self {
        Self {
            outs,
            pool: OnceCell::new(),
            inference: OnceCell::new(),
            sink_manager,
            in_flight: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Records between a source and a sink right now.
    pub fn in_flight(&self) -> usize {
        self.in_flight.load(Ordering::Acquire)
    }

    pub(crate) fn in_flight_counter(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.in_flight)
    }

    pub fn set_pool(&self, pool: &Arc<WorkerPool>) {
        let _ = self.pool.set(Arc::downgrade(pool));
    }
    #[inline]
    fn pool(&self) -> Option<Arc<WorkerPool>> {
        self.pool.get().and_then(|w| w.upgrade())
    }

    pub fn set_inference(&self, nodes: &Arc<InferenceNodes>) {
        let _ = self.inference.set(Arc::downgrade(nodes));
    }
    #[inline]
    fn inference(&self) -> Option<Arc<InferenceNodes>> {
        self.inference.get().and_then(|w| w.upgrade())
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
        if tos.iter().any(|to| matches!(to, NodeRef::Plugin { .. })) && pool.is_none() {
            anyhow::bail!(
                "router called before pool set (from={:?}, tos={:?}, frames={})",
                from,
                tos,
                frames.len(),
            );
        }

        let inference = self.inference();
        if tos.iter().any(|to| matches!(to, NodeRef::Inference { .. })) && inference.is_none() {
            anyhow::bail!(
                "router called before inference nodes set (from={:?}, tos={:?})",
                from,
                tos
            );
        }

        let shared = Arc::new(RefCountAck::new(acks, deliveries));

        if tos.len() == 1 {
            let to = &tos[0];
            for frame in frames.drain(..) {
                match to {
                    NodeRef::Plugin { name } => {
                        let pool = pool.as_ref().expect("pool must be set for plugin edges");
                        let rec = Record {
                            payload: Payload::Raw(frame),
                            ack: Some(shared.clone()),
                            to: Some(Arc::clone(name)),
                        };
                        pool.dispatch(rec).await?;
                    }
                    NodeRef::Inference { name } => {
                        let pm = inference
                            .as_ref()
                            .expect("inference nodes must be set for inference edges");
                        let rec = Record {
                            payload: Payload::Raw(frame),
                            ack: Some(shared.clone()),
                            to: None,
                        };
                        pm.dispatch(name, rec).await?;
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
                    NodeRef::Plugin { name } => {
                        if let Some(ref pool) = pool {
                            let rec = Record {
                                payload: Payload::Raw(frame.clone()),
                                ack: Some(shared.clone()),
                                to: Some(Arc::clone(name)),
                            };
                            pool.dispatch(rec).await?;
                        } else {
                            let _ = shared.ack().await;
                        }
                    }
                    NodeRef::Inference { name } => match &inference {
                        Some(pm) => {
                            let rec = Record {
                                payload: Payload::Raw(frame.clone()),
                                ack: Some(shared.clone()),
                                to: None,
                            };
                            pm.dispatch(name, rec).await?;
                        }
                        None => {
                            let _ = shared.ack().await;
                        }
                    },
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
