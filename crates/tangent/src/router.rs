use ahash::{AHashMap as HashMap, AHashSet as HashSet};
use anyhow::Result;
use async_trait::async_trait;
use bytes::BytesMut;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex, Weak,
};
use tangent_shared::dag::NodeRef;
use tokio::sync::OnceCell;

use crate::{
    inference::lines,
    sinks::manager::SinkManager,
    worker::{Ack, Payload, Record, WorkerPool},
    UNROUTED_TOTAL,
};

/// The records in a frame, one per NDJSON line. A frame with one line is
/// handed on as it is; a frame of many lines is split, because a worker
/// parses one record at a time.
fn records_in(frame: BytesMut) -> Vec<BytesMut> {
    if lines(&frame).nth(1).is_none() {
        return vec![frame];
    }
    lines(&frame)
        .map(|l| {
            let mut b = BytesMut::with_capacity(l.len() + 1);
            b.extend_from_slice(l);
            b.extend_from_slice(b"\n");
            b
        })
        .collect()
}

/// Acks its inner acks once, when the last of `n` deliveries has acked.
#[derive(Clone)]
pub(crate) struct RefCountAck {
    remaining: Arc<AtomicUsize>,
    inners: Arc<Vec<Arc<dyn Ack>>>,
}

impl RefCountAck {
    pub(crate) fn new(inner: Vec<Arc<dyn Ack>>, n: usize) -> Self {
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
    /// Records handed to a worker and not yet through their walk of the DAG.
    /// Zero means the DAG is quiet.
    in_flight: Arc<AtomicUsize>,
    /// Nodes already named for a record none of their targets took.
    unrouted: Mutex<HashSet<NodeRef>>,
}

impl Router {
    pub fn new(outs: HashMap<NodeRef, Vec<NodeRef>>, sink_manager: Arc<SinkManager>) -> Self {
        Self {
            outs,
            pool: OnceCell::new(),
            sink_manager,
            in_flight: Arc::new(AtomicUsize::new(0)),
            unrouted: Mutex::new(HashSet::default()),
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

    /// Where records from `node` go next; nothing where the DAG ends.
    pub fn outs(&self, node: &NodeRef) -> &[NodeRef] {
        self.outs.get(node).map_or(&[], Vec::as_slice)
    }

    pub fn sinks(&self) -> &Arc<SinkManager> {
        &self.sink_manager
    }

    /// `n` records from `from` that none of its targets took: each a plugin
    /// whose probe they did not match. They are dropped and acked, so they
    /// are counted. A source's are named once too: past the source, a probe
    /// that takes nothing is a filter doing its job (`fresh` drops context
    /// records so), but a record from a source no target takes is data lost,
    /// such as a `source` value no plugin on a fan-out claims.
    pub fn unrouted(&self, from: &NodeRef, n: usize) {
        UNROUTED_TOTAL
            .with_label_values(&[&node_label(from)])
            .inc_by(n as u64);
        if !matches!(from, NodeRef::Source { .. }) {
            return;
        }
        let first = self
            .unrouted
            .lock()
            .map(|mut seen| seen.insert(from.clone()))
            .unwrap_or(false);
        if first {
            tracing::warn!(
                node = %node_label(from),
                targets = ?self.outs(from),
                "a record from this source matched none of its targets and was dropped; \
                 tangent_unrouted_records_total counts them"
            );
        }
    }

    /// Records from a source into the DAG. With a plugin or an inference node
    /// among the source's outputs, each record goes to the worker pool once,
    /// and the worker walks the DAG from the source, its sinks included; with
    /// only sinks, the frames go to them whole, as they are.
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

        let needs_worker = tos
            .iter()
            .any(|to| matches!(to, NodeRef::Plugin { .. } | NodeRef::Inference { .. }));
        if needs_worker {
            let Some(pool) = self.pool() else {
                anyhow::bail!(
                    "router called before pool set (from={:?}, tos={:?}, frames={})",
                    from,
                    tos,
                    frames.len(),
                );
            };
            let records: Vec<BytesMut> = frames.drain(..).flat_map(records_in).collect();
            if records.is_empty() {
                for a in acks {
                    let _ = a.ack().await;
                }
                return Ok(());
            }
            let shared: Arc<dyn Ack> = Arc::new(RefCountAck::new(acks, records.len()));
            for record in records {
                pool.dispatch(Record {
                    payload: Payload::Raw(record),
                    ack: Some(Arc::clone(&shared)),
                    from: from.clone(),
                })
                .await?;
            }
            return Ok(());
        }

        let sinks: Vec<&NodeRef> = tos
            .iter()
            .filter(|to| matches!(to, NodeRef::Sink { .. }))
            .collect();
        let deliveries = frames.len() * sinks.len();
        if deliveries == 0 {
            for a in acks {
                let _ = a.ack().await;
            }
            return Ok(());
        }
        let shared: Arc<dyn Ack> = Arc::new(RefCountAck::new(acks, deliveries));
        for frame in frames.drain(..) {
            for to in &sinks {
                if let NodeRef::Sink { name, key_prefix } = to {
                    self.sink_manager
                        .enqueue(
                            name.clone(),
                            key_prefix.clone(),
                            frame.clone(),
                            vec![Arc::clone(&shared)],
                        )
                        .await?;
                }
            }
        }
        Ok(())
    }
}

/// `kind:name`, for a metric's label.
fn node_label(n: &NodeRef) -> String {
    match n {
        NodeRef::Source { name } => format!("source:{name}"),
        NodeRef::Plugin { name } => format!("plugin:{name}"),
        NodeRef::Inference { name } => format!("inference:{name}"),
        NodeRef::Sink { name, .. } => format!("sink:{name}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_frame_of_many_lines_is_many_records() {
        let one = records_in(BytesMut::from("{\"a\":1}\n"));
        assert_eq!(one.len(), 1);
        assert_eq!(&one[0][..], b"{\"a\":1}\n");
        let many = records_in(BytesMut::from("{\"a\":1}\n\n{\"a\":2}\n{\"a\":3}"));
        assert_eq!(many.len(), 3);
        assert_eq!(&many[1][..], b"{\"a\":2}\n");
        assert_eq!(&many[2][..], b"{\"a\":3}\n");
    }

    /// Every node's are counted; a source's are named once, a plugin's
    /// (a filter, as often as not) only counted.
    #[tokio::test]
    async fn records_no_target_took_are_counted_and_a_sources_named_once() {
        let router = Router::new(
            HashMap::default(),
            Arc::new(SinkManager::for_test(Vec::new(), 1)),
        );
        let counted = |label: &str| UNROUTED_TOTAL.with_label_values(&[label]).get();
        let source = NodeRef::Source {
            name: Arc::from("unrouted-test"),
        };
        router.unrouted(&source, 2);
        router.unrouted(&source, 3);
        assert_eq!(counted("source:unrouted-test"), 5);
        let filter = NodeRef::Plugin {
            name: Arc::from("unrouted-test"),
        };
        router.unrouted(&filter, 4);
        assert_eq!(counted("plugin:unrouted-test"), 4);
        let named = router.unrouted.lock().unwrap();
        assert_eq!(named.len(), 1, "the source, once");
        assert!(named.contains(&source));
    }
}
