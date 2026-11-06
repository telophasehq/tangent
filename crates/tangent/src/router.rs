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
    group::Grouper,
    inference::lines,
    sinks::manager::SinkManager,
    worker::{Ack, Record, WorkerPool},
    CONSUMER_BYTES_TOTAL, CONSUMER_OBJECTS_TOTAL, UNROUTED_TOTAL,
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

/// The frames, in order, as writes of up to `cap` bytes each, a frame never
/// split: one bigger than `cap` is a write of its own. A source's frames are
/// cut from one buffer, so joining them back is mostly free.
fn joined(frames: Vec<BytesMut>, cap: usize) -> Vec<BytesMut> {
    let mut out: Vec<BytesMut> = Vec::with_capacity(1);
    for frame in frames {
        match out.last_mut() {
            Some(w) if w.len() + frame.len() <= cap => {
                if !w.ends_with(b"\n") {
                    w.extend_from_slice(b"\n");
                }
                w.unsplit(frame);
            }
            _ => out.push(frame),
        }
    }
    out
}

/// The frames' bytes and records, a record a line as a worker counts them:
/// a frame of a polling source holds many.
fn consumed(frames: &[BytesMut]) -> (u64, u64) {
    frames.iter().fold((0, 0), |(bytes, records), f| {
        let unended = !f.is_empty() && !f.ends_with(b"\n");
        let lines = memchr::memchr_iter(b'\n', f).count() + usize::from(unended);
        (bytes + f.len() as u64, records + lines as u64)
    })
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
    /// Each grouped plugin's grouping task, by plugin.
    groupers: OnceCell<std::collections::HashMap<Arc<str>, Grouper>>,
    sink_manager: Arc<SinkManager>,
    /// Records handed to a worker and not yet through their walk of the DAG.
    /// Zero means the DAG is quiet.
    in_flight: Arc<AtomicUsize>,
    /// Nodes already named for a record none of their targets took.
    unrouted: Mutex<HashSet<NodeRef>>,
    /// The bytes a source's frames are joined into for one sink write.
    sink_batch: usize,
}

impl Router {
    pub fn new(outs: HashMap<NodeRef, Vec<NodeRef>>, sink_manager: Arc<SinkManager>) -> Self {
        Self {
            outs,
            pool: OnceCell::new(),
            groupers: OnceCell::new(),
            sink_manager,
            in_flight: Arc::new(AtomicUsize::new(0)),
            unrouted: Mutex::new(HashSet::default()),
            sink_batch: 0,
        }
    }

    /// A source wired straight to sinks has its frames joined into writes of
    /// up to `bytes`, as a worker's batch is, rather than a write a frame: a
    /// line-oriented source makes a frame of each line, and a write costs a
    /// sink a transaction, an append, a task.
    #[must_use]
    pub fn with_sink_batch(mut self, bytes: usize) -> Self {
        self.sink_batch = bytes;
        self
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

    pub fn set_groupers(&self, groupers: std::collections::HashMap<Arc<str>, Grouper>) {
        let _ = self.groupers.set(groupers);
    }

    /// The grouping task of plugin `name`, if it groups.
    pub fn grouper(&self, name: &str) -> Option<Grouper> {
        self.groupers.get()?.get(name).cloned()
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
                targets = %names(self.outs(from)),
                "a record from this source matched none of its targets and was dropped; \
                 tangent_unrouted_records_total counts them"
            );
        }
    }

    /// Records from a source into the DAG. With a plugin or an inference node
    /// among the source's outputs, each record goes to the worker pool once,
    /// and the worker walks the DAG from the source, its sinks included; with
    /// only sinks, the frames go to them whole, joined into writes of up to
    /// the sink batch.
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
                    payload: record,
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
        // a worker counts what it is handed; nothing does on this path
        let (bytes, records) = consumed(&frames);
        CONSUMER_BYTES_TOTAL.inc_by(bytes);
        CONSUMER_OBJECTS_TOTAL.inc_by(records);
        let mut frames = joined(frames, self.sink_batch);
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
                if let NodeRef::Sink { name } = to {
                    self.sink_manager
                        .enqueue(name.clone(), frame.clone(), vec![Arc::clone(&shared)])
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
        NodeRef::Sink { name } => format!("sink:{name}"),
    }
}

/// A node's name, as an edge writes it.
fn node_name(n: &NodeRef) -> &str {
    match n {
        NodeRef::Source { name }
        | NodeRef::Plugin { name }
        | NodeRef::Inference { name }
        | NodeRef::Sink { name } => name,
    }
}

/// Nodes by name, as an edge's `to` writes them: `[aws, lake]`.
fn names(nodes: &[NodeRef]) -> String {
    let names: Vec<&str> = nodes.iter().map(node_name).collect();
    format!("[{}]", names.join(", "))
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

    /// The warning names a source's targets as its edge does, not as Rust
    /// debug-prints them.
    #[test]
    fn targets_are_named_as_an_edge_writes_them() {
        let to = [
            NodeRef::Plugin {
                name: Arc::from("aws"),
            },
            NodeRef::Sink {
                name: Arc::from("lake"),
            },
        ];
        assert_eq!(names(&to), "[aws, lake]");
        assert_eq!(names(&[]), "[]");
    }

    #[test]
    fn frames_are_joined_up_to_the_cap_and_never_split() {
        let mut buf = BytesMut::from("{\"n\":1}\n{\"n\":2}\n{\"n\":3}\n{\"n\":4}");
        let frames: Vec<BytesMut> = (0..4).map(|_| buf.split_to(8.min(buf.len()))).collect();
        let writes = joined(frames.clone(), 24);
        assert_eq!(writes, ["{\"n\":1}\n{\"n\":2}\n{\"n\":3}\n", "{\"n\":4}"]);
        assert_eq!(
            joined(frames.clone(), 0).len(),
            4,
            "no cap, a write a frame"
        );
        let big = joined(vec![BytesMut::from("{\"a\":1}"), BytesMut::from("{}")], 4);
        assert_eq!(big, ["{\"a\":1}", "{}"]);
        let unended = joined(
            vec![BytesMut::from("{\"a\":1}"), BytesMut::from("{}\n")],
            64,
        );
        assert_eq!(unended, ["{\"a\":1}\n{}\n"]);
    }

    #[test]
    fn consumed_counts_a_record_a_line() {
        let frames = [
            BytesMut::from("{\"n\":1}\n{\"n\":2}\n"),
            BytesMut::from("{\"n\":3}"),
            BytesMut::new(),
        ];
        assert_eq!(consumed(&frames), (23, 3));
    }

    #[derive(Default)]
    struct Writes(tokio::sync::Mutex<Vec<BytesMut>>);

    #[async_trait]
    impl crate::sinks::manager::Sink for Writes {
        async fn write(&self, req: crate::sinks::manager::SinkWrite) -> Result<()> {
            self.0.lock().await.push(req.payload);
            Ok(())
        }
    }

    #[derive(Default)]
    struct Acked(AtomicUsize);

    #[async_trait]
    impl Ack for Acked {
        async fn ack(&self) -> Result<()> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn a_sources_lines_reach_a_sink_in_batches_and_are_acked_once() {
        let writes = Arc::new(Writes::default());
        let sinks: Vec<(Arc<str>, Arc<dyn crate::sinks::manager::Sink>)> =
            vec![(Arc::from("logs"), writes.clone())];
        let manager = Arc::new(SinkManager::for_test(sinks, 1));
        let source = NodeRef::Source {
            name: Arc::from("lines"),
        };
        let mut outs = HashMap::default();
        outs.insert(
            source.clone(),
            vec![NodeRef::Sink {
                name: Arc::from("logs"),
            }],
        );
        let router = Router::new(outs, Arc::clone(&manager)).with_sink_batch(1024);
        let mut buf = BytesMut::from("{\"n\":1}\n{\"n\":2}\n{\"n\":3}\n");
        let frames = (0..3).map(|_| buf.split_to(8)).collect();
        // other tests count too, at the same time: a delta, at least this
        let (bytes, objects) = (CONSUMER_BYTES_TOTAL.get(), CONSUMER_OBJECTS_TOTAL.get());
        let acked = Arc::new(Acked::default());
        router
            .forward(&source, frames, vec![acked.clone() as Arc<dyn Ack>])
            .await
            .unwrap();
        drop(router);
        let Ok(manager) = Arc::try_unwrap(manager) else {
            panic!("the router held the only other reference")
        };
        manager.join().await.unwrap();
        let writes = writes.0.lock().await;
        assert_eq!(*writes, ["{\"n\":1}\n{\"n\":2}\n{\"n\":3}\n"]);
        assert!(
            CONSUMER_BYTES_TOTAL.get() - bytes >= 24,
            "consumed, counted"
        );
        assert!(CONSUMER_OBJECTS_TOTAL.get() - objects >= 3);
        assert_eq!(acked.0.load(Ordering::SeqCst), 1);
    }
}
