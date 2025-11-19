use ahash::AHashMap as HashMap;

use anyhow::{anyhow, Context, Result};
use bytes::BytesMut;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tangent_shared::{dag::NodeRef, Config};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use wasmtime::component::Component;

use crate::{
    router::Router,
    sinks::manager::SinkManager,
    wasm::engine::WasmEngine,
    worker::{Ack, WorkerPool},
};

#[derive(Clone)]
pub struct DagRuntime {
    pub router: Arc<Router>,
    pool: Arc<WorkerPool>,
    sink_manager: Arc<SinkManager>,
}

impl DagRuntime {
    pub async fn build(cfg: &Config, cfg_path: &PathBuf) -> anyhow::Result<Self> {
        let sink_manager = Arc::new(SinkManager::new(&cfg.sinks).await?);
        let config_dir = cfg_path.parent().unwrap_or_else(|| Path::new("."));
        let plugin_root = config_dir.join(&cfg.runtime.plugins_path).canonicalize()?;

        let engines: Vec<WasmEngine> = (0..cfg.runtime.workers)
            .map(|_| WasmEngine::new())
            .collect::<Result<_, _>>()?;
        let mut components: Vec<Vec<(Arc<str>, Component)>> =
            Vec::with_capacity(cfg.runtime.workers);
        for i in 0..cfg.runtime.workers {
            components.push(Vec::<(Arc<str>, Component)>::new());
            for (name, _) in &cfg.plugins {
                let component_file = format!("{name}.cwasm");
                let plugin_path = plugin_root
                    .join(&component_file)
                    .canonicalize()
                    .with_context(|| {
                        format!(
                            "canonicalizing path: {}/{}",
                            plugin_root.display(),
                            &component_file
                        )
                    })?;

                components[i].push((
                    Arc::clone(name),
                    engines[i]
                        .load_precompiled(&plugin_path)
                        .with_context(|| format!("loading {}", &component_file))?,
                ));
            }
        }

        let mut outs: HashMap<NodeRef, Vec<NodeRef>> = HashMap::default();
        for e in &cfg.dag {
            outs.entry(e.from.clone()).or_default().extend(e.to.clone());
        }

        let router = Arc::new(Router::new(outs, Arc::clone(&sink_manager)));

        let pool = Arc::new(
            WorkerPool::new(
                cfg.runtime.workers,
                engines,
                components,
                cfg.batch_size_kb(),
                cfg.batch_age_ms(),
                Arc::clone(&router),
            )
            .await?,
        );

        router.set_pool(&pool);

        Ok(Self {
            router,
            pool,
            sink_manager,
        })
    }

    pub async fn push_from_source(
        &self,
        source_name: Arc<str>,
        frames: Vec<BytesMut>,
        acks: Vec<Arc<dyn Ack>>,
    ) -> anyhow::Result<()> {
        let from = NodeRef::Source { name: source_name };
        self.router.forward(&from, frames, acks).await
    }

    pub async fn shutdown(
        self,
        cancel: CancellationToken,
        worker_timeout: Duration,
        sink_timeout: Duration,
    ) -> Result<()> {
        cancel.cancel();

        let Self {
            router,
            pool,
            sink_manager,
        } = self;

        tracing::info!("waiting on workers to shutdown...");

        if let Err(e) = timeout(worker_timeout, pool.join()).await {
            tracing::warn!(?e, "pool shutdown timeout exceeded. Logs may be dropped.");
        }

        drop(router);

        tracing::info!("waiting on sink manager to shutdown...");
        let sink_owned = Arc::try_unwrap(sink_manager)
            .map_err(|_| anyhow!("SinkManager still has refs; drop all clones before shutdown"))?;
        match timeout(sink_timeout, sink_owned.join()).await {
            Err(e) => {
                tracing::warn!(
                    ?e,
                    "sink manager shutdown timeout exceeded. Logs may be dropped."
                )
            }
            Ok(_) => (),
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sinks::manager::{Sink, SinkManager, SinkWrite};
    use crate::worker::Ack;
    use anyhow::Result;
    use async_trait::async_trait;
    use bytes::BytesMut;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use tokio::sync::{Mutex, Notify};

    #[derive(Default)]
    struct BlockingSink {
        writes: Mutex<Vec<Vec<u8>>>,
        flush_ready: Notify,
        flush_called: AtomicBool,
    }

    impl BlockingSink {
        fn new() -> Arc<Self> {
            Arc::new(Self {
                writes: Mutex::new(Vec::new()),
                flush_ready: Notify::new(),
                flush_called: AtomicBool::new(false),
            })
        }

        fn allow_flush(&self) {
            self.flush_ready.notify_waiters();
        }

        async fn write_count(&self) -> usize {
            self.writes.lock().await.len()
        }

        fn flush_completed(&self) -> bool {
            self.flush_called.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl Sink for BlockingSink {
        async fn write(&self, req: SinkWrite) -> Result<()> {
            self.writes.lock().await.push(req.payload.freeze().to_vec());
            Ok(())
        }

        async fn flush(&self) -> Result<()> {
            self.flush_called.store(true, Ordering::SeqCst);
            self.flush_ready.notified().await;
            Ok(())
        }
    }

    #[derive(Default)]
    struct CountingAck {
        count: AtomicUsize,
    }

    impl CountingAck {
        fn count(&self) -> usize {
            self.count.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl Ack for CountingAck {
        async fn ack(&self) -> Result<()> {
            self.count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn shutdown_waits_for_sink_flush() {
        let sink_name: Arc<str> = Arc::from("blocking");
        let blocking_sink = BlockingSink::new();
        let sink_manager = Arc::new(SinkManager::for_test(
            vec![(sink_name.clone(), blocking_sink.clone())],
            1,
        ));

        let router = Arc::new(Router::new(HashMap::default(), Arc::clone(&sink_manager)));
        let worker_pool = Arc::new(WorkerPool::new_for_test(vec![tokio::spawn(async move {})]));

        let runtime = DagRuntime {
            router,
            pool: worker_pool,
            sink_manager: Arc::clone(&sink_manager),
        };

        let ack = Arc::new(CountingAck::default());
        let ack_dyn: Arc<dyn Ack> = ack.clone();
        sink_manager
            .enqueue(
                sink_name.clone(),
                None,
                BytesMut::from("{\"msg\":\"block\"}\n"),
                vec![ack_dyn],
            )
            .await
            .unwrap();

        drop(sink_manager);

        let shutdown_token = CancellationToken::new();
        let shutdown = runtime.shutdown(
            shutdown_token,
            Duration::from_millis(200),
            Duration::from_millis(200),
        );
        tokio::pin!(shutdown);

        assert!(
            tokio::time::timeout(Duration::from_millis(50), shutdown.as_mut())
                .await
                .is_err()
        );

        blocking_sink.allow_flush();
        shutdown.await.unwrap();

        assert_eq!(blocking_sink.write_count().await, 1);
        assert!(blocking_sink.flush_completed());
        assert_eq!(ack.count(), 1);
    }
}
