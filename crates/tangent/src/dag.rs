use ahash::AHashMap as HashMap;

use anyhow::{anyhow, Context, Result};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tangent_shared::{dag::NodeRef, sources::common::SourceConfig, Config};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use wasmtime::component::Component;

use crate::{
    cache::CacheHandle, router::Router, sinks::manager::SinkManager, sources,
    wasm::engine::WasmEngine, worker::WorkerPool,
};

pub struct DagRuntime {
    pub router: Arc<Router>,
    pool: Arc<WorkerPool>,
    sink_manager: Arc<SinkManager>,
    consumer_handles: Vec<tokio::task::JoinHandle<()>>,
}

impl DagRuntime {
    pub async fn build(
        cfg: Config,
        cfg_path: &PathBuf,
        shutdown: CancellationToken,
    ) -> anyhow::Result<Self> {
        let sink_manager = Arc::new(SinkManager::new(&cfg.sinks).await?);
        let config_dir = cfg_path.parent().unwrap_or_else(|| Path::new("."));
        let plugin_root = config_dir.join(&cfg.runtime.plugins_path).canonicalize()?;

        let workers = cfg.runtime.workers;

        let cache = Arc::new(CacheHandle::open(&cfg.runtime.cache.clone(), config_dir)?);

        let mut engines: Vec<WasmEngine> = (0..workers)
            .map(|_| WasmEngine::new(cache.clone(), cfg.runtime.disable_remote_calls))
            .collect::<Result<_, _>>()?;
        let mut components: Vec<Vec<(Arc<str>, Component)>> = Vec::with_capacity(workers);
        for i in 0..workers {
            components.push(Vec::<(Arc<str>, Component)>::new());
            for (name, plugin_cfg) in &cfg.plugins {
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
                        .load_precompiled(Arc::clone(name), &plugin_path, plugin_cfg.config.clone())
                        .with_context(|| format!("loading {}", &component_file))?,
                ));
            }
        }

        let mut outs: HashMap<NodeRef, Vec<NodeRef>> = HashMap::default();
        for e in &cfg.dag {
            outs.entry(e.from.clone()).or_default().extend(e.to.clone());
        }

        let router = Arc::new(Router::new(outs, Arc::clone(&sink_manager)));

        let batch_size = cfg.batch_size_kb();
        let batch_age = cfg.batch_age_ms();
        let sources = cfg.sources;

        let pool = Arc::new(
            WorkerPool::new(
                workers,
                engines,
                components,
                batch_size,
                batch_age,
                Arc::clone(&router),
            )
            .await?,
        );

        router.set_pool(&pool);

        let consumer_handles = spawn_consumers(
            sources,
            batch_size,
            router.clone(),
            shutdown.clone(),
            cache.clone(),
        );

        Ok(Self {
            router,
            pool,
            sink_manager,
            consumer_handles,
        })
    }

    pub async fn shutdown(self, worker_timeout: Duration, sink_timeout: Duration) -> Result<()> {
        let Self {
            router,
            pool,
            sink_manager,
            consumer_handles,
        } = self;

        tracing::info!("waiting on consumers to shutdown...");
        for mut h in consumer_handles {
            let sleep = tokio::time::sleep(Duration::from_secs(30));
            tokio::pin!(sleep);

            tokio::select! {
                _ = &mut sleep => {
                    tracing::warn!("consumer shutdown timeout exceeded; aborting task");
                    h.abort();
                    let _ = h.await;
                }
                res = &mut h => {
                    if let Err(e) = res {
                        tracing::warn!("consumer task panicked or was cancelled: {e}");
                    }
                }
            }
        }

        tracing::info!("waiting on workers to shutdown...");

        drop(router);

        if let Ok(pool_owned) = Arc::try_unwrap(pool) {
            if let Err(e) = timeout(worker_timeout, pool_owned.join()).await {
                tracing::warn!(?e, "pool shutdown timeout exceeded. Logs may be dropped.");
            }
        } else {
            tracing::warn!("WorkerPool still has refs; cannot consume for join()");
        }

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

fn spawn_consumers(
    sources: BTreeMap<Arc<str>, SourceConfig>,
    batch_size: usize,
    router: Arc<Router>,
    shutdown: CancellationToken,
    cache: Arc<CacheHandle>,
) -> Vec<tokio::task::JoinHandle<()>> {
    let mut handles = Vec::new();
    for src in sources {
        let shutdown = shutdown.clone();
        match src {
            (name, SourceConfig::MSK(kc)) => {
                let router = router.clone();
                handles.push(tokio::spawn(async move {
                    if let Err(e) =
                        sources::msk::run_consumer(name, kc, batch_size, router, shutdown.clone())
                            .await
                    {
                        tracing::error!("msk consumer error: {e}");
                    }
                }));
            }
            (name, SourceConfig::File(fc)) => {
                let router = router.clone();
                handles.push(tokio::spawn(async move {
                    if let Err(e) =
                        sources::file::run_consumer(name, fc, batch_size, router, shutdown.clone())
                            .await
                    {
                        tracing::error!("file consumer error: {e}");
                    }
                }));
            }
            (name, SourceConfig::Socket(sc)) => {
                let router = router.clone();
                handles.push(tokio::spawn(async move {
                    if let Err(e) =
                        sources::socket::run_consumer(name, sc, router, shutdown.clone()).await
                    {
                        tracing::error!("socket listener error: {e}");
                    }
                }));
            }
            (name, SourceConfig::Tcp(tc)) => {
                let router = router.clone();
                handles.push(tokio::spawn(async move {
                    if let Err(e) =
                        sources::tcp::run_consumer(name, tc, router, shutdown.clone()).await
                    {
                        tracing::error!("tcp listener error: {e}");
                    }
                }));
            }
            (name, SourceConfig::SQS(sq)) => {
                let router = router.clone();
                handles.push(tokio::spawn(async move {
                    if let Err(e) =
                        sources::sqs::run_consumer(name, sq, batch_size, router, shutdown.clone())
                            .await
                    {
                        tracing::error!("SQS consumer error: {e}");
                    }
                }));
            }
            (name, SourceConfig::GithubWebhook(gw)) => {
                let router = router.clone();
                handles.push(tokio::spawn(async move {
                    if let Err(e) =
                        sources::github_webhook::run_consumer(name, gw, router, shutdown.clone())
                            .await
                    {
                        tracing::error!("Github Webhook consumer error: {e}");
                    }
                }));
            }
            (name, SourceConfig::NPMRegistry(np)) => {
                let router = router.clone();
                let cache = cache.clone();
                handles.push(tokio::spawn(async move {
                    if let Err(e) = sources::npm_registry::run_consumer(
                        name,
                        np,
                        router,
                        shutdown.clone(),
                        cache,
                    )
                    .await
                    {
                        tracing::error!("NPM Registry consumer error: {e}");
                    }
                }));
            }
        }
    }

    handles
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
            consumer_handles: vec![],
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

        let shutdown = runtime.shutdown(Duration::from_millis(200), Duration::from_millis(200));
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
