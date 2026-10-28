use ahash::AHashMap as HashMap;

use anyhow::{anyhow, Context, Result};
use std::{collections::BTreeMap, path::Path, sync::Arc, time::Duration};
use tangent_shared::{dag::NodeRef, sources::common::SourceConfig, Config};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use wasmtime::component::Component;

use crate::{
    router::Router,
    sinks::manager::SinkManager,
    sources, state,
    state::StateStore,
    wasm::engine::WasmEngine,
    worker::WorkerPool,
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
        cfg_path: &Path,
        shutdown: CancellationToken,
    ) -> anyhow::Result<Self> {
        let sink_manager = Arc::new(SinkManager::new(&cfg.sinks).await?);
        let config_dir = cfg_path.parent().unwrap_or_else(|| Path::new("."));
        // A pipeline without plugins has no plugin directory to resolve.
        let plugin_root = if cfg.plugins.is_empty() {
            config_dir.to_path_buf()
        } else {
            config_dir
                .join(&cfg.runtime.plugins_path)
                .canonicalize()
                .with_context(|| {
                    format!(
                        "canonicalizing plugins_path {}",
                        config_dir.join(&cfg.runtime.plugins_path).display()
                    )
                })?
        };

        let workers = cfg.runtime.workers;

        let store = state::open(&cfg.runtime.state, config_dir)?;

        let mut engines: Vec<WasmEngine> = (0..workers)
            .map(|_| WasmEngine::new(Arc::clone(&store), cfg.runtime.disable_remote_calls))
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

                let data_dir = match &plugin_cfg.data {
                    Some(dir) => Some(config_dir.join(dir).canonicalize().with_context(|| {
                        format!(
                            "plugin {name}: data directory {}",
                            config_dir.join(dir).display()
                        )
                    })?),
                    None => None,
                };

                components[i].push((
                    Arc::clone(name),
                    engines[i]
                        .load_precompiled(
                            Arc::clone(name),
                            &plugin_path,
                            plugin_cfg.config.clone(),
                            plugin_cfg.state_namespace.as_deref(),
                            data_dir.as_deref(),
                        )
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
        // An inference node's `batch` is a record count the worker gathers
        // towards, waiting `batch_age_ms` for it; the runtime's byte and age
        // limits are floors, not the batch. The byte cap grows with the count
        // so it only bounds memory.
        let batch_records = cfg
            .inference
            .values()
            .map(|n| n.batch.max(1))
            .max()
            .unwrap_or(1);
        let (worker_batch_size, worker_batch_age) = if batch_records > 1 {
            let wait = cfg
                .inference
                .values()
                .filter(|n| n.batch > 1)
                .map(|n| Duration::from_millis(n.batch_age_ms))
                .max()
                .unwrap_or(batch_age);
            (batch_size.max(batch_records * 16 * 1024), batch_age.max(wait))
        } else {
            (batch_size, batch_age)
        };

        // Every worker hosts every plugin and every inference node, and walks
        // the DAG for the records it is handed.
        let pool = Arc::new(
            WorkerPool::new(
                workers,
                engines,
                components,
                &cfg.inference,
                config_dir,
                worker_batch_size,
                worker_batch_age,
                batch_records,
                Arc::clone(&router),
                cfg.runtime.partition.clone(),
                Arc::clone(&store),
            )
            .await?,
        );
        if let Some(p) = &cfg.runtime.partition {
            if let Some(ttl) = p.retain_ms {
                let cap = cfg.runtime.state.max_ttl_ms;
                if cap != 0 && ttl > cap {
                    tracing::warn!(
                        retain_ms = ttl,
                        max_ttl_ms = cap,
                        "partition.retain_ms is longer than runtime.state.max_ttl_ms; retained records will expire sooner"
                    );
                }
            }
        }

        router.set_pool(&pool);

        let consumer_handles = spawn_consumers(
            cfg.sources,
            batch_size,
            router.clone(),
            Arc::clone(&store),
            shutdown.clone(),
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

        // Nothing new comes in now, but records already handed to a worker
        // are still on their way through the DAG, a model call included.
        // Wait for the DAG to go quiet, then take the workers down.
        tracing::info!("draining workers...");
        let quiet = timeout(worker_timeout, async {
            while router.in_flight() > 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await;
        if quiet.is_err() {
            tracing::warn!(
                in_flight = router.in_flight(),
                "drain timeout exceeded. Records may be dropped."
            );
        }
        drop(router);

        tracing::info!("waiting on workers to shutdown...");
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
    store: Arc<dyn StateStore>,
    shutdown: CancellationToken,
) -> Vec<tokio::task::JoinHandle<()>> {
    let mut handles = Vec::new();
    for src in sources {
        let shutdown = shutdown.clone();
        // The polling sources keep their cursor in the state store.
        let store = Arc::clone(&store);
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
                handles.push(tokio::spawn(async move {
                    if let Err(e) =
                        sources::npm_registry::run_consumer(name, np, router, shutdown.clone())
                            .await
                    {
                        tracing::error!("NPM Registry consumer error: {e}");
                    }
                }));
            }
            (name, SourceConfig::Cloudtrail(ct)) => {
                let router = router.clone();
                handles.push(tokio::spawn(async move {
                    if let Err(e) = sources::cloudtrail::run_consumer(
                        name,
                        ct,
                        batch_size,
                        router,
                        store,
                        shutdown.clone(),
                    )
                    .await
                    {
                        tracing::error!("cloudtrail consumer error: {e:#}");
                    }
                }));
            }
            (name, SourceConfig::GithubAudit(ga)) => {
                let router = router.clone();
                handles.push(tokio::spawn(async move {
                    if let Err(e) = sources::github_audit::run_consumer(
                        name,
                        ga,
                        router,
                        store,
                        shutdown.clone(),
                    )
                    .await
                    {
                        tracing::error!("github_audit consumer error: {e:#}");
                    }
                }));
            }
            (name, SourceConfig::Okta(ok)) => {
                let router = router.clone();
                handles.push(tokio::spawn(async move {
                    if let Err(e) =
                        sources::okta::run_consumer(name, ok, router, store, shutdown.clone()).await
                    {
                        tracing::error!("okta consumer error: {e:#}");
                    }
                }));
            }
            (name, SourceConfig::GoogleWorkspace(gw)) => {
                let router = router.clone();
                handles.push(tokio::spawn(async move {
                    if let Err(e) = sources::google_workspace::run_consumer(
                        name,
                        gw,
                        router,
                        store,
                        shutdown.clone(),
                    )
                    .await
                    {
                        tracing::error!("google_workspace consumer error: {e:#}");
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
