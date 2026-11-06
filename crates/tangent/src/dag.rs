use ahash::AHashMap as HashMap;

use anyhow::{anyhow, Context, Result};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tangent_shared::{
    dag::NodeRef,
    plugins::{built, compiled},
    sources::common::SourceConfig,
    Config,
};
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;
use wasmtime::component::Component;

use crate::{
    enrichment::Tables,
    group::{self, Dispatch, GroupConfig, Grouper},
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
    /// Each grouped plugin's grouping task, and the pool it hands groups to.
    groups: Vec<(Grouper, tokio::task::JoinHandle<()>, Arc<WorkerPool>)>,
    /// The enrichment tables every plugin on every worker reads, here so a
    /// test can count what holds them; the engines keep them alive.
    #[cfg(test)]
    pub(crate) tables: Arc<Tables>,
}

impl DagRuntime {
    /// The DAG, and a consumer reading each source until `shutdown`.
    pub async fn build(
        cfg: Config,
        cfg_path: &Path,
        shutdown: CancellationToken,
        stop_models: CancellationToken,
    ) -> anyhow::Result<Self> {
        Self::assemble(cfg, cfg_path, Some(shutdown), stop_models).await
    }

    /// The DAG with nothing reading its sources: the caller hands each
    /// source's records to `router` as the source would, and the source's
    /// edges take them on. `tangent replay` feeds it the sources' past.
    pub async fn fed(
        cfg: Config,
        cfg_path: &Path,
        stop_models: CancellationToken,
    ) -> anyhow::Result<Self> {
        Self::assemble(cfg, cfg_path, None, stop_models).await
    }

    async fn assemble(
        cfg: Config,
        cfg_path: &Path,
        consumers: Option<CancellationToken>,
        stop_models: CancellationToken,
    ) -> anyhow::Result<Self> {
        let sink_manager = Arc::new(SinkManager::new(&cfg.sinks).await?);
        let config_dir = cfg_path.parent().unwrap_or_else(|| Path::new("."));
        // What `tangent plugin compile` built, and what `tangent plugin
        // fetch` downloaded; a config of prebuilt plugins has no plugins
        // directory.
        let plugins_dir = config_dir.join(&cfg.runtime.plugins_path);
        let cache = tangent_shared::cache::plugins().ok();
        let plugins = compiled_plugins(&cfg, &plugins_dir, cache.as_deref())?;

        let workers = cfg.runtime.workers;

        if cfg.runtime.state.read_only {
            if let Some(w) = state_writer(&cfg) {
                anyhow::bail!("runtime.state.read_only: this pipeline writes state ({w})");
            }
        }
        let store = state::open(&cfg.runtime.state, config_dir)?;

        // Read once, here, for the whole process: the workers' engines and
        // every plugin instance in them share these.
        let tables = Arc::new(Tables::load(&cfg.enrichment_tables, config_dir)?);

        for (name, p) in &cfg.plugins {
            if p.group_by.is_empty()
                && (p.max_events.is_some() || p.max_age_ms.is_some() || p.history > 0)
            {
                anyhow::bail!(
                    "plugin {name}: max_events, max_age_ms and history group records, and it has no group_by"
                );
            }
            if !p.group_by.is_empty() && p.expire_after_ms == 0 {
                anyhow::bail!("plugin {name}: expire_after_ms must be at least 1");
            }
            if p.max_age_ms == Some(0) {
                anyhow::bail!("plugin {name}: max_age_ms must be at least 1");
            }
        }
        let (engines, components) = load(&cfg, &plugins, config_dir, &store, &tables, workers)?;
        let mut outs: HashMap<NodeRef, Vec<NodeRef>> = HashMap::default();
        // `validate` has checked every name, so each resolves
        for e in &cfg.dag {
            let Some(from) = cfg.node(&e.from) else {
                continue;
            };
            let to = e.to.iter().filter_map(|t| cfg.node(t));
            outs.entry(from).or_default().extend(to);
        }

        let batch_size = cfg.batch_size_kb();
        let batch_age = cfg.batch_age_ms();

        let router =
            Arc::new(Router::new(outs, Arc::clone(&sink_manager)).with_sink_batch(batch_size));

        // One concurrency limit per inference node, whichever pool runs it.
        let permits = crate::inference::permits(&cfg.inference);
        // Every worker hosts every plugin and every inference node, and walks
        // the DAG for the records it is handed.
        let pool = Arc::new(
            WorkerPool::new(
                workers,
                engines,
                components,
                &cfg.inference,
                &permits,
                config_dir,
                batch_size,
                batch_age,
                Arc::clone(&router),
                stop_models.clone(),
            )
            .await?,
        );
        router.set_pool(&pool);

        // A grouped plugin gets a pool of its own and a grouping task that
        // hands it whole groups; a walk that reaches the plugin hands the
        // task its records instead.
        let mut groups = Vec::new();
        let mut groupers = std::collections::HashMap::new();
        for (name, p) in cfg.plugins.iter().filter(|(_, p)| !p.group_by.is_empty()) {
            let (engines, components) = load(&cfg, &plugins, config_dir, &store, &tables, workers)?;
            let pool = Arc::new(
                WorkerPool::new(
                    workers,
                    engines,
                    components,
                    &cfg.inference,
                    &permits,
                    config_dir,
                    batch_size,
                    batch_age,
                    Arc::clone(&router),
                    stop_models.clone(),
                )
                .await?,
            );
            let cfg = GroupConfig {
                group_by: p.group_by.clone(),
                max_events: p.max_events,
                expire_after: Duration::from_millis(p.expire_after_ms),
                max_age: p.max_age_ms.map(Duration::from_millis),
                history: p.history,
            };
            let (grouper, task) = group::spawn(
                Arc::clone(name),
                cfg,
                Arc::clone(&pool) as Arc<dyn Dispatch>,
                Arc::clone(&store),
            );
            groupers.insert(Arc::clone(name), grouper.clone());
            groups.push((grouper, task, pool));
        }
        router.set_groupers(groupers);

        let consumer_handles = match consumers {
            Some(shutdown) => spawn_consumers(
                cfg.sources,
                batch_size,
                router.clone(),
                Arc::clone(&store),
                shutdown,
            ),
            None => Vec::new(),
        };

        Ok(Self {
            router,
            pool,
            sink_manager,
            consumer_handles,
            groups,
            #[cfg(test)]
            tables,
        })
    }

    /// The end of input, before a shutdown: waits for what is on its way
    /// through the DAG, then closes every open group and waits until each is
    /// through, however long its walk takes. `tangent replay` ends its range
    /// with it: every actor's group closes at once there, and the judge can
    /// take minutes over them, past what shutdown waits.
    pub async fn finish(&self) {
        drained(&self.router).await;
        close_groups(&self.groups, &self.router, None).await;
    }

    /// Records in closed groups waiting for their key's group out to be
    /// through, across the grouped plugins: what a caller feeding the DAG
    /// faster than real time holds in memory.
    pub fn waiting(&self) -> usize {
        self.groups.iter().map(|(g, _, _)| g.waiting()).sum()
    }

    pub async fn shutdown(self, worker_timeout: Duration, sink_timeout: Duration) -> Result<()> {
        let Self {
            router,
            pool,
            sink_manager,
            consumer_handles,
            groups,
            ..
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
        // are still on their way through the DAG. After a signal the model
        // calls have ended, and records that were waiting on one go to no
        // sink and are not acked; a --once run waits for its calls. Wait for
        // the DAG to go quiet, then take the workers down.
        tracing::info!("draining workers...");
        if timeout(worker_timeout, drained(&router)).await.is_err() {
            tracing::warn!(
                in_flight = router.in_flight(),
                "drain timeout exceeded. Records may be dropped."
            );
        }

        // Then the groups: the end of input closes every open one.
        close_groups(&groups, &router, Some(worker_timeout)).await;
        let mut pools = vec![pool];
        for (grouper, task, pool) in groups {
            drop(grouper);
            task.abort();
            let _ = task.await;
            pools.push(pool);
        }
        drop(router);

        tracing::info!("waiting on workers to shutdown...");
        for pool in pools {
            if let Ok(pool_owned) = Arc::try_unwrap(pool) {
                if let Err(e) = timeout(worker_timeout, pool_owned.join()).await {
                    tracing::warn!(?e, "pool shutdown timeout exceeded. Logs may be dropped.");
                }
            } else {
                tracing::warn!("WorkerPool still has refs; cannot consume for join()");
            }
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

/// Each grouped plugin's grouping task, and the pool it hands groups to.
type Groups = [(Grouper, tokio::task::JoinHandle<()>, Arc<WorkerPool>)];

/// Closes every open group and waits until each is through. A group's walk
/// may reach another grouped plugin, so a round per grouped plugin, each
/// waiting for the DAG to go quiet again. With a `limit`, each wait gives up
/// after it, and the groups still open are left to be dropped.
async fn close_groups(groups: &Groups, router: &Router, limit: Option<Duration>) {
    for _ in 0..groups.len() {
        for (grouper, _, _) in groups {
            if within(limit, grouper.flush()).await.is_none() {
                tracing::warn!("closing the groups timed out; groups still open are dropped");
            }
        }
        within(limit, drained(router)).await;
    }
}

/// Until nothing is on its way through the DAG.
async fn drained(router: &Router) {
    while router.in_flight() > 0 {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// `f`, given up after `limit` when there is one.
async fn within<T>(limit: Option<Duration>, f: impl std::future::Future<Output = T>) -> Option<T> {
    match limit {
        Some(limit) => timeout(limit, f).await.ok(),
        None => Some(f.await),
    }
}

/// `workers` engines, and every plugin loaded into each.
type Loaded = (Vec<WasmEngine>, Vec<Vec<(Arc<str>, Component)>>);

/// Each plugin's `.cwasm`, in `cfg.plugins`' order: what `tangent plugin
/// compile` built in `plugins_dir`, else what `tangent plugin fetch` kept
/// under `cache` for its `prebuilt` ([`compiled`]). All are found before any
/// is loaded, so a plugin found nowhere stops the build at once, saying
/// which command makes it.
fn compiled_plugins(
    cfg: &Config,
    plugins_dir: &Path,
    cache: Option<&Path>,
) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::with_capacity(cfg.plugins.len());
    for (name, plugin) in &cfg.plugins {
        let Some(path) = compiled(name, plugin, plugins_dir, cache) else {
            let built = built(name, plugin, plugins_dir);
            let no_build = built
                .map(|b| format!("no {}, and ", b.display()))
                .unwrap_or_default();
            anyhow::bail!(match &plugin.prebuilt {
                None if no_build.is_empty() => {
                    format!("plugin {name}: no `path` to compile it from, and no `prebuilt`")
                }
                None => format!(
                    "plugin {name}: not compiled: {no_build}it has no `prebuilt`; `tangent plugin compile` builds it"
                ),
                Some(p) => format!(
                    "plugin {name}: not compiled: {no_build}none fetched from {} {}; `tangent plugin fetch` gets it",
                    p.repo,
                    p.release(env!("CARGO_PKG_VERSION"))
                ),
            });
        };
        paths.push(path);
    }
    Ok(paths)
}

fn load(
    cfg: &Config,
    plugins: &[PathBuf],
    config_dir: &Path,
    store: &Arc<dyn StateStore>,
    tables: &Arc<Tables>,
    workers: usize,
) -> Result<Loaded> {
    let mut engines: Vec<WasmEngine> = (0..workers)
        .map(|_| {
            WasmEngine::new(
                Arc::clone(store),
                cfg.runtime.state.max_ttl_ms,
                Arc::clone(tables),
                cfg.runtime.disable_remote_calls,
            )
        })
        .collect::<Result<_, _>>()?;
    let mut components: Vec<Vec<(Arc<str>, Component)>> = Vec::with_capacity(workers);
    for i in 0..workers {
        components.push(Vec::<(Arc<str>, Component)>::new());
        for ((name, plugin_cfg), plugin_path) in cfg.plugins.iter().zip(plugins) {
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
                        plugin_path,
                        plugin_cfg.config.clone(),
                        plugin_cfg.state_namespace.as_deref(),
                        data_dir.as_deref(),
                        plugin_cfg.drop_on_error,
                    )
                    .with_context(|| format!("plugin {name}: loading {}", plugin_path.display()))?,
            ));
        }
    }
    Ok((engines, components))
}

/// What writes state besides the plugins: a grouped plugin's history, and
/// the cursors of the sources that poll an API.
fn state_writer(cfg: &Config) -> Option<String> {
    if let Some((name, _)) = cfg
        .plugins
        .iter()
        .find(|(_, p)| !p.group_by.is_empty() && p.history > 0)
    {
        return Some(format!("plugin {name}'s history"));
    }
    cfg.sources
        .iter()
        .find_map(|(name, s)| s.keeps_cursor().then(|| format!("source {name}")))
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
        // The polling sources keep their cursor in the state store; those that
        // do say so in `SourceConfig::keeps_cursor`.
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
            (name, SourceConfig::Cloudtrail(ct)) => {
                let router = router.clone();
                handles.push(tokio::spawn(async move {
                    if let Err(e) = sources::cloudtrail::run_consumer(
                        name,
                        ct,
                        batch_size,
                        router,
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
            (name, SourceConfig::Sqlite(sq)) => {
                let router = router.clone();
                handles.push(tokio::spawn(async move {
                    if let Err(e) =
                        sources::sqlite::run_consumer(name, sq, router, shutdown.clone()).await
                    {
                        tracing::error!("sqlite source error: {e:#}");
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
            groups: vec![],
            tables: Default::default(),
        };

        let ack = Arc::new(CountingAck::default());
        let ack_dyn: Arc<dyn Ack> = ack.clone();
        sink_manager
            .enqueue(
                sink_name.clone(),
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

    /// A fed DAG reads none of its sources, and what its caller hands it as
    /// a source's records goes on along that source's edges.
    #[tokio::test]
    async fn a_fed_dag_takes_a_sources_records_from_its_caller() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        std::fs::write(root.join("in.ndjson"), "{\"n\":1}\n").unwrap();
        let cfg_path = root.join("tangent.yaml");
        std::fs::write(
            &cfg_path,
            format!(
                "runtime: {{ state: {{ backend: lru }} }}\nsources:\n  in: {{ type: file, path: {r}/in.ndjson, decoding: {{ format: {{ type: ndjson }} }} }}\nsinks:\n  out: {{ type: file, path: {r}/out.ndjson, compression: {{ type: none }} }}\ndag:\n  - from: in\n    to: [out]\n",
                r = root.display()
            ),
        )
        .unwrap();
        let cfg = Config::from_file(&cfg_path).unwrap();
        let dag = DagRuntime::fed(cfg, &cfg_path, CancellationToken::new())
            .await
            .unwrap();
        let ack = Arc::new(CountingAck::default());
        dag.router
            .forward(
                &NodeRef::Source { name: "in".into() },
                vec![BytesMut::from("{\"n\":2}\n")],
                vec![ack.clone() as Arc<dyn Ack>],
            )
            .await
            .unwrap();
        dag.shutdown(Duration::from_secs(5), Duration::from_secs(5))
            .await
            .unwrap();
        assert_eq!(ack.count(), 1);
        assert_eq!(
            std::fs::read_to_string(root.join("out.ndjson")).unwrap(),
            "{\"n\":2}\n"
        );
    }

    /// Three actors' records in open groups, and a walk that takes `walk` a
    /// group: a judge working through the groups at the end of a replay.
    /// Returns how many records are through.
    async fn slow_groups(walk: Duration) -> (DagRuntime, Arc<AtomicUsize>) {
        struct Slow(Arc<AtomicUsize>, Duration);
        #[async_trait]
        impl Dispatch for Slow {
            async fn dispatch(&self, g: group::Group) -> Result<()> {
                let (through, walk) = (Arc::clone(&self.0), self.1);
                tokio::spawn(async move {
                    tokio::time::sleep(walk).await;
                    through.fetch_add(g.records.len(), Ordering::SeqCst);
                    g.done.ack().await.unwrap();
                });
                Ok(())
            }
        }
        let through = Arc::new(AtomicUsize::new(0));
        let store = state::open(
            &tangent_shared::runtime::StateConfig {
                backend: tangent_shared::runtime::StateBackend::Lru,
                ..Default::default()
            },
            Path::new("."),
        )
        .unwrap();
        let (grouper, task) = group::spawn(
            Arc::from("judge"),
            GroupConfig {
                group_by: vec!["actor".into()],
                max_events: None,
                expire_after: Duration::from_secs(1800),
                max_age: None,
                history: 0,
            },
            Arc::new(Slow(Arc::clone(&through), walk)),
            store,
        );
        let records = ["a", "b", "c"]
            .iter()
            .map(|a| {
                let line = format!("{{\"actor\":\"{a}\"}}");
                crate::wasm::host::JsonLogView::from_bytes(line.as_str().into()).unwrap()
            })
            .collect();
        grouper.add(records).await.unwrap();
        let sink_manager = Arc::new(SinkManager::for_test(vec![], 1));
        let runtime = DagRuntime {
            router: Arc::new(Router::new(HashMap::default(), Arc::clone(&sink_manager))),
            pool: Arc::new(WorkerPool::new_for_test(vec![])),
            sink_manager,
            consumer_handles: vec![],
            groups: vec![(grouper, task, Arc::new(WorkerPool::new_for_test(vec![])))],
            tables: Default::default(),
        };
        (runtime, through)
    }

    /// The end of a replay: finish closes every open group and waits until
    /// each is through, however long past the time shutdown gives them.
    #[tokio::test]
    async fn finish_waits_for_every_group_however_long() {
        let (runtime, through) = slow_groups(Duration::from_millis(300)).await;
        runtime.finish().await;
        assert_eq!(through.load(Ordering::SeqCst), 3);
        runtime
            .shutdown(Duration::from_millis(100), Duration::from_millis(100))
            .await
            .unwrap();
    }

    /// A plugin with no `.cwasm` and no `prebuilt` stops the build naming
    /// the file it looked for and the command that builds it, also when
    /// there is no plugins directory at all.
    #[tokio::test]
    async fn a_plugin_never_compiled_names_the_command_that_builds_it() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path();
        let cfg_path = root.join("tangent.yaml");
        std::fs::write(
            &cfg_path,
            format!(
                "runtime: {{ state: {{ backend: lru }} }}\nplugins:\n  p: {{ module_type: rust, path: . }}\nsources:\n  in: {{ type: file, path: {}/in.ndjson, decoding: {{ format: {{ type: ndjson }} }} }}\nsinks:\n  out: {{ type: blackhole }}\ndag:\n  - from: in\n    to: [p]\n  - from: p\n    to: [out]\n",
                root.display()
            ),
        )
        .unwrap();
        let cfg = Config::from_file(&cfg_path).unwrap();
        let err = DagRuntime::build(
            cfg,
            &cfg_path,
            CancellationToken::new(),
            CancellationToken::new(),
        )
        .await
        .err()
        .unwrap();
        let err = format!("{err:#}");
        let want = format!(
            "plugin p: not compiled: no {}, and it has no `prebuilt`; `tangent plugin compile` builds it",
            root.join("plugins/p.cwasm").display()
        );
        assert!(err.contains(&want), "{err}");
    }

    /// A prebuilt plugin runs from what `tangent plugin fetch` kept, with no
    /// plugins directory, and only when it was fetched for this `prebuilt`.
    /// A plugin without a source never runs a plugins/ file of its name:
    /// this config's `tangent plugin compile` did not build it.
    #[test]
    fn a_fetched_plugin_loads_from_the_cache() {
        let dir = tempfile::tempdir().unwrap();
        let (root, cache) = (dir.path(), dir.path().join("cache"));
        let yaml = |release: &str| {
            format!("runtime: {{ state: {{ backend: lru }} }}\nplugins:\n  p: {{ module_type: rust, prebuilt: {{ repo: acme/packs, release: {release} }} }}\n")
        };
        let cfg: Config = serde_yaml::from_str(&yaml("packs-3")).unwrap();
        let fetched =
            tangent_shared::plugins::fetched(&cache, cfg.plugins["p"].prebuilt.as_ref().unwrap());
        std::fs::create_dir_all(&fetched).unwrap();
        let cwasm = tangent_shared::wasm_engine::build()
            .unwrap()
            .precompile_component(
                include_str!("../testdata/enrichment/plain.component.wat").as_bytes(),
            )
            .unwrap();
        std::fs::write(fetched.join("p.cwasm"), cwasm).unwrap();
        let store = state::open(&cfg.runtime.state, root).unwrap();
        let tables = Arc::new(Tables::default());
        let plugins_dir = root.join("plugins");
        std::fs::create_dir(&plugins_dir).unwrap();
        std::fs::write(plugins_dir.join("p.cwasm"), "another config's").unwrap();

        let plugins = compiled_plugins(&cfg, &plugins_dir, Some(&cache)).unwrap();
        assert_eq!(plugins, [fetched.join("p.cwasm")]);
        let (_, components) = load(&cfg, &plugins, root, &store, &tables, 2).unwrap();
        assert_eq!(components.len(), 2);
        assert!(components.iter().all(|c| c.len() == 1 && &*c[0].0 == "p"));

        let other: Config = serde_yaml::from_str(&yaml("packs-4")).unwrap();
        let err = compiled_plugins(&other, &plugins_dir, Some(&cache))
            .err()
            .unwrap();
        assert_eq!(
            format!("{err:#}"),
            "plugin p: not compiled: none fetched from acme/packs packs-4; `tangent plugin fetch` gets it"
        );
    }

    /// A read-only pipeline whose runtime writes state is refused: a
    /// polling source keeps its cursor there.
    #[test]
    fn what_writes_state_besides_the_plugins() {
        let cfg = |yaml: &str| -> Config { serde_yaml::from_str(yaml).unwrap() };
        let logs = "runtime: {}\nsources:\n  logs: { type: sqlite, path: logs.sqlite, table: logs, partition: actor, batch_size: 40, timeout_secs: 60 }\n";
        assert_eq!(state_writer(&cfg(logs)), None);
        let okta = format!("{logs}  okta: {{ type: okta }}\n");
        assert_eq!(state_writer(&cfg(&okta)).as_deref(), Some("source okta"));
        let history =
            "runtime: {}\nplugins:\n  r: { module_type: rust, group_by: [actor], history: 3 }\n";
        assert_eq!(
            state_writer(&cfg(history)).as_deref(),
            Some("plugin r's history")
        );
    }
}
