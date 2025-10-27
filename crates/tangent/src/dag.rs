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
        let engine = WasmEngine::new()?;

        let sink_manager = Arc::new(SinkManager::new(&cfg.sinks, cfg.batch_size_kb() * 10).await?);
        let config_dir = cfg_path.parent().unwrap_or_else(|| Path::new("."));
        let plugin_root = config_dir.join(&cfg.runtime.plugins_path).canonicalize()?;

        let mut components: Vec<(&String, Component)> = Vec::with_capacity(cfg.plugins.len());
        for (name, _) in &cfg.plugins {
            let component_file = format!("{name}.component.wasm");
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

            components.push((
                name,
                engine
                    .load_component(&plugin_path)
                    .with_context(|| format!("loading {}", &component_file))?,
            ));
        }

        let mut outs: HashMap<NodeRef, Vec<NodeRef>> = HashMap::default();
        for e in &cfg.dag {
            outs.entry(e.from.clone()).or_default().extend(e.to.clone());
        }

        let router = Arc::new(Router::new(outs, Arc::clone(&sink_manager)));

        let pool = Arc::new(
            WorkerPool::new(
                cfg.runtime.workers,
                engine,
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
        source_name: &str,
        frames: Vec<BytesMut>,
        acks: Vec<Arc<dyn Ack>>,
    ) -> anyhow::Result<()> {
        let from = NodeRef::Source {
            name: source_name.to_string(),
        };
        self.router.forward(&from, frames, acks).await
    }

    pub fn close(&mut self) {
        if let Some(pool) = Arc::get_mut(&mut self.pool) {
            pool.close();
        }
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
        let mut pool_owned = Arc::try_unwrap(pool)
            .map_err(|_| anyhow!("WorkerPool still has refs; drop all clones before shutdown"))?;
        pool_owned.close();
        let _ = timeout(worker_timeout, pool_owned.join()).await;

        drop(router);

        tracing::info!("waiting on sink manager to shutdown...");
        let sink_owned = Arc::try_unwrap(sink_manager)
            .map_err(|_| anyhow!("SinkManager still has refs; drop all clones before shutdown"))?;
        let _ = timeout(sink_timeout, sink_owned.join()).await;

        Ok(())
    }
}
