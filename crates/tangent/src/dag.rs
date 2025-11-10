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
        match timeout(worker_timeout, pool_owned.join()).await {
            Err(e) => {
                tracing::warn!(?e, "pool shutdown timeout exceeded. Logs may be dropped.")
            }
            Ok(_) => (),
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
