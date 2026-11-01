use std::path::{Path, PathBuf};
use std::sync::Arc;

use ahash::{HashMap, HashMapExt, HashSet, HashSetExt};
use anyhow::{Context, Result};

use serde_json::Value;
use wasmtime::component::{Component, Linker};
use wasmtime::{Engine, Store};
use wasmtime_wasi::{DirPerms, FilePerms, WasiCtxBuilder};

use crate::enrichment::Tables;
use crate::state::{StateStore, PLUGIN_NS};
use crate::wasm::host::tangent::logs::{config, enrichment, lock, log, remote, state};
use crate::wasm::host::{HostEngine, Processor};
pub struct WasmEngine {
    engine: Engine,
    linker: Linker<HostEngine>,
    state: Arc<dyn StateStore>,
    config: HashMap<Arc<str>, Arc<HashMap<String, Value>>>,
    /// Each plugin's state namespace: `PLUGIN_NS` unless configured otherwise.
    state_ns: HashMap<Arc<str>, Arc<str>>,
    /// Each plugin's data directory (`plugins.<name>.data`), seen by the
    /// guest read-only at `/data`.
    data: HashMap<Arc<str>, PathBuf>,
    /// The enrichment tables, loaded once for the process: every engine and
    /// every plugin instance holds the same ones.
    tables: Arc<Tables>,
    /// Plugins whose records go on unchanged when a call fails
    /// (`plugins.<name>.drop_on_error: false`).
    pass_on_error: HashSet<Arc<str>>,
    disable_remote_calls: bool,
}

impl WasmEngine {
    pub fn new(
        state: Arc<dyn StateStore>,
        tables: Arc<Tables>,
        disable_remote_calls: bool,
    ) -> Result<Self> {
        let engine = tangent_shared::wasm_engine::build()?;
        let mut linker = Linker::<HostEngine>::new(&engine);
        wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;
        log::add_to_linker::<HostEngine, HostEngine>(&mut linker, |host: &mut HostEngine| host)?;
        remote::add_to_linker::<HostEngine, HostEngine>(&mut linker, |host: &mut HostEngine| host)?;
        state::add_to_linker::<HostEngine, HostEngine>(&mut linker, |host: &mut HostEngine| host)?;
        config::add_to_linker::<HostEngine, HostEngine>(&mut linker, |host: &mut HostEngine| host)?;
        lock::add_to_linker::<HostEngine, HostEngine>(&mut linker, |host: &mut HostEngine| host)?;
        enrichment::add_to_linker::<HostEngine, HostEngine>(
            &mut linker,
            |host: &mut HostEngine| host,
        )?;

        Ok(Self {
            engine,
            linker,
            state,
            tables,
            disable_remote_calls,
            config: HashMap::new(),
            state_ns: HashMap::new(),
            data: HashMap::new(),
            pass_on_error: HashSet::new(),
        })
    }

    pub fn load_component(&self, loc: &Path) -> Result<Component> {
        Component::from_file(&self.engine, loc)
    }

    pub fn load_precompiled(
        &mut self,
        name: Arc<str>,
        loc: &Path,
        cfg: HashMap<String, Value>,
        state_ns: Option<&str>,
        data: Option<&Path>,
        drop_on_error: bool,
    ) -> Result<Component> {
        let comp = unsafe { Component::deserialize_file(&self.engine, &loc)? };

        let ns: Arc<str> = state_ns.map_or_else(|| Arc::from(PLUGIN_NS), Arc::from);
        self.state_ns.insert(Arc::clone(&name), ns);
        if let Some(dir) = data {
            self.data.insert(Arc::clone(&name), dir.to_path_buf());
        }
        if !drop_on_error {
            self.pass_on_error.insert(Arc::clone(&name));
        }
        self.config.insert(name, Arc::new(cfg));

        Ok(comp)
    }

    pub fn drops_on_error(&self, name: &str) -> bool {
        !self.pass_on_error.contains(name)
    }

    pub fn make_store(&self, component_name: &Arc<str>) -> Result<Store<HostEngine>> {
        let mut wasi = WasiCtxBuilder::new();
        wasi.inherit_stdout().inherit_stderr().inherit_env();
        if let Some(dir) = self.data.get(component_name) {
            // The plugin's data directory, read-only, at /data in the guest.
            wasi.preopened_dir(dir, "/data", DirPerms::READ, FilePerms::READ)
                .with_context(|| {
                    format!(
                        "opening data directory {} for plugin {component_name}",
                        dir.display()
                    )
                })?;
        }
        Ok(Store::new(
            &self.engine,
            HostEngine::new(
                wasi.build(),
                self.state.clone(),
                self.state_ns
                    .get(component_name)
                    .map_or_else(|| Arc::from(PLUGIN_NS), Arc::clone),
                self.config.get(component_name).unwrap().clone(),
                Arc::clone(&self.tables),
                self.disable_remote_calls,
            ),
        ))
    }

    pub async fn make_processor(
        &self,
        store: &mut Store<HostEngine>,
        component: &Component,
    ) -> Result<Processor> {
        Processor::instantiate_async(store, component, &self.linker).await
    }
}
