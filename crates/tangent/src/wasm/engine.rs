use std::path::Path;
use std::sync::Arc;

use ahash::{HashMap, HashMapExt};
use anyhow::Result;

use serde_json::Value;
use wasmtime::component::{Component, Linker};
use wasmtime::{Engine, Store};
use wasmtime_wasi::WasiCtxBuilder;

use crate::state::{StateStore, PLUGIN_NS};
use crate::wasm::host::tangent::logs::{config, lock, log, remote, state};
use crate::wasm::host::{HostEngine, Processor};
pub struct WasmEngine {
    engine: Engine,
    linker: Linker<HostEngine>,
    state: Arc<dyn StateStore>,
    config: HashMap<Arc<str>, Arc<HashMap<String, Value>>>,
    /// Each plugin's state namespace: `PLUGIN_NS` unless configured otherwise.
    state_ns: HashMap<Arc<str>, Arc<str>>,
    disable_remote_calls: bool,
}

impl WasmEngine {
    pub fn new(state: Arc<dyn StateStore>, disable_remote_calls: bool) -> Result<Self> {
        let engine = tangent_shared::wasm_engine::build()?;
        let mut linker = Linker::<HostEngine>::new(&engine);
        wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;
        log::add_to_linker::<HostEngine, HostEngine>(&mut linker, |host: &mut HostEngine| host)?;
        remote::add_to_linker::<HostEngine, HostEngine>(&mut linker, |host: &mut HostEngine| host)?;
        state::add_to_linker::<HostEngine, HostEngine>(&mut linker, |host: &mut HostEngine| host)?;
        config::add_to_linker::<HostEngine, HostEngine>(&mut linker, |host: &mut HostEngine| host)?;
        lock::add_to_linker::<HostEngine, HostEngine>(&mut linker, |host: &mut HostEngine| host)?;

        Ok(Self {
            engine,
            linker,
            state,
            disable_remote_calls,
            config: HashMap::new(),
            state_ns: HashMap::new(),
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
    ) -> Result<Component> {
        let comp = unsafe { Component::deserialize_file(&self.engine, &loc)? };

        let ns: Arc<str> = state_ns.map_or_else(|| Arc::from(PLUGIN_NS), Arc::from);
        self.state_ns.insert(Arc::clone(&name), ns);
        self.config.insert(name, Arc::new(cfg));

        Ok(comp)
    }

    pub fn make_store(&self, component_name: &Arc<str>) -> Store<HostEngine> {
        Store::new(
            &self.engine,
            HostEngine::new(
                WasiCtxBuilder::new()
                    .inherit_stdout()
                    .inherit_stderr()
                    .inherit_env()
                    .build(),
                self.state.clone(),
                self.state_ns
                    .get(component_name)
                    .map_or_else(|| Arc::from(PLUGIN_NS), Arc::clone),
                self.config.get(component_name).unwrap().clone(),
                self.disable_remote_calls,
            ),
        )
    }

    pub async fn make_processor(
        &self,
        store: &mut Store<HostEngine>,
        component: &Component,
    ) -> Result<Processor> {
        Processor::instantiate_async(store, component, &self.linker).await
    }
}
