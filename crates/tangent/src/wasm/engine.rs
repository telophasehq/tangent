use std::path::Path;

use anyhow::Result;

use wasmtime::component::{Component, Linker};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::p2::WasiCtxBuilder;

use crate::wasm::host::tangent::logs::log;
use crate::wasm::host::{HostEngine, Processor};
pub struct WasmEngine {
    engine: Engine,
    linker: Linker<HostEngine>,
}

impl WasmEngine {
    pub fn new() -> Result<Self> {
        let mut cfg = Config::new();
        cfg.wasm_component_model(true)
            .async_support(true)
            .wasm_simd(true)
            .cranelift_opt_level(wasmtime::OptLevel::Speed)
            .profiler(wasmtime::ProfilingStrategy::PerfMap)
            .allocation_strategy(wasmtime::InstanceAllocationStrategy::Pooling(
                wasmtime::PoolingAllocationConfig::default(),
            ));

        let engine = Engine::new(&cfg)?;
        let mut linker = Linker::<HostEngine>::new(&engine);
        wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;
        log::add_to_linker::<HostEngine, HostEngine>(&mut linker, |host: &mut HostEngine| host)?;

        Ok(Self { engine, linker })
    }

    pub fn load_component(&self, loc: &Path) -> Result<Component> {
        Component::from_file(&self.engine, loc)
    }

    pub fn make_store(&self) -> Store<HostEngine> {
        Store::new(
            &self.engine,
            HostEngine::new(
                WasiCtxBuilder::new()
                    .inherit_stdout()
                    .inherit_stderr()
                    .build(),
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
