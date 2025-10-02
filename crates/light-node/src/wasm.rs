use anyhow::Result;

use wasmtime::component::{bindgen, Component, Linker, ResourceTable};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::p2::{IoView, WasiCtx, WasiCtxBuilder, WasiView};

bindgen!({
    world: "processor",
    path: "../../wit",
    async: true,
});

pub struct Host {
    pub ctx: WasiCtx,
    pub table: ResourceTable,
}
impl IoView for Host {
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
}
impl WasiView for Host {
    fn ctx(&mut self) -> &mut WasiCtx {
        &mut self.ctx
    }
}

pub struct WasmEngine {
    engine: Engine,
    component: Component,
    linker: Linker<Host>,
}

impl WasmEngine {
    pub fn new() -> Result<Self> {
        let mut cfg = Config::new();
        cfg.wasm_component_model(true)
            .async_support(true)
            .wasm_simd(true)
            .cranelift_opt_level(wasmtime::OptLevel::Speed)
            .allocation_strategy(wasmtime::InstanceAllocationStrategy::Pooling(
                wasmtime::PoolingAllocationConfig::default(),
            ));

        let engine = Engine::new(&cfg)?;
        let path = std::env::var("WASM_COMPONENT")
            .unwrap_or_else(|_| "compiled/app.component.wasm".into());
        let component = Component::from_file(&engine, &path)?;
        let mut linker = Linker::<Host>::new(&engine);
        wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;
        Ok(Self {
            engine,
            component,
            linker,
        })
    }

    pub fn make_store(&self) -> Store<Host> {
        Store::new(
            &self.engine,
            Host {
                ctx: WasiCtxBuilder::new()
                    .inherit_stdout()
                    .inherit_stderr()
                    .build(),
                table: ResourceTable::new(),
            },
        )
    }

    pub async fn make_processor(&self, store: &mut Store<Host>) -> Result<Processor> {
        Processor::instantiate_async(store, &self.component, &self.linker).await
    }
}
