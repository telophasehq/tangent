use anyhow::{Context, Result};
use wasmtime::component::{bindgen, Component, Linker, ResourceTable};
use wasmtime::{Config, Engine, Store};
use wasmtime_wasi::p2::{IoView, WasiCtx, WasiCtxBuilder, WasiView};

bindgen!({
    world: "processor",
    path: "../wit/interface.wit",
    async: true
});

struct Host {
    ctx: WasiCtx,
    table: ResourceTable,
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
    pub fn new() -> Result<Self, anyhow::Error> {
        let engine = Engine::new(Config::new().wasm_component_model(true).async_support(true))?;
        let path = std::env::var("WASM_COMPONENT")
            .unwrap_or_else(|_| "../compiled/app.component.wasm".to_string());

        let meta = std::fs::metadata(&path)
            .with_context(|| format!("component path not found: {}", path))?;
        tracing::info!("Loading component {} ({} bytes)", path, meta.len());

        let component = Component::from_file(&engine, &path)
            .with_context(|| format!("failed to load component at {}", path))?;

        let mut linker = Linker::<Host>::new(&engine);
        wasmtime_wasi::p2::add_to_linker_async(&mut linker)?;

        Ok(Self {
            engine: engine,
            component: component,
            linker: linker,
        })
    }

    pub async fn process_logs(&self, input: &str) -> Result<()> {
        let ctx = WasiCtxBuilder::new()
            .inherit_stdout()
            .inherit_stderr()
            .build();

        let mut store = Store::new(
            &self.engine,
            Host {
                ctx,
                table: ResourceTable::new(),
            },
        );

        let processor =
            Processor::instantiate_async(&mut store, &self.component, &self.linker).await?;

        let result = processor.call_process_logs(&mut store, input).await?;
        result.map_err(|e| anyhow::anyhow!(e))?;
        Ok(())
    }
}
