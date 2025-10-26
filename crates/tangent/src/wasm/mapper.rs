use wasmtime::component::Component;
use wasmtime::Store;

use crate::wasm::engine::WasmEngine;
use crate::wasm::host::exports::tangent::logs::mapper::Selector;
use crate::wasm::host::{HostEngine, Processor};

use crate::wasm::probe::{compile_selector, CompiledSelector};

pub struct MapperCtx {
    pub cfg_name: String,
    pub name: String,
    pub version: String,
    pub store: Store<HostEngine>,
    pub proc: Processor,
    pub selectors: Vec<CompiledSelector>,
}

pub struct Mappers {
    pub mappers: Vec<MapperCtx>,
}

impl Mappers {
    pub async fn load_all(
        engine: &WasmEngine,
        components: &Vec<(&String, Component)>,
    ) -> anyhow::Result<Self> {
        let mut mappers = Vec::with_capacity(components.len());

        for (name, component) in components {
            let mut store = engine.make_store();

            let proc = engine.make_processor(&mut store, component).await?;
            let guest = proc.tangent_logs_mapper();

            let meta = guest.call_metadata(&mut store).await?;
            let sels: Vec<Selector> = guest.call_probe(&mut store).await?;

            let selectors: Vec<CompiledSelector> = sels
                .iter()
                .map(compile_selector)
                .collect::<anyhow::Result<_>>()?;

            mappers.push(MapperCtx {
                cfg_name: name.to_string(),
                name: meta.name,
                version: meta.version,
                store,
                proc,
                selectors,
            });
        }

        Ok(Self { mappers })
    }
}
