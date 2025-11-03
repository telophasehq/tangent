use anyhow::Result;

use wasmtime::{Config, Engine};

pub fn build() -> Result<Engine> {
    let mut cfg = Config::new();
    cfg.wasm_component_model(true)
        .async_support(true)
        .debug_info(false)
        .wasm_backtrace(false)
        .generate_address_map(false)
        .native_unwind_info(false)
        .profiler(wasmtime::ProfilingStrategy::None)
        .parallel_compilation(false)
        .async_support(true)
        .allocation_strategy(wasmtime::InstanceAllocationStrategy::Pooling(
            wasmtime::PoolingAllocationConfig::default(),
        ));

    Engine::new(&cfg)
}
