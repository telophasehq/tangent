use anyhow::Result;

use wasmtime::{Config, Engine};

pub fn build() -> Result<Engine> {
    let mut cfg = Config::new();
    // Async support is always on since wasmtime 48; the linker and the
    // calls say `_async` where it matters.
    cfg.wasm_component_model(true)
        .debug_info(false)
        .wasm_backtrace_max_frames(None)
        .generate_address_map(false)
        .native_unwind_info(false)
        .profiler(wasmtime::ProfilingStrategy::None)
        .parallel_compilation(false)
        .allocation_strategy(wasmtime::InstanceAllocationStrategy::pooling());

    Ok(Engine::new(&cfg)?)
}
