use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    version,
    about = "Compile Python scripts to WASI WebAssembly via py2wasm"
)]
struct Args {
    /// Path to YAML config
    #[arg(long)]
    config: PathBuf,
}

fn main() -> Result<()> {
    compile_wasm::compile_from_config(&Args::parse().config)
}
