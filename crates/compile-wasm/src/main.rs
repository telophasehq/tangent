use anyhow::{Context, Result};
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
    /// Path to WIT directory
    #[arg(long, default_value = "./wit")]
    wit: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let config = args
        .config
        .canonicalize()
        .with_context(|| format!("config path not found: {}", args.config.display()))?;
    let wit = args
        .wit
        .canonicalize()
        .with_context(|| format!("WIT path not found: {}", args.wit.display()))?;

    compile_wasm::compile_from_config(&config, &wit)
}
