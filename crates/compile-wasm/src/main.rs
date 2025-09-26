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
    /// Path to WIT directory
    #[arg(long, default_value = "./wit")]
    wit: PathBuf,
}

fn main() -> Result<()> {
    let args = &Args::parse();
    compile_wasm::compile_from_config(&args.config, &args.wit)
}
