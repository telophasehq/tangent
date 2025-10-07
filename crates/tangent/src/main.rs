use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;
use tangent_runtime::{run, RuntimeOptions};
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(version, about = "Run log processor")]
struct Args {
    /// Path to YAML config
    #[arg(long)]
    config: PathBuf,
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let (nb, _guard) = tracing_appender::non_blocking(std::io::stderr());
    tracing_subscriber::fmt()
        .with_writer(nb)
        .with_env_filter(filter)
        .init();

    let args = Args::parse();

    run(&args.config, RuntimeOptions::default()).await
}
