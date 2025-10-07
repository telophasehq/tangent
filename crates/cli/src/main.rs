use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use tangent_runtime::RuntimeOptions;

#[derive(Parser, Debug)]
#[command(name = "tangent", version, about = "Tangent CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Compile a WASM component from a config (py via componentize-py; go via TinyGo)
    CompileWasm {
        /// Path to YAML config (must contain entry_point, module_type)
        #[arg(long, value_name = "FILE")]
        config: PathBuf,
        /// Path to WIT directory (folder with the `processor` world)
        #[arg(long, default_value = "./wit", value_name = "DIR")]
        wit: PathBuf,
        /// Path to out directory
        #[arg(long, default_value = "./compiled", value_name = "DIR")]
        out: PathBuf,
    },

    Run {
        /// Path to YAML config
        #[arg(long, value_name = "FILE")]
        config: PathBuf,
        /// Exit after one drain cycle (for tests)
        #[arg(long, default_value_t = false)]
        once: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let cli = Cli::parse();

    match cli.command {
        Commands::CompileWasm { config, wit, out } => {
            // resolve to absolute paths to help downstream error messages
            let cfg = config.canonicalize().unwrap_or(config);
            let wit = wit.canonicalize().unwrap_or(wit);
            compile_wasm::compile_from_config(&cfg, &wit, &out)?;
        }

        Commands::Run { config, once } => {
            let cfg = config.canonicalize().unwrap_or(config);
            let opts = RuntimeOptions {
                once,
                ..Default::default()
            };
            tangent_runtime::run(&cfg, opts).await?;
        }
    }

    Ok(())
}
