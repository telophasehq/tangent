use std::path::PathBuf;

use anyhow::Result;
use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use tangent_bench::BenchOptions;
use tangent_runtime::RuntimeOptions;

mod scaffold;
mod test;
mod wit_assets;

#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Parser, Debug)]
#[command(name = "tangent", version, about = "Tangent CLI")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    Run {
        /// Path to YAML config
        #[arg(long, value_name = "FILE")]
        config: PathBuf,
        /// Exit after one drain cycle (for tests)
        #[arg(long, default_value_t = false)]
        once: bool,
    },

    Bench {
        /// Path to tangent.yaml
        #[arg(long, value_name = "FILE")]
        config: PathBuf,

        /// Duration (seconds)
        #[arg(long, default_value_t = 15)]
        seconds: u64,

        /// Concurrent connections
        #[arg(long, default_value_t = 2)]
        connections: u16,

        /// Payload filepath.
        #[arg(long)]
        payload: PathBuf,

        /// Batch-bytes cap per write (0 = disabled)
        #[arg(long, default_value_t = 65_536)]
        max_bytes: usize,

        /// Prometheus metrics endpoint
        #[arg(long, default_value = "http://127.0.0.1:9184/metrics")]
        metrics_url: String,

        /// For S3 + SQS bench
        #[arg(long)]
        bucket: Option<String>,

        #[arg(long)]
        object_prefix: Option<String>,

        /// disable metrics. Only use if benchmarking outside of tangent run.
        #[arg(long, default_value_t = false)]
        disable_metrics: bool,

        /// Synthesize logs. Used to generate payloads from the input payload.
        #[arg(long, default_value_t = false)]
        synthesize: bool,
    },

    Plugin {
        #[command(subcommand)]
        command: PluginCommands,
    },
}

#[derive(Subcommand, Debug)]
enum PluginCommands {
    /// Scaffold a new plugin project
    Scaffold {
        /// Project name (folder will be created with this name)
        #[arg(long)]
        name: String,
        /// Language: go|py|rust
        #[arg(long)]
        lang: String,
    },
    /// Test a plugin with input/expected fixtures
    Test {
        /// Test a specific plugin
        #[arg(long)]
        plugin: Option<String>,
        /// Runtime config
        #[arg(long, value_name = "FILE")]
        config: PathBuf,
    },

    /// Compile a WASM component from a config (py via componentize-py; go via TinyGo)
    Compile {
        /// Path to YAML config (must contain entry_point, module_type)
        #[arg(long, value_name = "FILE")]
        config: PathBuf,
        /// Path to WIT directory (folder with the `processor` world)
        #[arg(long, default_value = ".tangent/wit", value_name = "DIR")]
        wit: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt().with_env_filter(filter).init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Run { config, once } => {
            let cfg = config.canonicalize().unwrap_or(config);
            let opts = RuntimeOptions {
                once,
                ..Default::default()
            };

            tangent_runtime::run(&cfg, opts).await?
        }
        Commands::Bench {
            config,
            seconds,
            connections,
            payload,
            max_bytes,
            metrics_url,
            bucket,
            object_prefix,
            disable_metrics,
            synthesize,
        } => {
            let opts = BenchOptions {
                config_path: Some(config.clone()),
                seconds,
                connections,
                payload,
                max_bytes,
                metrics_url,
                bucket,
                object_prefix,
                disable_metrics,
                synthesize,
            };
            tangent_bench::run(&config, opts).await?;
        }

        Commands::Plugin { command } => match command {
            PluginCommands::Compile { config, wit } => {
                // resolve to absolute paths to help downstream error messages
                let cfg = config.canonicalize().unwrap_or(config);
                let wit = wit.canonicalize().unwrap_or(wit);
                compile_wasm::compile_from_config(&cfg, &wit)?;
            }
            PluginCommands::Scaffold { name, lang } => scaffold::scaffold(&name, &lang)?,
            PluginCommands::Test { plugin, config } => {
                let config = config.canonicalize().unwrap_or(config);
                test::run(test::TestOptions {
                    plugin,
                    config_path: config,
                })
                .await?;
            }
        },
    }

    Ok(())
}
