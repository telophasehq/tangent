use std::path::{Path, PathBuf};

use anyhow::Result;
use clap::{Parser, Subcommand};
use serde_yaml::Value;
use tracing_subscriber::EnvFilter;

use tangent_bench::BenchOptions;
use tangent_runtime::RuntimeOptions;

mod fetch;
mod scaffold;
mod state;
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

    /// Print the config as tangent reads it: its includes merged, `${VAR}`
    /// expanded, defaults filled in. Secrets (tokens, api_key, webhook_url,
    /// passwords) are left out, so the output is for reading, not for running
    Config {
        /// Path to YAML config
        #[arg(long, value_name = "FILE")]
        config: PathBuf,
    },

    /// Read a past range of the config's sources through it, as fast as it
    /// is read, then close every group and wait for the judge. Each source
    /// whose past tangent can read is read (a cloudtrail source's, from its
    /// account's Event history), and the rest are left out, as are Slack, S3
    /// and rolling_file sinks. The history it builds starts empty and is gone
    /// after; a live pipeline's state and lake are left as they are
    Replay {
        /// Path to YAML config
        #[arg(long, value_name = "FILE")]
        config: PathBuf,
        /// Start of the range: a date (2026-10-01, midnight UTC), an RFC 3339
        /// time, or a span before now (1d, 12h)
        #[arg(long, value_name = "WHEN")]
        from: String,
        /// End of the range, as --from; five minutes ago when left out, since
        /// CloudTrail typically has an event within five minutes of its call
        #[arg(long, value_name = "WHEN")]
        to: Option<String>,
    },

    /// Inspect a pipeline's state store (read-only, works while it runs)
    State {
        /// Path to YAML config; the store is found from `runtime.state`
        #[arg(long, value_name = "FILE")]
        config: PathBuf,
        #[command(subcommand)]
        command: state::StateCommands,
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
        /// Also run the project's setup.sh: installs the language toolchain
        /// and may add PATH lines to your shell profile
        #[arg(long)]
        setup: bool,
    },
    /// Test a plugin with input/expected fixtures
    Test {
        /// Test a specific plugin
        #[arg(long)]
        plugin: Option<String>,
        /// Runtime config
        #[arg(long, value_name = "FILE")]
        config: PathBuf,

        /// Enable http calls in tests
        #[arg(long, default_value_t = false)]
        enable_http: bool,
    },

    /// Compile a WASM component from a config (py via componentize-py; go via TinyGo)
    Compile {
        /// Path to YAML config (must contain entry_point, module_type)
        #[arg(long, value_name = "FILE")]
        config: PathBuf,
        /// A WIT directory (with the `processor` world) to compile every plugin
        /// against. Without it, each plugin compiles against this tangent's
        /// own WIT, written into the plugin's `.tangent/wit` first
        #[arg(long, value_name = "DIR")]
        wit: Option<PathBuf>,
        /// Compile one plugin, not every plugin in the config
        #[arg(long)]
        plugin: Option<String>,
    },

    /// Download the plugins a config names as `prebuilt`, compiled by their
    /// repository's CI, and check GitHub's attestation of each before it is
    /// kept, in tangent's cache for this version
    /// (~/.cache/tangent/<version>/plugins), in place of any `tangent plugin
    /// compile` build: no WASM toolchain needed, only `gh`. `tangent run`
    /// fetches the ones it is missing; this fetches them again, or ahead of
    /// time
    Fetch {
        /// Path to YAML config
        #[arg(long, value_name = "FILE")]
        config: PathBuf,
        /// Fetch one plugin, not every prebuilt plugin in the config
        #[arg(long)]
        plugin: Option<String>,
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
            fetch::missing(&cfg).await?;
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

        Commands::Config { config } => print!("{}", show_config(&config)?),

        Commands::Replay { config, from, to } => {
            let config = config.canonicalize().unwrap_or(config);
            fetch::missing(&config).await?;
            let state = tempfile::tempdir()?;
            tangent_runtime::replay::replay(tangent_runtime::replay::ReplayOptions {
                config,
                from,
                to,
                state_dir: state.path().to_path_buf(),
            })
            .await?;
        }

        Commands::State { config, command } => {
            let cfg = config.canonicalize().unwrap_or(config);
            state::run(&cfg, command).await?;
        }

        Commands::Plugin { command } => match command {
            PluginCommands::Compile {
                config,
                wit,
                plugin,
            } => {
                // resolve to absolute paths to help downstream error messages
                let cfg = config.canonicalize().unwrap_or(config);
                let wit = wit.map(|w| w.canonicalize().unwrap_or(w));
                let wit = match &wit {
                    Some(dir) => compile_wasm::Wit::Dir(dir),
                    None => compile_wasm::Wit::Write(&scaffold::write_embedded_wit),
                };
                compile_wasm::compile_from_config(&cfg, wit, plugin.as_deref())?;
            }
            PluginCommands::Fetch { config, plugin } => {
                let config = config.canonicalize().unwrap_or(config);
                fetch::run(fetch::FetchOptions {
                    config_path: config,
                    plugin,
                })
                .await?;
            }
            PluginCommands::Scaffold { name, lang, setup } => {
                scaffold::scaffold(&name, &lang, setup)?
            }
            PluginCommands::Test {
                plugin,
                config,
                enable_http,
            } => {
                let config = config.canonicalize().unwrap_or(config);
                test::run(test::TestOptions {
                    plugin,
                    config_path: config,
                    enable_http: enable_http,
                })
                .await?;
            }
        },
    }

    Ok(())
}

/// The config at `path` as YAML, after a header naming the files it was
/// merged from. Every path that names a file is absolute; `runtime` paths
/// are as written, relative to `path`. The DAG, each s3 sink's `key_prefix`,
/// each s3 and rolling_file sink's `timestamp_field`, and the required
/// tables' files are checked after: a config that does not hold together
/// still prints, then fails. What `run` would warn of is warned of on stderr.
fn show_config(path: &Path) -> Result<String> {
    let loaded = tangent_shared::Config::load(path)?;
    let dir = path
        .canonicalize()?
        .parent()
        .map(PathBuf::from)
        .unwrap_or_default();
    let mut value = serde_yaml::to_value(&loaded.config)?;
    if let Value::Mapping(m) = &mut value {
        tangent_shared::include::rebase(m, &dir);
        // a plugin's own config is a hash map: in a fixed order, to diff
        if let Some(Value::Mapping(plugins)) = m.get_mut("plugins") {
            for p in plugins.values_mut() {
                if let Some(config) = p.get_mut("config") {
                    sort_keys(config);
                }
            }
        }
    }
    let mut out = String::from("# merged from, in order:\n");
    for f in &loaded.files {
        out.push_str(&format!("#   {}\n", f.display()));
    }
    out.push_str(
        "# secrets (tokens, api_key, webhook_url, passwords) are left out: read it, don't run it\n",
    );
    out.push_str(&serde_yaml::to_string(&value)?);
    // on stderr, so what is printed stays YAML
    for warning in loaded.config.warnings() {
        eprintln!("warning: {warning}");
    }
    let checked = loaded
        .config
        .validate()
        .and_then(|()| tangent_runtime::sinks::s3::check_sinks(&loaded.config.sinks))
        .and_then(|()| {
            loaded
                .config
                .enrichment_tables
                .iter()
                .try_for_each(|(name, table)| table.check(name, &dir))
        });
    if let Err(e) = checked {
        print!("{out}");
        return Err(e);
    }
    Ok(out)
}

fn sort_keys(v: &mut Value) {
    match v {
        Value::Mapping(m) => {
            let mut entries: Vec<(Value, Value)> = std::mem::take(m).into_iter().collect();
            entries.sort_by(|(a, _), (b, _)| {
                serde_yaml::to_string(a)
                    .unwrap_or_default()
                    .cmp(&serde_yaml::to_string(b).unwrap_or_default())
            });
            for (k, mut v) in entries {
                sort_keys(&mut v);
                m.insert(k, v);
            }
        }
        Value::Sequence(items) => items.iter_mut().for_each(sort_keys),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_fails_on_a_required_table_without_its_file() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("profile")).unwrap();
        std::fs::write(dir.path().join("profile/profile.yaml"), "org: acme\n").unwrap();
        let config = |path: &str| {
            let file = dir.path().join("ingest.yaml");
            std::fs::write(
                &file,
                format!(
                    "enrichment_tables:\n  profile: {{ type: document, path: {path}, required: true }}\n  geo: {{ type: mmdb, path: missing.mmdb }}\n"
                ),
            )
            .unwrap();
            show_config(&file)
        };
        let shown = config("profile/profile.yaml").unwrap();
        assert!(shown.contains("required: true"), "{shown}");
        let err = format!("{:#}", config("profle/profile.yaml").unwrap_err());
        assert!(err.contains("enrichment table profile: "), "{err}");
        assert!(
            err.contains("profle/profile.yaml not found, and the table is required"),
            "{err}"
        );
    }

    #[test]
    fn config_prints_an_s3_key_prefix_and_fails_on_one_not_strftime() {
        let dir = tempfile::tempdir().unwrap();
        let config = |prefix: &str| {
            let file = dir.path().join("tangent.yaml");
            std::fs::write(
                &file,
                format!("sinks:\n  archive: {{ type: s3, bucket_name: logs, key_prefix: \"{prefix}\" }}\n"),
            )
            .unwrap();
            show_config(&file)
        };
        let shown = config("cloudtrail/%Y/%m/%d").unwrap();
        assert!(shown.contains("key_prefix: cloudtrail/%Y/%m/%d"), "{shown}");
        let err = format!("{:#}", config("cloudtrail/%Q").unwrap_err());
        assert!(
            err.contains("sink 'archive'") && err.contains("not a strftime template"),
            "{err}"
        );
    }

    #[test]
    fn config_shows_the_default_url_only_where_it_is_read() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("tangent.yaml");
        std::fs::write(
            &file,
            "inference:\n  cloud: { backend: bedrock, model: m, prompt_file: p.md }\n  local: { model: m, prompt_file: p.md }\n",
        )
        .unwrap();
        let shown = show_config(&file).unwrap();
        let value: Value = serde_yaml::from_str(&shown).unwrap();
        assert_eq!(
            value["inference"]["cloud"].get("url"),
            None,
            "bedrock reads no url: {shown}"
        );
        assert_eq!(
            value["inference"]["local"]["url"],
            Value::from("http://localhost:8080/v1"),
            "{shown}"
        );
    }

    /// Two models wired round in a cycle: `tangent config` fails, as `run`
    /// does, before a record goes round it.
    #[test]
    fn config_fails_on_a_cycle() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("tangent.yaml");
        std::fs::write(
            &file,
            "sources:\n  input: { type: file, path: in.ndjson, decoding: { format: { type: ndjson } } }\n\
             inference:\n  first: { model: m, prompt_file: p.txt }\n  \
             second: { model: m, prompt_file: p.txt }\n\
             sinks:\n  looped: { type: file, path: looped.ndjson }\n\
             dag:\n  - from: input\n    to: [first]\n  - from: first\n    to: [second]\n  \
             - from: second\n    to: [first, looped]\n",
        )
        .unwrap();
        let err = format!("{:#}", show_config(&file).unwrap_err());
        assert!(err.contains("a cycle: first -> second -> first"), "{err}");
    }
}
