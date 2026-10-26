//! `tangent state`: look inside a pipeline's state store, while it runs.
//! `ls`, `get` and `scan` go through the `StateStore` trait and work on any
//! backend that can be opened from outside the process; `sql` is the sqlite
//! backend's own.

use std::path::{Path, PathBuf};

use anyhow::{bail, Result};
use clap::Subcommand;
use tangent_runtime::state::{self, sqlite::SqliteStore, sqlite_path};
use tangent_shared::runtime::StateBackend;
use tangent_shared::Config;

#[derive(Subcommand, Debug)]
pub enum StateCommands {
    /// Key counts per namespace and partition
    Ls {
        #[arg(long)]
        ns: Option<String>,
    },
    /// One entry, as JSON
    Get {
        key: String,
        /// Namespace; plugin state is in "plugins"
        #[arg(long, default_value = "plugins")]
        ns: String,
        /// Partition; empty for an unpartitioned stream
        #[arg(long, default_value = "")]
        partition: String,
    },
    /// Live entries by key prefix, one JSON object per line
    Scan {
        #[arg(long)]
        ns: Option<String>,
        #[arg(long)]
        partition: Option<String>,
        #[arg(long, default_value = "")]
        prefix: String,
        #[arg(long, default_value_t = 100)]
        limit: usize,
    },
    /// A read-only SQL statement against the sqlite backend (table `state`)
    Sql {
        query: String,
        #[arg(long, default_value_t = 1000)]
        limit: usize,
    },
}

pub async fn run(config: &PathBuf, cmd: StateCommands) -> Result<()> {
    let cfg = Config::from_file(config)?;
    let dir = config.parent().unwrap_or(Path::new("."));

    if let StateCommands::Sql { query, limit } = cmd {
        if cfg.runtime.state.backend != StateBackend::Sqlite {
            bail!(
                "`sql` needs the sqlite backend; this pipeline uses {:?}",
                cfg.runtime.state.backend
            );
        }
        let path = sqlite_path(&cfg.runtime.state, dir);
        if !path.exists() {
            bail!("no state at {} (has the pipeline run yet?)", path.display());
        }
        for row in SqliteStore::open_readonly(&path)?.sql(&query, limit)? {
            println!("{}", serde_json::Value::Object(row));
        }
        return Ok(());
    }

    let store = state::open_reader(&cfg.runtime.state, dir)?;
    match cmd {
        StateCommands::Ls { ns } => {
            let counts = store.counts(ns.as_deref()).await?;
            if counts.is_empty() {
                eprintln!("(no live entries)");
            }
            println!("{:<24} {:<40} {:>8}", "namespace", "partition", "keys");
            for c in counts {
                let partition = if c.partition.is_empty() {
                    "-"
                } else {
                    &c.partition
                };
                println!("{:<24} {:<40} {:>8}", c.ns, partition, c.keys);
            }
        }
        StateCommands::Get { key, ns, partition } => {
            match store.get(&ns, &partition, &key).await? {
                Some(v) => println!("{}", serde_json::to_string(&v)?),
                None => {
                    eprintln!("not found: ns={ns} partition={partition:?} key={key}");
                    std::process::exit(1);
                }
            }
        }
        StateCommands::Scan {
            ns,
            partition,
            prefix,
            limit,
        } => {
            for e in store
                .scan(ns.as_deref(), partition.as_deref(), &prefix, limit)
                .await?
            {
                println!("{}", serde_json::to_string(&e)?);
            }
        }
        StateCommands::Sql { .. } => unreachable!("handled above"),
    }
    Ok(())
}
