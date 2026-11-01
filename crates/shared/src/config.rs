use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crate::dag::{Edge, NodeRef};
use crate::sinks::common::SinkConfig;
use crate::sources::common::SourceConfig;

pub mod dag;
pub mod enrichment;
pub mod include;
pub mod inference;
pub mod plugins;
pub mod runtime;
pub mod sinks;
pub mod sources;
pub mod wasm_engine;

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    #[serde(default)]
    pub runtime: runtime::RuntimeConfig,

    #[serde(default)]
    pub sources: std::collections::BTreeMap<Arc<str>, SourceConfig>,

    #[serde(default)]
    pub sinks: std::collections::BTreeMap<Arc<str>, SinkConfig>,

    #[serde(default)]
    pub plugins: std::collections::BTreeMap<Arc<str>, plugins::PluginConfig>,

    /// Inference nodes, each with its own model and prompt, addressed as
    /// `kind: inference` in the DAG.
    #[serde(default)]
    pub inference: std::collections::BTreeMap<Arc<str>, inference::InferenceConfig>,

    /// Reference data loaded once per process and shared by every plugin,
    /// each looked up by its name.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub enrichment_tables: std::collections::BTreeMap<Arc<str>, enrichment::TableConfig>,

    #[serde(default)]
    pub dag: Vec<Edge>,
}

/// A config and the files it was read from.
pub struct Loaded {
    pub config: Config,
    /// Every file merged, each after the files it includes; the one named
    /// last.
    pub files: Vec<PathBuf>,
}

impl Config {
    pub fn from_file(path: &PathBuf) -> Result<Self> {
        Ok(Self::load(path)?.config)
    }

    /// The config at `path`, with what it includes (see [`include`]). A file
    /// without `include:` is read as it always was, but that a top-level key
    /// tangent does not know is an error (one starting `x-` aside).
    pub fn load(path: &Path) -> Result<Loaded> {
        let read = include::read(path)?;
        if !read.map.contains_key("include") {
            let config = serde_yaml::from_str(&read.text)
                .with_context(|| format!("parsing YAML {}", path.display()))?;
            return Ok(Loaded {
                config,
                files: vec![path.to_path_buf()],
            });
        }

        let merged = include::merge(path, read)?;
        let mut config: Config =
            serde_path_to_error::deserialize(serde_yaml::Deserializer::from_str(&merged.text))
                .map_err(|e| merged.explain(e))
                .with_context(|| format!("loading {}", path.display()))?;
        config.dag = dedupe(std::mem::take(&mut config.dag));
        Ok(Loaded {
            config,
            files: merged.files,
        })
    }

    pub const fn batch_age_ms(&self) -> Duration {
        Duration::from_millis(self.runtime.batch_age)
    }

    pub const fn batch_size_kb(&self) -> usize {
        self.runtime.batch_size << 10
    }

    pub fn validate(&self) -> Result<()> {
        let mut missing: Vec<String> = Vec::new();
        let exists = |n: &NodeRef, this: &Config| -> bool {
            match n {
                NodeRef::Source { name } => this.sources.contains_key(name),
                NodeRef::Plugin { name } => this.plugins.contains_key(name),
                NodeRef::Inference { name } => this.inference.contains_key(name),
                NodeRef::Sink {
                    name,
                    key_prefix: _,
                } => this.sinks.contains_key(name),
            }
        };

        for e in &self.dag {
            if !exists(&e.from, self) {
                missing.push(format!("from {:?} does not exist", e.from));
            }
            for t in &e.to {
                if !exists(&t, self) {
                    missing.push(format!("to {:?} does not exist", t));
                }
            }
        }
        if !missing.is_empty() {
            anyhow::bail!(
                "DAG references missing nodes:\n  - {}",
                missing.join("\n  - ")
            );
        }

        Ok(())
    }
}

/// Each edge once: a target a node already sends to is dropped from a later
/// edge, and an edge left with no target goes. The router extends a node's
/// targets with every edge from it, so an edge two files both state would
/// otherwise deliver every record twice.
fn dedupe(dag: Vec<Edge>) -> Vec<Edge> {
    let mut seen: HashSet<(NodeRef, NodeRef)> = HashSet::new();
    dag.into_iter()
        .filter_map(|mut e| {
            let had = !e.to.is_empty();
            let from = e.from.clone();
            e.to.retain(|t| seen.insert((from.clone(), t.clone())));
            (!had || !e.to.is_empty()).then_some(e)
        })
        .collect()
}
