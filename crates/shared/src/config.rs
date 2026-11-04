use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crate::dag::{Edge, NodeRef};
use crate::sinks::common::{Encoding, SinkConfig, SinkKind};
use crate::sinks::rolling_file;
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

        // a file sink appends each write's encoding to one file, which only
        // NDJSON survives: a Parquet, Avro or JSON file after another is not one
        for (name, sink) in &self.sinks {
            if let SinkKind::File(_) = &sink.kind {
                if !matches!(sink.common.encoding, Encoding::NDJSON) {
                    anyhow::bail!(
                        "sink '{name}': a file sink appends to one file, which only NDJSON \
                         survives; write {} with a rolling_file sink",
                        sink.common.encoding.extension()
                    );
                }
            }
        }

        // a rolling_file sink's key prefix is a directory below its path
        for t in self.dag.iter().flat_map(|e| &e.to) {
            if let NodeRef::Sink {
                name,
                key_prefix: Some(prefix),
            } = t
            {
                if let SinkKind::RollingFile(_) = &self.sinks[name].kind {
                    rolling_file::subdir(prefix).with_context(|| format!("sink '{name}'"))?;
                }
            }
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

#[cfg(test)]
mod tests {
    use super::*;

    fn with_prefix(prefix: &str) -> Config {
        serde_yaml::from_str(&format!(
            "sources:\n  lines: {{ type: tcp, bind_address: \"127.0.0.1:9\" }}\n\
             sinks:\n  lake: {{ type: rolling_file, path: /lake/out/%Y/%m/%d, wal_path: /lake/wal }}\n\
             dag:\n  - from: {{ kind: source, name: lines }}\n    \
             to: [{{ kind: sink, name: lake, key_prefix: \"{prefix}\" }}]\n"
        ))
        .unwrap()
    }

    #[test]
    fn a_rolling_file_sinks_key_prefix_stays_below_its_path() {
        assert!(with_prefix("acme/day").validate().is_ok());
        for bad in ["../up", "/abs", "a/../../b"] {
            let e = with_prefix(bad).validate().unwrap_err();
            assert!(format!("{e:#}").contains("relative path"), "{bad}: {e:#}");
        }
    }

    #[test]
    fn a_file_sink_takes_ndjson_only() {
        let file = |encoding: &str| -> Config {
            serde_yaml::from_str(&format!(
                "sources:\n  lines: {{ type: tcp, bind_address: \"127.0.0.1:9\" }}\n\
                 sinks:\n  out: {{ type: file, path: /tmp/out, encoding: {encoding} }}\n\
                 dag:\n  - from: {{ kind: source, name: lines }}\n    \
                 to: [{{ kind: sink, name: out }}]\n"
            ))
            .unwrap()
        };
        assert!(file("{ type: ndjson }").validate().is_ok());
        let parquet = "{ type: parquet, schema: { n: int } }";
        for enc in [parquet, "{ type: json }"] {
            let e = file(enc).validate().unwrap_err();
            assert!(e.to_string().contains("rolling_file"), "{enc}: {e}");
        }
    }
}
