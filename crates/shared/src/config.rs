use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::dag::{Edge, NodeRef};
use crate::sinks::common::SinkConfig;
use crate::sources::common::SourceConfig;

pub mod dag;
pub mod plugins;
pub mod runtime;
pub mod sinks;
pub mod sources;
pub mod wasm_engine;

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    pub runtime: runtime::RuntimeConfig,

    #[serde(default)]
    pub sources: std::collections::BTreeMap<Arc<str>, SourceConfig>,

    #[serde(default)]
    pub sinks: std::collections::BTreeMap<Arc<str>, SinkConfig>,

    #[serde(default)]
    pub plugins: std::collections::BTreeMap<Arc<str>, plugins::PluginConfig>,

    #[serde(default)]
    pub dag: Vec<Edge>,
}

impl Config {
    pub fn from_file(path: &PathBuf) -> Result<Self> {
        let contents =
            fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
        let expanded = Self::expand_env(&contents);
        let cfg = serde_yaml::from_str(&expanded)
            .with_context(|| format!("parsing YAML {}", path.display()))?;

        Ok(cfg)
    }

    pub const fn batch_age_ms(&self) -> Duration {
        Duration::from_millis(self.runtime.batch_age)
    }

    pub const fn batch_size_kb(&self) -> usize {
        self.runtime.batch_size << 10
    }

    fn expand_env(s: &str) -> String {
        let mut out = String::new();
        let mut rest = s;

        while let Some(start) = rest.find("${") {
            out.push_str(&rest[..start]);
            if let Some(end) = rest[start + 2..].find('}') {
                let var = &rest[start + 2..start + 2 + end];
                let val = std::env::var(var).unwrap_or_default();
                out.push_str(&val);
                rest = &rest[start + 3 + end..];
            } else {
                // no closing }, just append
                out.push_str(&rest[start..]);
                break;
            }
        }
        out.push_str(rest);
        out
    }

    pub fn validate(&self) -> Result<()> {
        let mut missing: Vec<String> = Vec::new();
        let exists = |n: &NodeRef, this: &Config| -> bool {
            match n {
                NodeRef::Source { name } => this.sources.contains_key(name),
                NodeRef::Plugin { name } => this.plugins.contains_key(name),
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
