use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use crate::dag::{Edge, NodeRef};
use crate::sinks::common::SinkConfig;
use crate::sources::common::SourceConfig;

pub mod dag;
pub mod plugins;
pub mod runtime;
pub mod sinks;
pub mod sources;

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    pub runtime: runtime::RuntimeConfig,

    #[serde(default)]
    pub sources: std::collections::BTreeMap<String, SourceConfig>,

    #[serde(default)]
    pub sinks: std::collections::BTreeMap<String, SinkConfig>,

    #[serde(default)]
    pub plugins: std::collections::BTreeMap<String, plugins::PluginConfig>,

    #[serde(default)]
    pub dag: Vec<Edge>,
}

impl Config {
    pub fn from_file(path: &PathBuf) -> Result<Self> {
        let bytes = fs::read(path).with_context(|| format!("reading {}", path.display()))?;
        let cfg = serde_yaml::from_slice(&bytes)
            .with_context(|| format!("parsing YAML {}", path.display()))?;

        Ok(cfg)
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
