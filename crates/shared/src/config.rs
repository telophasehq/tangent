use anyhow::{Context, Result};
use serde::Deserialize;
use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use crate::sinks::common::SinkConfig;
use crate::sources::common::SourceConfig;

pub mod plugins;
pub mod runtime;
pub mod sinks;
pub mod sources;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub runtime: runtime::RuntimeConfig,

    #[serde(default)]
    pub sources: std::collections::BTreeMap<String, SourceConfig>,

    #[serde(default)]
    pub sinks: std::collections::BTreeMap<String, SinkConfig>,

    #[serde(default)]
    pub plugins: std::collections::BTreeMap<String, plugins::PluginConfig>,
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
}
