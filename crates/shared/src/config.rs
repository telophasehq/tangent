use anyhow::{Context, Result};
use serde::Deserialize;
use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use crate::sinks::common::SinkConfig;
use crate::sources::common::SourceConfig;

pub mod sinks;
pub mod sources;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub entry_point: String,
    pub module_type: String,

    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    #[serde(default = "default_batch_age")]
    pub batch_age: u64,

    #[serde(default = "default_workers")]
    pub workers: usize,

    #[serde(default)]
    pub sources: std::collections::BTreeMap<String, SourceConfig>,

    #[serde(default)]
    pub sinks: std::collections::BTreeMap<String, SinkConfig>,

    #[serde(default)]
    pub plugins: Vec<PathBuf>,
}

impl Config {
    pub fn from_file(path: &PathBuf) -> Result<Self> {
        let bytes = fs::read(path).with_context(|| format!("reading {}", path.display()))?;
        let cfg = serde_yaml::from_slice(&bytes)
            .with_context(|| format!("parsing YAML {}", path.display()))?;

        Ok(cfg)
    }

    pub const fn batch_age_ms(&self) -> Duration {
        Duration::from_millis(self.batch_age)
    }

    pub const fn batch_size_kb(&self) -> usize {
        self.batch_size << 10
    }
}

#[must_use]
const fn default_batch_size() -> usize {
    256
}

#[must_use]
const fn default_batch_age() -> u64 {
    5
}
fn default_workers() -> usize {
    num_cpus::get()
}
