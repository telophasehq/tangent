use anyhow::{Context, Result};
use serde::Deserialize;
use std::fs;
use std::path::PathBuf;
use std::time::Duration;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub entry_point: String,
    pub module_type: String,

    #[serde(default = "default_socket_path")]
    pub socket_path: PathBuf,

    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    #[serde(default = "default_batch_age")]
    pub batch_age: u64,

    #[serde(default = "default_workers")]
    pub workers: usize,
}

fn default_socket_path() -> PathBuf {
    "/tmp/sidecar.sock".into()
}
fn default_batch_size() -> usize {
    256
}
fn default_batch_age() -> u64 {
    5
}
fn default_workers() -> usize {
    num_cpus::get()
}

impl Config {
    pub fn from_file(path: &PathBuf) -> Result<Self> {
        let bytes = fs::read(path).with_context(|| format!("reading {}", path.display()))?;
        let cfg: Config = serde_yaml::from_slice(&bytes)
            .with_context(|| format!("parsing YAML {}", path.display()))?;

        Ok(cfg)
    }

    pub fn batch_age_ms(&self) -> Duration {
        Duration::from_millis(self.batch_age)
    }

    pub fn batch_size_kb(&self) -> usize {
        self.batch_size << 10
    }
}
