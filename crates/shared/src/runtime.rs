use std::path::PathBuf;

use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct RuntimeConfig {
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    #[serde(default = "default_batch_age")]
    pub batch_age: u64,

    #[serde(default = "default_workers")]
    pub workers: usize,

    #[serde(default = "default_plugin_path")]
    pub plugins_path: PathBuf,
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

fn default_plugin_path() -> PathBuf {
    "plugins/".into()
}
