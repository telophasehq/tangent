use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RuntimeConfig {
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    #[serde(default = "default_batch_age")]
    pub batch_age: u64,

    #[serde(default = "default_workers")]
    pub workers: usize,

    #[serde(default = "default_plugin_path")]
    pub plugins_path: PathBuf,

    #[serde(default = "default_cache")]
    pub cache: CacheConfig,

    /// When true, the runtime will not make outbound HTTP requests from plugins.
    /// Useful for `tangent plugin test` or benchmarking to avoid external calls.
    #[serde(default)]
    pub disable_remote_calls: bool,
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

fn default_cache() -> CacheConfig {
    CacheConfig::default()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheConfig {
    #[serde(default = "default_cache_path")]
    pub path: PathBuf,

    #[serde(default = "default_cache_ttl_ms")]
    pub default_ttl_ms: u64,

    #[serde(default = "default_cache_max_ttl_ms")]
    pub max_ttl_ms: u64,

    #[serde(default = "default_cache_lock_timeout_ms")]
    pub lock_timeout_ms: u64,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            path: default_cache_path(),
            default_ttl_ms: default_cache_ttl_ms(),
            max_ttl_ms: default_cache_max_ttl_ms(),
            lock_timeout_ms: default_cache_lock_timeout_ms(),
        }
    }
}

fn default_cache_path() -> PathBuf {
    "cache.sqlite".into()
}

const fn default_cache_ttl_ms() -> u64 {
    10 * 60 * 1000
}

const fn default_cache_max_ttl_ms() -> u64 {
    60 * 60 * 1000
}

const fn default_cache_lock_timeout_ms() -> u64 {
    30_000
}
