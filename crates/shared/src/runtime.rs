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

    /// The state store plugins (and the runtime) keep keyed state in.
    /// `cache:` is accepted as the older name of this section.
    #[serde(default, alias = "cache")]
    pub state: StateConfig,

    /// How the stream is partitioned. With a partition key set, a plugin's
    /// state calls are scoped to the partition of the record it is processing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition: Option<PartitionConfig>,

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

/// Which fields of a record identify its partition, e.g. `[org_id]`. The
/// values are joined with `separator`; a record with none of the fields is
/// unpartitioned.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionConfig {
    pub key: Vec<String>,

    #[serde(default = "default_separator")]
    pub separator: String,
}

fn default_separator() -> String {
    "|".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateConfig {
    #[serde(default)]
    pub backend: StateBackend,

    /// SQLite file (relative to the config), for the `sqlite` backend.
    #[serde(default = "default_state_path")]
    pub path: PathBuf,

    /// TTL applied when a write gives none; 0 means entries never expire.
    #[serde(default = "default_state_ttl_ms")]
    pub default_ttl_ms: u64,

    /// Longest TTL a write may ask for; 0 means no cap.
    #[serde(default = "default_state_max_ttl_ms")]
    pub max_ttl_ms: u64,

    #[serde(default = "default_state_lock_timeout_ms")]
    pub lock_timeout_ms: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum StateBackend {
    /// Durable, queryable with any SQLite client, shared by all workers.
    #[default]
    Sqlite,
    /// Process-local and gone on exit; for tests and benchmarks.
    Memory,
}

impl Default for StateConfig {
    fn default() -> Self {
        Self {
            backend: StateBackend::default(),
            path: default_state_path(),
            default_ttl_ms: default_state_ttl_ms(),
            max_ttl_ms: default_state_max_ttl_ms(),
            lock_timeout_ms: default_state_lock_timeout_ms(),
        }
    }
}

fn default_state_path() -> PathBuf {
    "state.sqlite".into()
}

const fn default_state_ttl_ms() -> u64 {
    10 * 60 * 1000
}

const fn default_state_max_ttl_ms() -> u64 {
    60 * 60 * 1000
}

const fn default_state_lock_timeout_ms() -> u64 {
    30_000
}

/// The former name of the state section.
pub type CacheConfig = StateConfig;
