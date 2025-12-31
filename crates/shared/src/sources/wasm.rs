use ahash::HashMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WasmSourceConfig {
    /// Path to a compiled WebAssembly component (e.g., my-source.cwasm).
    pub path: PathBuf,

    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,

    /// Configuration values exposed to the guest via the `config` interface.
    #[serde(default)]
    pub config: HashMap<String, Value>,
}

fn default_poll_interval_ms() -> u64 {
    1_000
}
