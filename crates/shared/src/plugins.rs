use ahash::HashMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PluginConfig {
    pub module_type: String,
    pub path: PathBuf,

    #[serde(default)]
    pub tests: Vec<PluginTests>,

    #[serde(default)]
    pub config: HashMap<String, Value>,

    /// Namespace of this plugin's state. Unset, every plugin shares one
    /// (`plugins`); set it to give this plugin, or a group of plugins, keys
    /// of their own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_namespace: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginTests {
    pub input: PathBuf,
    pub expected: PathBuf,
}
