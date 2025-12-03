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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginTests {
    pub input: PathBuf,
    pub expected: PathBuf,
}
