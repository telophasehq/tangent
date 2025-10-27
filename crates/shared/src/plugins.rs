use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PluginConfig {
    pub module_type: String,
    pub path: PathBuf,

    #[serde(default)]
    pub tests: Vec<PluginTests>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginTests {
    pub input: PathBuf,
    pub expected: PathBuf,
}
