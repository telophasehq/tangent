use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize)]
pub struct PluginConfig {
    pub entry_point: String,
    pub module_type: String,
    pub path: PathBuf,
}
