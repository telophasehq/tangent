use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PluginConfig {
    pub module_type: String,
    pub path: PathBuf,
}
