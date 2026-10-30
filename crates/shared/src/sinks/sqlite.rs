use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SqliteConfig {
    pub path: PathBuf,

    #[serde(default)]
    pub time: Option<String>,

    #[serde(default = "default_retain_secs")]
    pub retain_secs: u64,

    #[serde(default)]
    pub partitions: BTreeMap<String, Vec<String>>, // name: fields
}

const fn default_retain_secs() -> u64 {
    2_592_000 // 30d
}
