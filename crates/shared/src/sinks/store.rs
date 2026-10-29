use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StoreSinkConfig {
    #[serde(default = "default_retain_secs")]
    pub retain_secs: u64,
    #[serde(default)]
    pub recent: BTreeMap<String, String>, // scope: field, indexed for history
}

const fn default_retain_secs() -> u64 {
    2_592_000 // 30d
}
