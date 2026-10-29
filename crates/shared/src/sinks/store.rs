use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StoreSinkConfig {
    #[serde(default = "default_retain_secs")]
    pub retain_secs: u64,
    #[serde(default)]
    pub recent: BTreeMap<String, String>, // scope: field, indexed for history
    #[serde(default)]
    pub novel: BTreeMap<String, Vec<String>>, // scope: fields whose unseen values mark `_novel`
    #[serde(default)]
    pub follow: Option<String>, // 1h: a value's records after a new one are marked too
}

const fn default_retain_secs() -> u64 {
    2_592_000 // 30d
}
