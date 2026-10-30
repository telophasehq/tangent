use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SqliteConfig {
    pub path: PathBuf,

    pub table: String,

    #[serde(default)]
    pub time: Option<String>,

    #[serde(default)]
    pub partitions: BTreeMap<String, Vec<String>>, // name: fields
}
