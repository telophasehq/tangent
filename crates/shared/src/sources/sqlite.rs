use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqliteConfig {
    pub path: PathBuf,

    pub table: String,

    pub partition: String, // a sqlite sink's partition

    pub batch_size: usize,

    pub timeout_secs: u64,
}
