use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SqliteConfig {
    pub path: PathBuf,

    pub partition: String, // a sqlite sink's partition

    pub batch_records: usize,

    pub timeout_secs: u64,
}
