use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
pub struct S3Config {
    pub bucket_name: String,
    pub region: Option<String>,

    #[serde(default = "wal_path")]
    pub wal_path: PathBuf,

    #[serde(default = "max_file_age_seconds")]
    pub max_file_age_seconds: u64,
}

fn wal_path() -> PathBuf {
    "/tmp/wal".into()
}

const fn max_file_age_seconds() -> u64 {
    60
}
