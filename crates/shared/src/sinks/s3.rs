use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct S3Config {
    pub bucket_name: String,
    pub region: Option<String>,

    /// What each object's key starts with, before `/` and its name: a
    /// strftime template rendered in UTC when a write reaches the sink, as a
    /// rolling_file sink's `path` is, so `logs/cloudtrail/%Y/%m/%d` puts each
    /// day's objects under a prefix of their own (`%%` is a `%`). Without
    /// it, objects go to the bucket's root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_prefix: Option<String>,

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
