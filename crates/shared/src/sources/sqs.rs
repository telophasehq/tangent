use serde::Deserialize;

use crate::sources::common::Decoding;

#[derive(Debug, Deserialize)]
pub struct SQSConfig {
    pub queue_url: String,
    #[serde(default = "default_wait_time_seconds")]
    pub wait_time_seconds: i64,
    #[serde(default = "default_visibility_timeout")]
    pub visibility_timeout: i64,

    #[serde(default)]
    pub decoding: Decoding,
}

const fn default_wait_time_seconds() -> i64 {
    20
}

const fn default_visibility_timeout() -> i64 {
    60
}
