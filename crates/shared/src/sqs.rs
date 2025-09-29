use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct SQSConfig {
    pub queue_url: String,
    #[serde(default = "default_wait_time_seconds")]
    pub wait_time_seconds: i64,
    #[serde(default = "default_visibility_timeout")]
    pub visibility_timeout: i64,
}

fn default_wait_time_seconds() -> i64 {
    20
}

fn default_visibility_timeout() -> i64 {
    60
}
