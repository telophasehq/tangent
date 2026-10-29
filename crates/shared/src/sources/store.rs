use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StoreSourceConfig {
    pub buffer_secs: u64,
    #[serde(default = "default_late_secs")]
    pub late_secs: u64,
    #[serde(default)]
    pub idle_secs: Option<u64>, // late_secs when unset
    #[serde(default)]
    pub grace_secs: Option<u64>, // late_secs when unset
    #[serde(default)]
    pub ranges: Vec<TimeRange>,
    #[serde(default = "default_in_flight")]
    pub in_flight: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TimeRange {
    pub from: String,
    pub to: Option<String>,
}

const fn default_late_secs() -> u64 {
    1200
}
const fn default_in_flight() -> usize {
    16
}
