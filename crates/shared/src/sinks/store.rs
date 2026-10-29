use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StoreConfig {
    pub time: Option<String>, // the record's time field; arrival time without it
    pub retain_ms: u64,
}
