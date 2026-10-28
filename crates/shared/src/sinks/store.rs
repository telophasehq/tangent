use serde::{Deserialize, Serialize};

/// Keep every record in the state store, under the partition its own fields
/// give it (`runtime.partition.key`), as `ev:<epoch ms, 13 digits>:<ulid>`
/// in `namespace`, for `retain`. A partition's history is then a key range:
/// what a `store` source reads windows from and an inference node reads
/// context from. The sink also keeps, per partition, what the `store`
/// source needs to know when a window may close (namespace `windows`).
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StoreSinkConfig {
    #[serde(default = "default_namespace")]
    pub namespace: String,
    /// How long a record is kept: `30d`, `12h`, `90m`.
    #[serde(default = "default_retain")]
    pub retain: String,
    /// The field holding a record's own time; arrival time without it.
    #[serde(default = "default_time")]
    pub time: String,
}

pub fn default_namespace() -> String {
    "events".into()
}
fn default_retain() -> String {
    "30d".into()
}
fn default_time() -> String {
    "ts".into()
}
