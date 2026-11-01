use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SqliteConfig {
    pub path: PathBuf,

    pub table: String,

    /// The field holding a record's own time (RFC 3339 or epoch), stored as
    /// `at`: it orders a partition's rows and clocks a reader's timeout.
    /// Arrival time without it.
    #[serde(default)]
    pub timestamp_key: Option<String>,

    #[serde(default)]
    pub partitions: BTreeMap<String, Vec<String>>, // name: fields
}
