use serde::{Deserialize, Serialize};

/// Windows of stored records, read back from the state store: per
/// partition, consecutive spans of the records' own time. A window closes
/// when the partition's watermark (the latest record time stored for it)
/// passes the window's end plus `late`; a partition that has had no new
/// record for `idle` has its watermark moved to the clock less `late`, so
/// its last window closes too. A closed window's records are emitted as one
/// batch, in time order, each with `_span: {from, to}`; the window counts
/// as done when they have reached their sinks. Records stored after their
/// window was read are emitted again later, as a small batch marked `_late`.
///
/// Everything this needs to resume (each partition's watermark, how far it
/// was judged, the late markers) is in the store, so a restart picks up
/// where it stopped and catches up what fell due meanwhile.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StoreSourceConfig {
    /// Where the `store` sink keeps records.
    #[serde(default = "crate::sinks::store::default_namespace")]
    pub namespace: String,
    /// A window's length in the records' own time: `15m`, `1h`.
    pub span: String,
    /// How late a record may arrive and still be in its window.
    #[serde(default = "default_late")]
    pub late: String,
    /// How long a partition may go without a new record before its
    /// watermark follows the clock; `late` when unset.
    #[serde(default)]
    pub idle: Option<String>,
    /// After a start, how long to let the sources catch up before closing
    /// anything; `late` when unset.
    #[serde(default)]
    pub grace: Option<String>,
    /// How often to look for windows that may close.
    #[serde(default = "default_interval_secs")]
    pub interval_secs: u64,
    /// Windows read and not yet done, across partitions.
    #[serde(default = "default_in_flight")]
    pub in_flight: usize,
}

fn default_late() -> String {
    "20m".into()
}
const fn default_interval_secs() -> u64 {
    10
}
const fn default_in_flight() -> usize {
    16
}
