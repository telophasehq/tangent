use serde::{Deserialize, Serialize};

/// Stored records, read back from the state store a buffer at a time: per
/// partition, consecutive spans of the records' own time, `buffer` long.
/// A buffer closes when the partition's watermark (the latest record time
/// stored for it) passes its end plus `late`; a partition that has had no
/// new record for `idle` has its watermark moved to the clock less `late`,
/// so its last buffer closes too. A closed buffer's records are emitted as
/// one batch, in time order, each with `_buffer: {from, to}`; the buffer
/// counts as done when they have reached their sinks. Records stored after
/// their buffer was read are emitted again later, as a small batch marked
/// `_late`. How much history a batch is read with is the judge's `window`,
/// not this.
///
/// Everything this needs to resume (each partition's watermark, how far it
/// was judged, the late markers) is in the store, so a restart picks up
/// where it stopped and catches up what fell due meanwhile.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StoreSourceConfig {
    /// Where the `store` sink keeps records.
    #[serde(default = "crate::sinks::store::default_namespace")]
    pub namespace: String,
    /// How long records gather before they are judged, in their own time:
    /// `5m`, `1h`. With `late`, how long a record waits for its verdict.
    #[serde(alias = "span")]
    pub buffer: String,
    /// How late a record may arrive and still be in its buffer.
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
    /// Judge nothing before this time (RFC 3339), on a partition's first
    /// sight: records before it are still stored and are history for what
    /// comes after. A first start after a long backfill, or an evaluation
    /// of one stretch, sets it.
    #[serde(default)]
    pub since: Option<String>,
    /// Judge only the buffers that overlap one of these spans of time
    /// (`{from, to}`, RFC 3339); the rest are passed over as if empty.
    /// Judging one incident again after a change to the brief, or an
    /// evaluation that needs a sample, sets it.
    #[serde(default)]
    pub ranges: Vec<TimeRange>,
    /// Buffers read and not yet done, across partitions.
    #[serde(default = "default_in_flight")]
    pub in_flight: usize,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TimeRange {
    pub from: String,
    pub to: String,
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
