use std::path::PathBuf;

use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct S3Config {
    pub bucket_name: String,
    pub region: Option<String>,

    /// What each object's key starts with, before `/` and its name: a
    /// strftime template rendered in UTC for each record's own time, read
    /// from `timestamp_field`, so `logs/cloudtrail/%Y/%m/%d` puts each day's
    /// records under a prefix of their own, whenever they arrive (`%%` is a
    /// `%`). A record without a time there goes under the time it reaches
    /// the sink. Without it, objects go to the bucket's root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key_prefix: Option<String>,

    /// The field holding each record's own time, an RFC 3339 string, for
    /// `key_prefix` to render, found as a node's `group_by` finds one:
    /// `a.b` is a key holding all of it, or else field `b` of the object at
    /// `a`, and `a[0].b` is field `b` of the first item in the list at `a`.
    /// A record whose field is missing or not RFC 3339 (an epoch number,
    /// which a sqlite sink's `timestamp_key` takes, is not) goes under the
    /// time it reaches the sink, counted in
    /// `tangent_sink_records_untimed_total`. A `key_prefix` without a
    /// specifier reads no time.
    #[serde(default = "crate::sinks::common::timestamp_field")]
    pub timestamp_field: String,

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
