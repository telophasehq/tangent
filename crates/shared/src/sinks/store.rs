use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Keep every record in the state store, under the partition its own fields
/// give it (`runtime.partition.key`), as `ev:<epoch ms, 13 digits>:<ulid>`
/// in `namespace`, for `retain`. A partition's history is then a key range:
/// what a `store` source reads windows from and an inference node reads
/// context from. The sink also keeps, per partition, what the `store`
/// source needs to know when a window may close (namespace `windows`), and
/// an index of the records by scope (namespace `recent:<namespace>`), which
/// an inference node's history is read from.
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
    /// The scopes history is read by: each record is indexed by the value of
    /// each scope's field (`{actor: _norm.actor, org: _norm.org}`), kept as
    /// long as the record, so an inference node reads any scope's latest
    /// records before its batch, however long ago, newest first. A scope
    /// whose field is the partition key is not indexed: the partition is its
    /// history.
    #[serde(default)]
    pub recent: BTreeMap<String, String>,
    /// What is new, per scope of `recent`: the fields whose values each
    /// scope value is remembered to have had (`{actor: [_norm.kind,
    /// _facts.ip.org], org: [_norm.actor]}`). A record with a value its
    /// scope has not had within `retain` before the record's own time is
    /// stored with `_novel`, which scope and fields:
    /// `{"actor": ["_norm.kind"]}`. A record that has none is what its
    /// scopes do every day. A record with no value for any of these fields
    /// cannot be told either way and is marked `{"_": ["unscoped"]}`, so a
    /// source the config does not cover is still judged.
    #[serde(default)]
    pub novel: BTreeMap<String, Vec<String>>,
    /// How long, in record time, a scope value's records after a new one
    /// are marked too (`1h`): `{"actor": ["follows"]}`, at most 50 a
    /// window, so the session that something new begins is judged whole,
    /// not only its first record. Not for a scope that is the partition,
    /// whose every record would follow. Off without it.
    #[serde(default)]
    pub follow: Option<String>,
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

