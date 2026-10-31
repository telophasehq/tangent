use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// A file of reference data the runtime loads once per process and every
/// plugin reads through the `enrichment` interface, by the table's name.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableConfig {
    #[serde(rename = "type")]
    pub kind: TableKind,

    /// The file, relative to the config file.
    pub path: PathBuf,

    /// Without its file, nothing starts: `tangent run` and `tangent plugin
    /// test` stop, naming the table and the file. A table that is not
    /// required and has no file answers nothing.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub required: bool,
}

/// A required table's file is not there.
pub fn not_found(name: &str, path: &Path) -> anyhow::Error {
    anyhow::anyhow!(
        "enrichment table {name}: {} not found, and the table is required",
        path.display()
    )
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum TableKind {
    /// A MaxMind DB. The key is an address; the answer, its record as JSON.
    Mmdb,
    /// `prefix,label` lines. The key is an address; the answer, the labels
    /// of every prefix that holds it, as a JSON array.
    Cidr,
    /// A YAML or JSON file. The key is a dotted path, "" for all of it; the
    /// answer, the value there as JSON.
    Document,
}

impl TableKind {
    pub fn name(self) -> &'static str {
        match self {
            TableKind::Mmdb => "mmdb",
            TableKind::Cidr => "cidr",
            TableKind::Document => "document",
        }
    }
}
