//! Enrichment tables: reference data under `enrichment_tables` in the
//! config, read from disk once per process when the DAG is built and shared
//! by every worker and every plugin. A plugin asks `lookup(table, key)` and
//! gets JSON back; nothing is read per call.
//!
//! The runtime knows three shapes of file and nothing about what is in them:
//! - `mmdb`, a MaxMind DB: the key is an address, the answer its record;
//! - `cidr`, `prefix,label` lines: the key is an address, the answer the
//!   labels of every prefix that holds it;
//! - `document`, YAML or JSON: the key is a dotted path, the answer the value
//!   there.
//!
//! A file that is missing is warned about once, and its table answers
//! nothing, as a plugin reading its own files did, unless the table is
//! `required`. A required table without its file, and a file that is there
//! but cannot be read, are errors naming the table and the file.

mod cidr;
mod document;
#[cfg(test)]
mod tests;

use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use parking_lot::Mutex;
use serde::de::{Deserialize, Deserializer, Error as _};
use tangent_shared::enrichment::{self, TableConfig, TableKind};

pub use cidr::Cidr;
pub use document::Document;

/// Every table a config names, by name.
#[derive(Default)]
pub struct Tables {
    tables: HashMap<Arc<str>, Arc<Table>>,
    /// Names a plugin asked for that no config defines, each warned about
    /// once.
    unknown: Mutex<HashSet<String>>,
}

pub enum Table {
    Mmdb(maxminddb::Reader<Vec<u8>>),
    Cidr(Cidr),
    Document(Document),
    /// The file was not there at load: it answers nothing.
    Missing,
}

impl Tables {
    /// The tables in `cfgs`, each path relative to `dir` unless absolute. A
    /// file two tables name, as the same type, is read once.
    pub fn load(cfgs: &BTreeMap<Arc<str>, TableConfig>, dir: &Path) -> Result<Self> {
        let mut read: HashMap<(TableKind, PathBuf), Arc<Table>> = HashMap::new();
        let mut tables = HashMap::with_capacity(cfgs.len());
        for (name, cfg) in cfgs {
            let path = dir.join(&cfg.path);
            let path = path.canonicalize().unwrap_or(path);
            let table = match read.get(&(cfg.kind, path.clone())) {
                Some(t) if cfg.required && matches!(**t, Table::Missing) => {
                    return Err(enrichment::not_found(name, &path));
                }
                Some(t) => Arc::clone(t),
                None => {
                    let t = Arc::new(Table::load(name, cfg.kind, &path, cfg.required)?);
                    read.insert((cfg.kind, path), Arc::clone(&t));
                    t
                }
            };
            tables.insert(Arc::clone(name), table);
        }
        Ok(Self {
            tables,
            unknown: Mutex::new(HashSet::new()),
        })
    }

    /// What `table` holds for `key`, as JSON; None when nothing, or when
    /// there is no such table.
    pub fn lookup(&self, table: &str, key: &str) -> Option<String> {
        match self.tables.get(table) {
            Some(t) => t.lookup(key),
            None => {
                let mut unknown = self.unknown.lock();
                if !unknown.contains(table) {
                    tracing::warn!(
                        table,
                        "a plugin looked up enrichment table {table}, which no config defines; it answers nothing"
                    );
                    unknown.insert(table.to_string());
                }
                None
            }
        }
    }

    pub fn get(&self, table: &str) -> Option<&Arc<Table>> {
        self.tables.get(table)
    }
}

impl Table {
    /// Table `name`'s file at `path`, read and indexed.
    pub fn load(name: &str, kind: TableKind, path: &Path, required: bool) -> Result<Self> {
        let bytes = match std::fs::read(path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound && required => {
                return Err(enrichment::not_found(name, path));
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::warn!(
                    table = name,
                    path = %path.display(),
                    "enrichment table {name}: {} not found; it answers nothing",
                    path.display()
                );
                return Ok(Table::Missing);
            }
            Err(e) => {
                return Err(e).with_context(|| {
                    format!("enrichment table {name}: reading {}", path.display())
                })
            }
        };
        #[cfg(test)]
        tests::count_load(path);
        let table = match kind {
            TableKind::Mmdb => maxminddb::Reader::from_source(bytes)
                .map(Table::Mmdb)
                .map_err(anyhow::Error::from),
            TableKind::Cidr => Cidr::parse(&bytes).map(Table::Cidr),
            TableKind::Document => Document::parse(&bytes, path).map(Table::Document),
        }
        .with_context(|| {
            format!(
                "enrichment table {name}: {} is not a {} file",
                path.display(),
                kind.name()
            )
        })?;
        let what = match &table {
            Table::Mmdb(r) => r.metadata().database_type.clone(),
            Table::Cidr(c) => format!("{} prefixes", c.prefixes()),
            Table::Document(_) | Table::Missing => kind.name().to_string(),
        };
        tracing::info!(
            table = name,
            path = %path.display(),
            "enrichment table {name}: {what} from {}",
            path.display()
        );
        Ok(table)
    }

    pub fn lookup(&self, key: &str) -> Option<String> {
        match self {
            Table::Mmdb(reader) => {
                let ip = address(key)?;
                let found = reader.lookup(ip).ok()?;
                found.decode::<Json>().ok()?.map(|j| j.0)
            }
            Table::Cidr(c) => c.lookup(address(key)?).map(|labels| labels.to_string()),
            Table::Document(d) => d.lookup(key),
            Table::Missing => None,
        }
    }
}

/// An address as text, IPv4-mapped IPv6 as the IPv4 address it maps.
fn address(key: &str) -> Option<IpAddr> {
    key.trim()
        .parse::<IpAddr>()
        .ok()
        .map(|ip| ip.to_canonical())
}

/// A MaxMind record written straight to JSON text, without a tree between.
struct Json(String);

impl<'de> Deserialize<'de> for Json {
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let mut out = Vec::with_capacity(256);
        serde_transcode::transcode(d, &mut serde_json::Serializer::new(&mut out))
            .map_err(D::Error::custom)?;
        String::from_utf8(out).map(Json).map_err(D::Error::custom)
    }
}
