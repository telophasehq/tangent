use serde::{Deserialize, Serialize};

use crate::sinks::{blackhole, file, rolling_file, s3, slack, sqlite};

#[derive(Debug, Deserialize, Serialize)]
pub struct SinkConfig {
    #[serde(flatten)]
    pub common: CommonSinkOptions,

    #[serde(flatten)]
    pub kind: SinkKind,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum SinkKind {
    #[serde(rename = "s3")]
    S3(s3::S3Config),
    #[serde(rename = "file")]
    File(file::FileConfig),
    #[serde(rename = "rolling_file")]
    RollingFile(rolling_file::RollingFileConfig),
    #[serde(rename = "blackhole")]
    Blackhole(blackhole::BlackholeConfig),
    #[serde(rename = "sqlite")]
    Sqlite(sqlite::SqliteConfig),
    #[serde(rename = "slack")]
    Slack(slack::SlackConfig),
}

#[derive(Debug, Deserialize, Serialize)]
pub struct CommonSinkOptions {
    #[serde(default)]
    pub compression: Compression,

    #[serde(default)]
    pub encoding: Encoding,

    #[serde(default = "object_max_bytes")]
    pub object_max_bytes: usize,

    #[serde(default = "in_flight_limit")]
    pub in_flight_limit: usize,

    #[serde(default = "default_sink")]
    pub default: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Encoding {
    #[default]
    NDJSON,
    JSON,
    Avro {
        schema: String,
    },
    Parquet {
        schema: Columns,
        /// Each record, in a Utf8 column of this name, beside the columns
        /// `schema` reads out of it: a query can still reach a field the
        /// schema leaves out. The record is its line without the line break
        /// (a trailing CR or LF goes), invalid UTF-8 replaced. A record the
        /// schema cannot read is kept here with its typed columns null.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        record_column: Option<String>,
    },
}

/// A Parquet file's columns, in order: each a name and its type, a nested
/// map a struct of columns of its own. Every column may be null.
///
/// ```yaml
/// schema:
///   ts: timestamp
///   source: string
///   _norm: { org: string, actor: string }
/// ```
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Columns(pub Vec<(String, Column)>);

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
pub enum Column {
    Type(ColumnType),
    Struct(Columns),
}

/// `int` is 64 bits and `float` a double; a `timestamp` is milliseconds in
/// UTC, read from RFC 3339 text in any zone.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ColumnType {
    String,
    Int,
    Float,
    Bool,
    Timestamp,
}

const COLUMN_TYPES: &[&str] = &["string", "int", "float", "bool", "timestamp"];

/// A column, or with `top` the schema's map of them.
struct ColumnVisitor {
    top: bool,
}

impl<'de> serde::de::Visitor<'de> for ColumnVisitor {
    type Value = Column;
    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str(if self.top {
            "a map of column names to their types"
        } else {
            "a column type (string, int, float, bool, timestamp) or a map of columns"
        })
    }
    fn visit_str<E: serde::de::Error>(self, v: &str) -> Result<Column, E> {
        let ty = match v {
            "string" => ColumnType::String,
            "int" => ColumnType::Int,
            "float" => ColumnType::Float,
            "bool" => ColumnType::Bool,
            "timestamp" => ColumnType::Timestamp,
            _ => return Err(E::unknown_variant(v, COLUMN_TYPES)),
        };
        Ok(Column::Type(ty))
    }
    fn visit_map<A: serde::de::MapAccess<'de>>(self, mut m: A) -> Result<Column, A::Error> {
        let mut cols = Vec::new();
        while let Some((name, col)) = m.next_entry::<String, Column>()? {
            if cols.iter().any(|(n, _)| *n == name) {
                return Err(serde::de::Error::custom(format!("column {name:?} twice")));
            }
            cols.push((name, col));
        }
        Ok(Column::Struct(Columns(cols)))
    }
}

impl<'de> Deserialize<'de> for Column {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        d.deserialize_any(ColumnVisitor { top: false })
    }
}

impl<'de> Deserialize<'de> for Columns {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        match d.deserialize_map(ColumnVisitor { top: true })? {
            Column::Struct(cols) => Ok(cols),
            Column::Type(_) => unreachable!("deserialize_map gives a map"),
        }
    }
}

impl Serialize for Columns {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut m = s.serialize_map(Some(self.0.len()))?;
        for (name, col) in &self.0 {
            m.serialize_entry(name, col)?;
        }
        m.end()
    }
}

impl Encoding {
    pub const fn content_type(&self) -> &'static str {
        match self {
            Self::NDJSON => "application/x-ndjson",
            Self::JSON => "application/json",
            Self::Avro { .. } => "application/avro",
            Self::Parquet { .. } => "application/vnd.apache.parquet",
        }
    }

    pub const fn extension(&self) -> &'static str {
        match self {
            Self::NDJSON => "ndjson",
            Self::JSON => "json",
            Self::Avro { .. } => "avro",
            Self::Parquet { .. } => "parquet",
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Compression {
    None,
    Gzip {
        #[serde(default = "default_gzip_level")]
        level: u32,
    },
    Zstd {
        #[serde(default = "default_zstd_level")]
        level: i32,
    },
    Snappy {
        #[serde(default = "default_zstd_level")]
        level: i32,
    },
    Deflate {
        #[serde(default = "default_zstd_level")]
        level: i32,
    },
}

impl Compression {
    pub const fn extension(&self) -> &'static str {
        match self {
            Self::None => "",
            Self::Gzip { .. } => ".gz",
            Self::Zstd { .. } => ".zst",
            Self::Snappy { .. } => "",
            Self::Deflate { .. } => "",
        }
    }
}

impl Default for Compression {
    fn default() -> Self {
        Self::Zstd {
            level: default_zstd_level(),
        }
    }
}

const fn default_gzip_level() -> u32 {
    6
}
const fn default_zstd_level() -> i32 {
    3
}

pub const fn object_max_bytes() -> usize {
    134217728
}

pub const fn in_flight_limit() -> usize {
    16
}

/// A WAL sink's `timestamp_field` unless set: the field every pack
/// normalizes a record's time to.
pub fn timestamp_field() -> String {
    "ts".into()
}

const fn default_sink() -> bool {
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_parquet_schema_is_columns_in_order() {
        let enc: Encoding = serde_yaml::from_str(
            "{ type: parquet, schema: { ts: timestamp, _norm: { org: string, n: int }, ok: bool, x: float } }",
        )
        .unwrap();
        let Encoding::Parquet { schema, .. } = enc else {
            panic!("{enc:?}")
        };
        let names: Vec<&str> = schema.0.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, ["ts", "_norm", "ok", "x"]);
        let Column::Struct(norm) = &schema.0[1].1 else {
            panic!("{schema:?}")
        };
        assert_eq!(
            norm.0,
            [
                ("org".into(), Column::Type(ColumnType::String)),
                ("n".into(), Column::Type(ColumnType::Int))
            ]
        );
        let back: Columns = serde_yaml::from_str(&serde_yaml::to_string(&schema).unwrap()).unwrap();
        assert_eq!(back, schema);
    }

    #[test]
    fn a_parquet_schema_names_known_types_once() {
        let parse = |s: &str| serde_yaml::from_str::<Columns>(s);
        let e = parse("{ _norm: { n: integer } }").unwrap_err().to_string();
        assert!(
            e.contains("unknown variant `integer`") && e.contains("timestamp"),
            "{e}"
        );
        assert!(parse("{ n: int, n: string }").is_err());
        assert!(parse("'{\"fields\":[]}'").is_err());
    }
}
