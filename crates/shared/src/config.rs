use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use std::time::Duration;

use crate::msk::MSKConfig;
use crate::s3::S3Config;
use crate::socket::SocketConfig;
use crate::sqs::SQSConfig;

pub mod msk;
pub mod s3;
pub mod socket;
pub mod sqs;

#[derive(Debug, Deserialize)]
pub struct Config {
    pub entry_point: String,
    pub module_type: String,

    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    #[serde(default = "default_batch_age")]
    pub batch_age: u64,

    #[serde(default = "default_workers")]
    pub workers: usize,

    #[serde(default)]
    pub sources: std::collections::BTreeMap<String, SourceConfig>,

    #[serde(default)]
    pub sinks: std::collections::BTreeMap<String, SinkConfig>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum SourceConfig {
    #[serde(rename = "msk")]
    MSK(MSKConfig),
    #[serde(rename = "socket")]
    Socket(SocketConfig),
    #[serde(rename = "sqs")]
    SQS(SQSConfig),
}

#[derive(Debug, Deserialize)]
pub struct CommonSinkOptions {
    #[serde(default)]
    pub compression: Compression,

    #[serde(default)]
    pub encoding: Encoding,

    #[serde(default = "object_max_bytes")]
    pub object_max_bytes: usize,

    #[serde(default = "in_flight_limit")]
    pub in_flight_limit: usize,

    #[serde(default = "wal_path")]
    pub wal_path: PathBuf,

    #[serde(default = "max_file_age_seconds")]
    pub max_file_age_seconds: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Encoding {
    NDJSON,
    JSON,
    Avro,
    Parquet,
}

impl Default for Encoding {
    fn default() -> Self {
        Encoding::NDJSON
    }
}

impl Encoding {
    pub fn content_type(&self) -> &'static str {
        match self {
            Encoding::NDJSON => "application/x-ndjson",
            Encoding::JSON => "application/json",
            Encoding::Avro => "application/avro",
            Encoding::Parquet => "application/vnd.apache.parquet",
        }
    }

    pub fn extension(&self) -> &'static str {
        match self {
            Encoding::NDJSON => "ndjson",
            Encoding::JSON => "json",
            Encoding::Avro => "avro",
            Encoding::Parquet => "parquet",
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
}

impl Default for Compression {
    fn default() -> Self {
        Compression::Zstd {
            level: default_zstd_level(),
        }
    }
}

fn default_gzip_level() -> u32 {
    6
}
fn default_zstd_level() -> i32 {
    3
}

fn object_max_bytes() -> usize {
    134217728
}

fn in_flight_limit() -> usize {
    16
}

fn wal_path() -> PathBuf {
    "/tmp/wal".into()
}

fn max_file_age_seconds() -> u64 {
    60
}

#[derive(Debug, Deserialize)]
pub struct SinkConfig {
    #[serde(flatten)]
    pub common: CommonSinkOptions,

    #[serde(flatten)]
    pub kind: SinkKind,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum SinkKind {
    #[serde(rename = "s3")]
    S3(S3Config),
}

fn default_batch_size() -> usize {
    256
}
fn default_batch_age() -> u64 {
    5
}
fn default_workers() -> usize {
    num_cpus::get()
}

impl Config {
    pub fn from_file(path: &PathBuf) -> Result<Self> {
        let bytes = fs::read(path).with_context(|| format!("reading {}", path.display()))?;
        let cfg: Config = serde_yaml::from_slice(&bytes)
            .with_context(|| format!("parsing YAML {}", path.display()))?;

        Ok(cfg)
    }

    pub fn batch_age_ms(&self) -> Duration {
        Duration::from_millis(self.batch_age)
    }

    pub fn batch_size_kb(&self) -> usize {
        self.batch_size << 10
    }
}
