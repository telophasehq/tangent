use serde::{Deserialize, Serialize};

use crate::sinks::{blackhole, file, s3};

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
    S3(s3::S3Config),
    #[serde(rename = "file")]
    File(file::FileConfig),
    #[serde(rename = "blackhole")]
    Blackhole(blackhole::BlackholeConfig),
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

    #[serde(default = "default_sink")]
    pub default: bool,
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

pub fn object_max_bytes() -> usize {
    134217728
}

pub fn in_flight_limit() -> usize {
    16
}

fn default_sink() -> bool {
    false
}
