use serde::{Deserialize, Serialize};

use crate::sources::file::FileConfig;
use crate::sources::github_webhook::GithubWebhookConfig;
use crate::sources::msk::MSKConfig;
use crate::sources::socket::SocketConfig;
use crate::sources::sqs::SQSConfig;
use crate::sources::tcp::TcpConfig;

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "type")]
pub enum SourceConfig {
    #[serde(rename = "msk")]
    MSK(MSKConfig),
    #[serde(rename = "file")]
    File(FileConfig),
    #[serde(rename = "socket")]
    Socket(SocketConfig),
    #[serde(rename = "tcp")]
    Tcp(TcpConfig),
    #[serde(rename = "sqs")]
    SQS(SQSConfig),
    #[serde(rename = "github_webhook")]
    GithubWebhook(GithubWebhookConfig),
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Decoding {
    pub format: DecodeFormat, // ndjson | json | json-array | text | msgpack

    #[serde(default)]
    pub compression: DecodeCompression, // auto | none | gzip | zstd
}

impl Decoding {
    #[must_use]
    pub fn resolve_compression(
        &self,
        meta: Option<&str>,
        filename: Option<&str>,
        sniff: &[u8],
    ) -> DecodeCompression {
        match &self.compression {
            DecodeCompression::Auto => {}
            other => return other.clone(),
        }

        if let Some(enc) = meta.map(str::to_ascii_lowercase) {
            if enc.contains("gzip") {
                return DecodeCompression::Gzip;
            }
            if enc.contains("zstd") || enc.contains("zst") {
                return DecodeCompression::Zstd;
            }
            if enc.contains("identity") || enc.contains("none") {
                return DecodeCompression::None;
            }
        }

        if let Some(name) = filename {
            let n = name.to_ascii_lowercase();
            if n.ends_with(".gz") || n.ends_with(".gzip") {
                return DecodeCompression::Gzip;
            }
            if n.ends_with(".zst") || n.ends_with(".zstd") {
                return DecodeCompression::Zstd;
            }
        }

        if is_gzip(sniff) {
            return DecodeCompression::Gzip;
        }
        if is_zstd(sniff) {
            return DecodeCompression::Zstd;
        }

        DecodeCompression::None
    }
}

fn is_gzip(b: &[u8]) -> bool {
    b.len() >= 2 && b[0] == 0x1f && b[1] == 0x8b
}

fn is_zstd(b: &[u8]) -> bool {
    b.len() >= 4 && b[0] == 0x28 && b[1] == 0xB5 && b[2] == 0x2F && b[3] == 0xFD
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum DecodeFormat {
    Ndjson,
    Json,
    JsonArray,
    Msgpack,
    Text,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum DecodeCompression {
    Auto,
    None,
    Gzip,
    Zstd,
}
impl Default for DecodeCompression {
    fn default() -> Self {
        Self::Auto
    }
}
