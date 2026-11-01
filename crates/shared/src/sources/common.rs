use serde::{de, Deserialize, Deserializer, Serialize};
use std::fmt;

use crate::sources::cloudtrail::CloudtrailConfig;
use crate::sources::file::FileConfig;
use crate::sources::github_audit::GithubAuditConfig;
use crate::sources::github_webhook::GithubWebhookConfig;
use crate::sources::google_workspace::GoogleWorkspaceConfig;
use crate::sources::msk::MSKConfig;
use crate::sources::npm_registry::NpmRegistryConfig;
use crate::sources::okta::OktaConfig;
use crate::sources::socket::SocketConfig;
use crate::sources::sqlite::SqliteConfig;
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
    #[serde(rename = "npm_registry")]
    NPMRegistry(NpmRegistryConfig),
    /// The audit logs a company reads about itself, polled from their APIs
    /// with nothing in between.
    #[serde(rename = "cloudtrail")]
    Cloudtrail(CloudtrailConfig),
    #[serde(rename = "github_audit")]
    GithubAudit(GithubAuditConfig),
    #[serde(rename = "okta")]
    Okta(OktaConfig),
    #[serde(rename = "google_workspace")]
    GoogleWorkspace(GoogleWorkspaceConfig),
    #[serde(rename = "sqlite")]
    Sqlite(SqliteConfig),
}

/// A string field that may be written bare. Inside a `type`-tagged block
/// serde reads `since: 3600` as a number before the field sees it, so a
/// number or a bool is taken as the text it was written as.
pub fn bare_string<'de, D: Deserializer<'de>>(d: D) -> Result<String, D::Error> {
    d.deserialize_any(Bare)
}

/// [`bare_string`] for an optional field; empty is none.
pub fn bare_string_opt<'de, D: Deserializer<'de>>(d: D) -> Result<Option<String>, D::Error> {
    d.deserialize_option(BareOpt)
}

struct Bare;

impl<'de> de::Visitor<'de> for Bare {
    type Value = String;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a string, number or bool")
    }
    fn visit_str<E: de::Error>(self, v: &str) -> Result<String, E> {
        Ok(v.to_string())
    }
    fn visit_string<E: de::Error>(self, v: String) -> Result<String, E> {
        Ok(v)
    }
    fn visit_u64<E: de::Error>(self, v: u64) -> Result<String, E> {
        Ok(v.to_string())
    }
    fn visit_i64<E: de::Error>(self, v: i64) -> Result<String, E> {
        Ok(v.to_string())
    }
    fn visit_f64<E: de::Error>(self, v: f64) -> Result<String, E> {
        Ok(v.to_string())
    }
    fn visit_bool<E: de::Error>(self, v: bool) -> Result<String, E> {
        Ok(v.to_string())
    }
}

struct BareOpt;

impl<'de> de::Visitor<'de> for BareOpt {
    type Value = Option<String>;

    fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.write_str("a string, number or bool, or nothing")
    }
    fn visit_none<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(None)
    }
    fn visit_unit<E: de::Error>(self) -> Result<Self::Value, E> {
        Ok(None)
    }
    fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<Self::Value, D::Error> {
        bare_string(d).map(Some)
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
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
