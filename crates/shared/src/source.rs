use serde::{Deserialize, Serialize};

use crate::msk::MSKConfig;
use crate::socket::SocketConfig;
use crate::sqs::SQSConfig;

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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Decoding {
    #[serde(default)]
    pub format: DecodeFormat, // auto | ndjson | json | json-array | text | msgpack

    #[serde(default)]
    pub compression: DecodeCompression, // auto | none | gzip | zstd
}

impl Decoding {
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

        if let Some(enc) = meta.map(|m| m.to_ascii_lowercase()) {
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
    pub fn resolve_format(&self, bytes: &[u8]) -> DecodeFormat {
        if !matches!(self.format, DecodeFormat::Auto) {
            return self.format.clone();
        }

        let i = skip_ws(bytes);
        if i >= bytes.len() {
            return DecodeFormat::Text;
        }
        let b0 = bytes[i];

        match b0 {
            b'{' => return DecodeFormat::Json,
            b'[' => return DecodeFormat::JsonArray,
            _ => {}
        }

        if matches!(b0, b'"' | b'-' | b'0'..=b'9' | b't' | b'f' | b'n') {
            return DecodeFormat::Ndjson;
        }

        if likely_msgpack_prefix(b0) {
            return DecodeFormat::Msgpack;
        }

        DecodeFormat::Text
    }
}

fn skip_ws(b: &[u8]) -> usize {
    let mut i = 0;
    while i < b.len() && matches!(b[i], b' ' | b'\t' | b'\r' | b'\n') {
        i += 1;
    }
    i
}

fn is_gzip(b: &[u8]) -> bool {
    b.len() >= 2 && b[0] == 0x1f && b[1] == 0x8b
}

fn is_zstd(b: &[u8]) -> bool {
    b.len() >= 4 && b[0] == 0x28 && b[1] == 0xB5 && b[2] == 0x2F && b[3] == 0xFD
}

fn likely_msgpack_prefix(b0: u8) -> bool {
    matches!(
        b0,
        0xc4 | 0xc5 | 0xc6 | 0xd9 | 0xda | 0xdb | 0xdc | 0xdd | 0xde | 0xdf
    ) || (0xa0..=0xbf).contains(&b0)
        || (0x90..=0x9f).contains(&b0)
        || (0x80..=0x8f).contains(&b0)
}

impl Default for Decoding {
    fn default() -> Self {
        Self {
            format: DecodeFormat::Auto,
            compression: DecodeCompression::Auto,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum DecodeFormat {
    Auto,
    Ndjson,
    Json,
    JsonArray,
    Msgpack,
    Text,
}
impl Default for DecodeFormat {
    fn default() -> Self {
        Self::Auto
    }
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
