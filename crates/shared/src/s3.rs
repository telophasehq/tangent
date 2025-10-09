use serde::Deserialize;

use crate::source::Decoding;

#[derive(Debug, Deserialize)]
pub struct S3Config {
    pub bucket_name: String,
    pub region: Option<String>,
    #[serde(default)]
    pub decoding: Decoding,
}
