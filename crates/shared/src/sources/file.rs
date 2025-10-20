use serde::Deserialize;
use std::path::PathBuf;

use crate::sources::common::Decoding;

#[derive(Debug, Clone, Deserialize)]
pub struct FileConfig {
    pub path: PathBuf,

    #[serde(default)]
    pub decoding: Decoding,
}
