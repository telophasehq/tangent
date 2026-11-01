use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::sources::common::Decoding;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileConfig {
    pub path: PathBuf,

    pub decoding: Decoding,
}
