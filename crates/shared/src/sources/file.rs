use serde::{Deserialize, Serialize};
use std::path::PathBuf;

use crate::sources::common::Decoding;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileConfig {
    pub path: PathBuf,

    pub decoding: Decoding,
}
