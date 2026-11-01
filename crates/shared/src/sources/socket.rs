use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SocketConfig {
    #[serde(default = "default_socket_path")]
    pub socket_path: PathBuf,
}

fn default_socket_path() -> PathBuf {
    PathBuf::from("/tmp/sidecar.sock")
}
