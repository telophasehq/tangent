use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Deserialize, Serialize)]
pub struct SocketConfig {
    #[serde(default = "default_socket_path")]
    pub socket_path: PathBuf,
}

fn default_socket_path() -> PathBuf {
    PathBuf::from("/tmp/sidecar.sock")
}
