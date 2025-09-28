use serde::Deserialize;
use std::path::PathBuf;

#[derive(Debug, Deserialize)]
pub struct SocketConfig {
    #[serde(default = "default_socket_path")]
    pub socket_path: PathBuf,
}

fn default_socket_path() -> PathBuf {
    "/tmp/sidecar.sock".into()
}
