use serde::{Deserialize, Serialize};
use std::net::SocketAddr;

#[derive(Debug, Deserialize, Serialize)]
pub struct TcpConfig {
    #[serde(default = "default_bind_address")]
    pub bind_address: SocketAddr,

    #[serde(default = "default_read_buffer_size")]
    pub read_buffer_size: usize,
}

fn default_bind_address() -> SocketAddr {
    "0.0.0.0:9000"
        .parse()
        .expect("default TCP bind address should be valid")
}

const fn default_read_buffer_size() -> usize {
    512 * 1024
}
