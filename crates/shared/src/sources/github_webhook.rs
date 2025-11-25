use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GithubWebhookConfig {
    #[serde(default = "default_bind_address")]
    pub bind_address: SocketAddr,
    pub path: String,
    pub secret: Option<String>,
    pub token: String,
}

fn default_bind_address() -> SocketAddr {
    "0.0.0.0:9000"
        .parse()
        .expect("default TCP bind address should be valid")
}
