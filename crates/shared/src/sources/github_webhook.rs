use std::net::SocketAddr;

use secrecy::SecretString;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GithubWebhookConfig {
    #[serde(default = "default_bind_address")]
    pub bind_address: SocketAddr,
    pub path: String,
    #[serde(skip_serializing)]
    pub secret: Option<SecretString>,
    #[serde(skip_serializing)]
    pub token: SecretString,
}

fn default_bind_address() -> SocketAddr {
    "0.0.0.0:9000"
        .parse()
        .expect("default TCP bind address should be valid")
}
