use std::fmt;
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use crate::redact::Redacted;

#[derive(Clone, Serialize, Deserialize)]
pub struct GithubWebhookConfig {
    #[serde(default = "default_bind_address")]
    pub bind_address: SocketAddr,
    pub path: String,
    pub secret: Option<String>,
    pub token: String,
}

/// By hand: the startup log shows the secret and the token as `<redacted>`.
impl fmt::Debug for GithubWebhookConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GithubWebhookConfig")
            .field("bind_address", &self.bind_address)
            .field("path", &self.path)
            .field("secret", &Redacted(&self.secret))
            .field("token", &Redacted(&self.token))
            .finish()
    }
}

fn default_bind_address() -> SocketAddr {
    "0.0.0.0:9000"
        .parse()
        .expect("default TCP bind address should be valid")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_hides_the_secret_and_token() {
        let c: GithubWebhookConfig =
            serde_yaml::from_str("{path: /hook, secret: HOOK_SECRET, token: ghp_TOKEN}").unwrap();
        crate::redact::assert_hides(&c, &["HOOK_SECRET", "ghp_TOKEN"]);
    }
}
