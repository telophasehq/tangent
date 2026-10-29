use std::fmt;

use serde::{Deserialize, Serialize};

use crate::redact::Redacted;

/// A GitHub organization's audit log, polled from the REST API
/// (`GET /orgs/{org}/audit-log`; GitHub Enterprise Cloud). The token needs
/// `read:audit_log`. Records are emitted as GitHub returns them, plus
/// `source: github_audit` and `ts` (from `@timestamp`).
#[derive(Clone, Serialize, Deserialize)]
pub struct GithubAuditConfig {
    /// The organization's login, e.g. `acme`. Empty, the source stays idle
    /// and says so.
    #[serde(default)]
    pub org: Option<String>,
    /// A token with `read:audit_log`, e.g. `${GITHUB_TOKEN}`.
    #[serde(default)]
    pub token: Option<String>,
    #[serde(default = "default_interval_secs")]
    pub interval_secs: u64,
    /// On first start, how far back to read; afterwards from the cursor.
    #[serde(default = "default_since")]
    pub since: String,
    /// `web` (default on the API), `git`, or `all`.
    #[serde(default = "default_include")]
    pub include: String,
    /// The API root; GitHub Enterprise Server has its own.
    #[serde(default = "default_api")]
    pub api: String,
}

/// By hand: the startup log shows the token as `<redacted>`.
impl fmt::Debug for GithubAuditConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GithubAuditConfig")
            .field("org", &self.org)
            .field("token", &Redacted(&self.token))
            .field("interval_secs", &self.interval_secs)
            .field("since", &self.since)
            .field("include", &self.include)
            .field("api", &self.api)
            .finish()
    }
}

const fn default_interval_secs() -> u64 {
    60
}
fn default_since() -> String {
    "1h".into()
}
fn default_include() -> String {
    "all".into()
}
fn default_api() -> String {
    "https://api.github.com".into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_hides_the_token() {
        let c: GithubAuditConfig = serde_yaml::from_str("{org: acme, token: ghp_TOKEN}").unwrap();
        crate::redact::assert_hides(&c, &["ghp_TOKEN"]);
    }
}
