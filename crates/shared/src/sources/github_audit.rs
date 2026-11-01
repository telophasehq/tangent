use secrecy::SecretString;
use serde::{Deserialize, Serialize};

use crate::sources::common::{bare_string, bare_string_opt};

/// A GitHub organization's audit log, polled from the REST API
/// (`GET /orgs/{org}/audit-log`; GitHub Enterprise Cloud). The token needs
/// `read:audit_log`. Records are emitted as GitHub returns them, plus
/// `source: github_audit` and `ts` (from `@timestamp`).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GithubAuditConfig {
    /// The organization's login, e.g. `acme`. Empty, the source stays idle
    /// and says so.
    #[serde(default, deserialize_with = "bare_string_opt")]
    pub org: Option<String>,
    /// A token with `read:audit_log`, e.g. `${GITHUB_TOKEN}`.
    #[serde(default, skip_serializing)]
    pub token: Option<SecretString>,
    #[serde(default = "default_interval_secs")]
    pub interval_secs: u64,
    /// On first start, how far back to read; afterwards from the cursor.
    #[serde(default = "default_since", deserialize_with = "bare_string")]
    pub since: String,
    /// `web` (default on the API), `git`, or `all`.
    #[serde(default = "default_include")]
    pub include: String,
    /// The API root; GitHub Enterprise Server has its own.
    #[serde(default = "default_api")]
    pub api: String,
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
