use serde::{Deserialize, Serialize};

/// Google Workspace's audit activity, polled from the Admin SDK Reports API
/// as a service account with domain-wide delegation acting for an admin.
/// Records are emitted as the API returns them, plus `source:
/// google_workspace` and `ts` (from `id.time`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoogleWorkspaceConfig {
    /// The service account's JSON key file, e.g.
    /// `${GOOGLE_APPLICATION_CREDENTIALS}`. Empty, the source stays idle.
    #[serde(default)]
    pub credentials_file: Option<String>,
    /// The admin the service account acts as (domain-wide delegation).
    #[serde(default)]
    pub subject: Option<String>,
    /// Which applications' activity to read: `login`, `token`, `drive`,
    /// `admin`, `saml`, `groups`, ...
    #[serde(default = "default_applications")]
    pub applications: Vec<String>,
    #[serde(default = "default_interval_secs")]
    pub interval_secs: u64,
    /// On first start, how far back to read; afterwards from the cursor.
    #[serde(default = "default_since")]
    pub since: String,
    /// The Reports API root.
    #[serde(default = "default_api")]
    pub api: String,
}

const fn default_interval_secs() -> u64 {
    60
}
fn default_since() -> String {
    "1h".into()
}
fn default_api() -> String {
    "https://admin.googleapis.com".into()
}
fn default_applications() -> Vec<String> {
    ["login", "token", "drive", "admin"]
        .iter()
        .map(|s| s.to_string())
        .collect()
}
