use serde::{Deserialize, Serialize};

/// Okta's System Log, polled (`GET /api/v1/logs?since=…`, paged by the
/// `Link` header). Records are emitted as Okta returns them, plus
/// `source: okta` and `ts` (from `published`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OktaConfig {
    /// The org URL, e.g. `https://acme.okta.com`. Empty, the source stays
    /// idle and says so.
    #[serde(default)]
    pub org_url: Option<String>,
    /// An API token (`SSWS`), e.g. `${OKTA_API_TOKEN}`.
    #[serde(default)]
    pub token: Option<String>,
    #[serde(default = "default_interval_secs")]
    pub interval_secs: u64,
    /// On first start, how far back to read; afterwards from the cursor.
    #[serde(default = "default_since")]
    pub since: String,
}

const fn default_interval_secs() -> u64 {
    30
}
fn default_since() -> String {
    "1h".into()
}
