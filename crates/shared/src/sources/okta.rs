use std::fmt;

use serde::{Deserialize, Serialize};

use crate::redact::Redacted;

/// Okta's System Log, polled (`GET /api/v1/logs?since=…`, paged by the
/// `Link` header). Records are emitted as Okta returns them, plus
/// `source: okta` and `ts` (from `published`).
#[derive(Clone, Serialize, Deserialize)]
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

/// By hand: the startup log shows the token as `<redacted>`.
impl fmt::Debug for OktaConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OktaConfig")
            .field("org_url", &self.org_url)
            .field("token", &Redacted(&self.token))
            .field("interval_secs", &self.interval_secs)
            .field("since", &self.since)
            .finish()
    }
}

const fn default_interval_secs() -> u64 {
    30
}
fn default_since() -> String {
    "1h".into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_hides_the_token() {
        let c: OktaConfig =
            serde_yaml::from_str("{org_url: https://acme.okta.com, token: SSWS_TOKEN}").unwrap();
        crate::redact::assert_hides(&c, &["SSWS_TOKEN"]);
    }
}
