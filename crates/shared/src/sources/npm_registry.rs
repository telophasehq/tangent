use std::fmt;

use serde::{Deserialize, Serialize};

use crate::redact::Redacted;

#[derive(Clone, Serialize, Deserialize)]
pub struct NpmRegistryConfig {
    /// List of npm package names to poll, e.g. ["tangent-home-js", "@telophasehq/foo"].
    /// Leave empty to check all packages registered to your org.  Must specify this or orgs.
    pub packages: Option<Vec<String>>,

    /// List of orgs to poll for packages, e.g. ["telophasehq", "ethanblackburn"]. Must specify this or packages.
    pub orgs: Option<Vec<String>>,

    /// Poll interval in seconds
    #[serde(default = "default_interval_secs")]
    pub interval_secs: u64,

    pub token: Option<String>,
}

/// By hand: the startup log shows the token as `<redacted>`.
impl fmt::Debug for NpmRegistryConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NpmRegistryConfig")
            .field("packages", &self.packages)
            .field("orgs", &self.orgs)
            .field("interval_secs", &self.interval_secs)
            .field("token", &Redacted(&self.token))
            .finish()
    }
}

fn default_interval_secs() -> u64 {
    5
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_hides_the_token() {
        let c: NpmRegistryConfig =
            serde_yaml::from_str("{orgs: [acme], token: npm_TOKEN}").unwrap();
        crate::redact::assert_hides(&c, &["npm_TOKEN"]);
    }
}
