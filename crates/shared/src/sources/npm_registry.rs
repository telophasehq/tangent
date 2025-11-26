use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
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

fn default_interval_secs() -> u64 {
    30
}
