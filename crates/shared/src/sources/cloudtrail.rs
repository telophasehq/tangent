use serde::{Deserialize, Serialize};

/// CloudTrail, read from a trail's files through the SQS queue told of each.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudtrailConfig {
    #[serde(default)]
    pub region: Option<String>,
    #[serde(default)]
    pub profile: Option<String>,
    #[serde(default)]
    pub queue_url: Option<String>,
    #[serde(default)]
    pub bucket_role: Option<String>, // assumed to read another account's bucket
}
