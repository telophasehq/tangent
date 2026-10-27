use serde::{Deserialize, Serialize};

/// CloudTrail, read directly. By default through the `LookupEvents` API,
/// which every account answers for the last 90 days with nothing set up:
/// management events (IAM, STS, console logins, bucket policy changes),
/// about fifteen minutes behind, two requests a second. With `queue_url`,
/// the notifications of a trail's S3 delivery are consumed instead: every
/// event, data events included, gzipped files every few minutes.
///
/// Records are emitted as CloudTrail writes them, plus `source: cloudtrail`
/// and `ts` (the event time).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudtrailConfig {
    /// AWS region; the SDK's default chain when unset.
    #[serde(default)]
    pub region: Option<String>,
    /// AWS profile; the default credential chain when unset.
    #[serde(default)]
    pub profile: Option<String>,
    /// How often to poll `LookupEvents`.
    #[serde(default = "default_interval_secs")]
    pub interval_secs: u64,
    /// On first start, how far back to read (`15m`, `1h`, `7d`; 90 days at
    /// most). Afterwards the source resumes from where it stopped.
    #[serde(default = "default_since")]
    pub since: String,
    /// An SQS queue receiving the S3 event notifications of a trail's
    /// delivery bucket. Set, the source reads the delivered files instead of
    /// polling `LookupEvents`.
    #[serde(default)]
    pub queue_url: Option<String>,
}

const fn default_interval_secs() -> u64 {
    60
}
fn default_since() -> String {
    "1h".into()
}
