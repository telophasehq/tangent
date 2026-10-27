use serde::{Deserialize, Deserializer, Serialize};

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
    /// Regions to poll, each with its own cursor: a list, or one string with
    /// commas (`${AWS_REGIONS}`). Empty, `region` or the default chain
    /// decides. `LookupEvents` is regional, and IAM, STS and console sign-in
    /// events are recorded in `us-east-1`: include it.
    #[serde(default, deserialize_with = "list_or_csv")]
    pub regions: Vec<String>,
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

/// A list, or a comma-separated string (what `${VAR}` expands to), or nothing.
fn list_or_csv<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<String>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum ListOrCsv {
        List(Vec<String>),
        Csv(String),
    }
    let items = match Option::<ListOrCsv>::deserialize(d)? {
        Some(ListOrCsv::List(v)) => v,
        Some(ListOrCsv::Csv(s)) => s.split(',').map(str::to_string).collect(),
        None => Vec::new(),
    };
    Ok(items
        .into_iter()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn regions_as_list_csv_or_nothing() {
        let c: CloudtrailConfig = serde_yaml::from_str("regions: [us-east-1, us-west-2]").unwrap();
        assert_eq!(c.regions, ["us-east-1", "us-west-2"]);
        let c: CloudtrailConfig = serde_yaml::from_str("regions: us-east-1, us-west-2,").unwrap();
        assert_eq!(c.regions, ["us-east-1", "us-west-2"]);
        let c: CloudtrailConfig = serde_yaml::from_str("regions:").unwrap();
        assert!(c.regions.is_empty());
        let c: CloudtrailConfig = serde_yaml::from_str("since: 1d").unwrap();
        assert!(c.regions.is_empty());
    }
}
