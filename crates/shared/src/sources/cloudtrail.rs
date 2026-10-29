use serde::{Deserialize, Serialize};

/// CloudTrail, read from a trail's delivery: the notifications of each file
/// the trail writes to its bucket are consumed from an SQS queue
/// (`queue_url`), and every record of every file is emitted: every event,
/// data events included, gzipped files every few minutes. An organization
/// trail is read this way: one bucket for every account and region, usually
/// the log archive account's (`bucket_role`). (There is no `LookupEvents`
/// polling: a poller's own calls are CloudTrail events, which it would read
/// back, its own traffic drowning the account's.)
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
    /// An SQS queue told of each file the trail delivers: S3 event
    /// notifications on the trail's bucket (straight, through SNS or through
    /// EventBridge), or the trail's own SNS notifications. With Control
    /// Tower, the trail notifies the audit account's
    /// `aws-controltower-AllConfigNotifications` topic, which AWS Config
    /// shares; its messages are skipped. Required.
    #[serde(default)]
    pub queue_url: Option<String>,
    /// A role to assume for reading the trail's bucket when it is another
    /// account's (an organization trail's is the log archive account's).
    /// The queue is read with the source's own credentials.
    #[serde(default)]
    pub bucket_role: Option<String>,
}
