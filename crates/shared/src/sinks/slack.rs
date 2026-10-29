use secrecy::SecretString;
use serde::{Deserialize, Serialize};

/// Post each record to a Slack incoming webhook as one message, made from
/// `text`: `{field.path}` is the record's field (`-` when it has none), the
/// rest as written. `{a|b}` is the first of `a` and `b` the record has; an
/// alternative's words before its path are written before its value, so
/// `{judge.why|not judged: judge.error}` says why a record was not judged.
///
/// A field's value is escaped, so a record cannot mention a channel, dress
/// up a link, start a line, or bold, strike or code the message: newlines
/// become spaces, `*`, `~`, a backtick and an `_` at a word's edge become
/// look-alikes, `://` becomes `[:]//`, a value stops at 300 characters and
/// a message at 3,000. Slack is told not to unfurl links.
///
/// Messages wait in a queue and are posted in order, one at a time, so a
/// slow Slack never holds the pipeline. A message Slack refuses or does not
/// take within 30 s, one the full queue has no room for, and what is still
/// queued 10 s into shutdown are dropped: logged, and counted in
/// `tangent_slack_dropped_total`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SlackConfig {
    /// Whoever has it can post to the channel: `${SLACK_WEBHOOK_URL}`.
    #[serde(skip_serializing)]
    pub webhook_url: SecretString,
    #[serde(default = "default_text")]
    pub text: String,
}

fn default_text() -> String {
    "{_norm.actor} {_norm.kind}: {judge.why|not judged: judge.error}".into()
}
