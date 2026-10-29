use std::fmt;

use serde::{Deserialize, Serialize};

use crate::redact::Redacted;

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
#[derive(Clone, Deserialize, Serialize)]
pub struct SlackConfig {
    /// Whoever has it can post to the channel: `${SLACK_WEBHOOK_URL}`.
    pub webhook_url: String,
    #[serde(default = "default_text")]
    pub text: String,
}

/// By hand: the startup log shows the webhook as `<redacted>`.
impl fmt::Debug for SlackConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SlackConfig")
            .field("webhook_url", &Redacted(&self.webhook_url))
            .field("text", &self.text)
            .finish()
    }
}

fn default_text() -> String {
    "{_norm.actor} {_norm.kind}: {judge.why|not judged: judge.error}".into()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_hides_the_webhook() {
        let c: SlackConfig =
            serde_yaml::from_str("webhook_url: https://hooks.slack.com/services/T0/B0/SECRET")
                .unwrap();
        crate::redact::assert_hides(&c, &["SECRET", "hooks.slack.com"]);
    }
}
