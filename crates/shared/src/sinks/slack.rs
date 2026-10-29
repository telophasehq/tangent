use std::fmt;

use serde::{Deserialize, Serialize};

use crate::redact::Redacted;

/// Post each record to a Slack incoming webhook as one message, made from
/// `text`: `{field.path}` is the record's field (`-` when it has none), the
/// rest as written. A field's value is escaped, so a record cannot mention
/// a channel or dress up a link.
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
    "{_norm.actor} {_norm.kind}: {judge.why}".into()
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
