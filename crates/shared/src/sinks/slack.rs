use serde::{Deserialize, Serialize};

/// Post each record to a Slack incoming webhook as one message, made from
/// `text`: `{field.path}` is the record's field (`-` when it has none), the
/// rest as written. A field's value is escaped, so a record cannot mention
/// a channel or dress up a link.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct SlackConfig {
    pub webhook_url: String,
    #[serde(default = "default_text")]
    pub text: String,
}

fn default_text() -> String {
    "{_norm.actor} {_norm.kind}: {judge.why}".into()
}
