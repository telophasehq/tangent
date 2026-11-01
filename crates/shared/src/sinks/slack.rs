use secrecy::SecretString;
use serde::{Deserialize, Serialize};

/// One message a record: `{a.b}` is the record's field, escaped (`-` when missing),
/// and `{x|words y}` the first one it has.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SlackConfig {
    #[serde(skip_serializing)]
    pub webhook_url: SecretString,
    #[serde(default = "default_text")]
    pub text: String,
}

fn default_text() -> String {
    "{_norm.actor} {_norm.kind}: {judge.why|not judged: judge.error}".into()
}
