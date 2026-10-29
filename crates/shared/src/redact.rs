//! What a config's `Debug` shows of a credential: whether it is set, never
//! what it is. The runtime logs its whole config at startup, after `${VAR}`
//! is expanded, so a derived `Debug` would print every token in the log.

use std::fmt;

/// A credential as `Debug` shows it: `<redacted>`, or `""` when it is empty
/// (a `${VAR}` that was not set), so the startup log still says which.
pub struct Redacted<'a, T: ?Sized>(pub &'a T);

impl fmt::Debug for Redacted<'_, String> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.0.is_empty() {
            f.write_str("\"\"")
        } else {
            f.write_str("<redacted>")
        }
    }
}

impl fmt::Debug for Redacted<'_, Option<String>> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.0 {
            Some(s) => f.debug_tuple("Some").field(&Redacted(s)).finish(),
            None => f.write_str("None"),
        }
    }
}

/// `cfg`'s `Debug` holds none of `secrets` and still names every field it
/// serializes: a hand-written `Debug` must not drop one.
#[cfg(test)]
pub(crate) fn assert_hides<T: fmt::Debug + serde::Serialize>(cfg: &T, secrets: &[&str]) {
    let shown = format!("{cfg:?}");
    for s in secrets {
        assert!(!shown.contains(s), "{s} is in {shown}");
    }
    assert!(shown.contains("<redacted>"), "{shown}");
    if let serde_json::Value::Object(fields) = serde_json::to_value(cfg).unwrap() {
        for k in fields.keys() {
            assert!(
                shown.contains(&format!("{k}: ")),
                "{k} is missing from {shown}"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_credential_shows_whether_it_is_set() {
        assert_eq!(
            format!("{:?}", Redacted(&"xoxb-1".to_string())),
            "<redacted>"
        );
        assert_eq!(format!("{:?}", Redacted(&String::new())), "\"\"");
        assert_eq!(
            format!("{:?}", Redacted(&Some("xoxb-1".to_string()))),
            "Some(<redacted>)"
        );
        assert_eq!(format!("{:?}", Redacted(&None::<String>)), "None");
    }

    /// The startup log: the whole config, every credential set.
    #[test]
    fn the_config_s_debug_holds_no_credential() {
        let yaml = r#"
runtime: {}
sources:
  gh: { type: github_audit, org: acme, token: ghp_AUDIT }
  hook: { type: github_webhook, path: /h, secret: HOOK_SECRET, token: ghp_HOOK }
  npm: { type: npm_registry, orgs: [acme], token: npm_TOKEN }
  okta: { type: okta, org_url: "https://acme.okta.com", token: SSWS_TOKEN }
  kafka:
    type: msk
    bootstrap_servers: b:9092
    topic: t
    auth: { mode: scram, username: u, password: MSK_PASSWORD }
    decoding: { format: { type: ndjson } }
sinks:
  slack: { type: slack, webhook_url: "https://hooks.slack.com/services/T0/B0/WEBHOOK" }
inference:
  judge: { model: m, prompt_file: p.md, api_key: sk-JUDGE }
"#;
        let cfg: crate::Config = serde_yaml::from_str(yaml).unwrap();
        let shown = format!("{cfg:?}");
        for s in [
            "ghp_AUDIT",
            "HOOK_SECRET",
            "ghp_HOOK",
            "npm_TOKEN",
            "SSWS_TOKEN",
            "MSK_PASSWORD",
            "WEBHOOK",
            "sk-JUDGE",
        ] {
            assert!(!shown.contains(s), "{s} is in {shown}");
        }
        assert!(
            shown.contains("acme.okta.com") && shown.contains("org: Some(\"acme\")"),
            "{shown}"
        );
    }
}
