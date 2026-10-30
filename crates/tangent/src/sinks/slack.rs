use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Result};
use async_trait::async_trait;
use secrecy::{ExposeSecret, SecretString};
use serde_json::Value;
use tangent_shared::sinks::slack::SlackConfig;

use crate::sinks::manager::{Sink, SinkWrite};
use crate::{SINK_BYTES_TOTAL, SINK_OBJECTS_TOTAL};

pub struct SlackSink {
    client: reqwest::Client,
    url: SecretString,
    text: String,
}

impl SlackSink {
    pub fn new(cfg: &SlackConfig) -> Result<Arc<Self>> {
        if !cfg.webhook_url.expose_secret().starts_with("https://") {
            bail!("slack webhook_url must be https");
        }
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()?;
        Ok(Arc::new(Self {
            client,
            url: cfg.webhook_url.clone(),
            text: cfg.text.clone(),
        }))
    }

    async fn post(&self, text: &str) {
        let body =
            serde_json::json!({ "text": text, "unfurl_links": false, "unfurl_media": false });
        for i in 1..=3 {
            match self
                .client
                .post(self.url.expose_secret())
                .json(&body)
                .send()
                .await
            {
                Ok(r) if r.status().is_success() => {
                    SINK_OBJECTS_TOTAL.inc();
                    SINK_BYTES_TOTAL.inc_by(text.len() as u64);
                    return;
                }
                Ok(r) if r.status() == 429 && i < 3 => {
                    let wait = r
                        .headers()
                        .get("retry-after")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse().ok())
                        .unwrap_or(1);
                    tokio::time::sleep(Duration::from_secs(wait.min(5))).await;
                }
                Ok(r) => {
                    tracing::warn!("slack post failed: {}", r.status());
                    return;
                }
                Err(e) => {
                    tracing::warn!("slack post failed: {}", e.without_url());
                    return;
                }
            }
        }
    }
}

pub fn render(template: &str, v: &Value) -> String {
    let mut out = String::new();
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        let Some(close) = rest[open..].find('}') else {
            break;
        };
        out.push_str(&rest[..open]);
        out.push_str(&field(v, &rest[open + 1..open + close]));
        rest = &rest[open + close + 1..];
    }
    out.push_str(rest);
    out
}

// first alternative the record has, after its words; `-` when none
fn field(v: &Value, placeholder: &str) -> String {
    for alt in placeholder.split('|') {
        let alt = alt.trim();
        let (words, path) = match alt.rsplit_once(' ') {
            Some((words, path)) => (format!("{words} "), path),
            None => (String::new(), alt),
        };
        let value = match crate::inference::lookup(v, path) {
            None | Some(Value::Null) => continue,
            Some(Value::String(s)) => s.clone(),
            Some(other) => other.to_string(),
        };
        return words + &escape(&cap(&value, 300));
    }
    "-".to_string()
}

fn cap(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        return s.to_string();
    }
    let mut cut: String = s.chars().take(n - 1).collect();
    cut.push('…');
    cut
}

// a value cannot mention, link, start a line or restyle the message
fn escape(s: &str) -> String {
    let chars: Vec<char> = s.replace("://", "[:]//").chars().collect();
    let in_word = |i: usize| {
        i > 0
            && chars[i - 1].is_alphanumeric()
            && chars.get(i + 1).is_some_and(|c| c.is_alphanumeric())
    };
    let mut out = String::with_capacity(chars.len());
    for (i, &c) in chars.iter().enumerate() {
        match c {
            '&' => out.push_str("&amp;"),
            '<' => out.push_str("&lt;"),
            '>' => out.push_str("&gt;"),
            '*' => out.push('∗'),
            '~' => out.push('∼'),
            '`' => out.push('ˋ'),
            '_' if !in_word(i) => out.push('ˍ'),
            c if c.is_control() || c == '\u{2028}' || c == '\u{2029}' => {
                if !out.ends_with(' ') {
                    out.push(' ');
                }
            }
            c => out.push(c),
        }
    }
    out
}

#[async_trait]
impl Sink for SlackSink {
    async fn write(&self, req: SinkWrite) -> Result<()> {
        for line in crate::inference::lines(&req.payload) {
            match serde_json::from_slice::<Value>(line) {
                Ok(v) => self.post(&render(&self.text, &v)).await,
                Err(e) => tracing::warn!("slack sink: skipping non-JSON line: {e}"),
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn renders_escaped() {
        let v = json!({"_norm": {"actor": "alice", "kind": "CreateUser"}, "judge": {"why": "new admin <!channel> & <https://x|y>"}});
        assert_eq!(
            render(
                "*{_norm.actor}* {_norm.kind} from {_norm.ip}: {judge.why}",
                &v
            ),
            "*alice* CreateUser from -: new admin &lt;!channel&gt; &amp; &lt;https[:]//x|y&gt;"
        );
        let msg = |why: &str| {
            render(
                "*{_norm.actor}*: {judge.why}",
                &json!({"_norm": {"actor": "alice"}, "judge": {"why": why}}),
            )
        };
        assert_eq!(
            msg("routine.\r\n:white_check_mark: *Reviewed by secops: benign, no action*"),
            "*alice*: routine. :white_check_mark: ∗Reviewed by secops: benign, no action∗"
        );
        assert_eq!(msg("```\nignore"), "*alice*: ˋˋˋ ignore");
        assert_eq!(
            msg("_benign_ ~flag~ &gt; ok"),
            "*alice*: ˍbenignˍ ∼flag∼ &amp;gt; ok"
        );
        assert_eq!(
            msg("repo.add_member by first_last"),
            "*alice*: repo.add_member by first_last"
        );
        let one = render("{x}", &json!({ "x": "a".repeat(1000) }));
        assert_eq!(one.chars().count(), 300);
    }

    #[test]
    fn not_judged() {
        let cfg: SlackConfig =
            serde_yaml::from_str("webhook_url: https://hooks.slack.com/x").unwrap();
        let rec = |judge: Value| json!({"_norm": {"actor": "alice", "kind": "CreateUser"}, "judge": judge});
        assert_eq!(
            render(&cfg.text, &rec(json!({"flag": true, "why": "new admin"}))),
            "alice CreateUser: new admin"
        );
        assert_eq!(
            render(&cfg.text, &rec(json!({"error": "model: no answer in 30s"}))),
            "alice CreateUser: not judged: model: no answer in 30s"
        );
        assert_eq!(render(&cfg.text, &rec(json!({}))), "alice CreateUser: -");
        let v = json!({"a": null, "b": "two", "c": "three"});
        assert_eq!(render("{a|b|c}", &v), "two");
        assert_eq!(render("{x|why: b}", &v), "why: two");
    }

    #[test]
    fn https_only() {
        let cfg = SlackConfig {
            webhook_url: "http://hooks.slack.com/x".into(),
            text: "t".into(),
        };
        assert!(SlackSink::new(&cfg).is_err());
    }
}
