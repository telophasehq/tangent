//! The `slack` sink: each record a message in a Slack channel, through an
//! incoming webhook.

use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use serde_json::Value;
use tangent_shared::sinks::slack::SlackConfig;

use crate::sinks::manager::{Sink, SinkWrite};
use crate::{SINK_BYTES_TOTAL, SINK_OBJECTS_TOTAL};

/// Tries per message; Slack answers a webhook posting too fast with 429.
const TRIES: u32 = 4;

pub struct SlackSink {
    client: reqwest::Client,
    url: String,
    text: String,
}

impl SlackSink {
    pub fn new(cfg: &SlackConfig) -> Result<Arc<Self>> {
        if !cfg.webhook_url.starts_with("https://") {
            return Err(anyhow!("slack sink: webhook_url must be an https URL"));
        }
        let client = reqwest::Client::builder().timeout(Duration::from_secs(10)).build()?;
        Ok(Arc::new(Self { client, url: cfg.webhook_url.clone(), text: cfg.text.clone() }))
    }

    async fn post(&self, text: &str) -> Result<()> {
        let body = serde_json::json!({ "text": text });
        let mut last = String::new();
        for attempt in 0..TRIES {
            match self.client.post(&self.url).json(&body).send().await {
                Ok(r) if r.status().is_success() => return Ok(()),
                Ok(r) => {
                    let wait = r
                        .headers()
                        .get("retry-after")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse::<u64>().ok())
                        .unwrap_or(1 << attempt);
                    last = format!("{}", r.status());
                    if !(r.status().as_u16() == 429 || r.status().is_server_error()) {
                        break;
                    }
                    tokio::time::sleep(Duration::from_secs(wait.min(30))).await;
                }
                Err(e) => {
                    last = e.to_string();
                    tokio::time::sleep(Duration::from_secs(1 << attempt)).await;
                }
            }
        }
        Err(anyhow!("slack sink: the webhook did not take a message: {last}"))
    }
}

/// `text` with each `{field.path}` the record's value, escaped for Slack.
pub fn render(template: &str, v: &Value) -> String {
    let mut out = String::new();
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        let Some(close) = rest[open..].find('}') else { break };
        out.push_str(&rest[..open]);
        let path = &rest[open + 1..open + close];
        let value = match crate::inference::lookup(v, path) {
            None | Some(Value::Null) => "-".to_string(),
            Some(Value::String(s)) => s.clone(),
            Some(other) => other.to_string(),
        };
        out.push_str(&escape(&value));
        rest = &rest[open + close + 1..];
    }
    out.push_str(rest);
    out
}

/// What Slack reads as markup in a message: `<!channel>`, `<url|label>`.
fn escape(s: &str) -> String {
    s.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

#[async_trait]
impl Sink for SlackSink {
    async fn write(&self, req: SinkWrite) -> Result<()> {
        for line in crate::inference::lines(&req.payload) {
            let v: Value = match serde_json::from_slice(line) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("slack sink: a line that is not JSON is skipped: {e}");
                    continue;
                }
            };
            // One message lost is logged with what it was; the rest still go.
            let text = render(&self.text, &v);
            if let Err(e) = self.post(&text).await {
                tracing::error!(message = %text, "{e:#}");
            }
        }
        SINK_OBJECTS_TOTAL.inc();
        SINK_BYTES_TOTAL.inc_by(req.payload.len() as u64);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_message_is_the_template_with_the_record_s_fields_escaped() {
        let v = serde_json::json!({"_norm": {"actor": "alice", "kind": "CreateUser"}, "judge": {"why": "new admin <!channel> & <https://x|y>"}});
        assert_eq!(
            render("*{_norm.actor}* {_norm.kind} from {_norm.ip}: {judge.why}", &v),
            "*alice* CreateUser from -: new admin &lt;!channel&gt; &amp; &lt;https://x|y&gt;"
        );
    }

    #[tokio::test]
    async fn each_record_is_posted_as_one_message() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let mut bodies = Vec::new();
            for _ in 0..2 {
                let (mut sock, _) = listener.accept().await.unwrap();
                let mut buf = vec![0u8; 8192];
                let mut got = 0;
                loop {
                    got += sock.read(&mut buf[got..]).await.unwrap();
                    let text = String::from_utf8_lossy(&buf[..got]).to_string();
                    if let Some(head) = text.find("\r\n\r\n") {
                        let len: usize = text[..head]
                            .lines()
                            .find_map(|l| l.to_ascii_lowercase().strip_prefix("content-length: ").map(|n| n.trim().parse().unwrap()))
                            .unwrap();
                        if got >= head + 4 + len {
                            bodies.push(text[head + 4..head + 4 + len].to_string());
                            break;
                        }
                    }
                }
                sock.write_all(b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok").await.unwrap();
            }
            bodies
        });
        let sink = SlackSink {
            client: reqwest::Client::new(),
            url: format!("http://{addr}/hook"),
            text: "{_norm.actor}: {judge.why}".into(),
        };
        let mut payload = bytes::BytesMut::new();
        payload.extend_from_slice(b"{\"_norm\":{\"actor\":\"alice\"},\"judge\":{\"why\":\"one\"}}\n{\"_norm\":{\"actor\":\"bob\"},\"judge\":{\"why\":\"two\"}}\n");
        sink.write(SinkWrite { sink_name: Arc::from("slack"), payload, s3: None }).await.unwrap();
        let bodies = server.await.unwrap();
        assert_eq!(bodies, [r#"{"text":"alice: one"}"#, r#"{"text":"bob: two"}"#]);
    }

    #[test]
    fn a_webhook_is_https() {
        let cfg = SlackConfig { webhook_url: "http://hooks.slack.com/x".into(), text: "t".into() };
        assert!(SlackSink::new(&cfg).is_err());
    }
}
