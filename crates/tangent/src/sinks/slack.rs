use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use async_trait::async_trait;
use bytes::Bytes;
use parking_lot::Mutex;
use secrecy::{ExposeSecret, SecretString};
use serde_json::Value;
use tangent_shared::sinks::slack::SlackConfig;

use crate::sinks::manager::{Sink, SinkWrite};
use crate::{SINK_BYTES_TOTAL, SINK_OBJECTS_TOTAL};

pub struct SlackSink {
    client: reqwest::Client,
    url: SecretString,
    text: String,
    /// The line a failed write stopped at, by its payload. The manager retries
    /// a write whole, and the retry starts there, so Slack does not get a line
    /// twice. Two writes of one payload at once share an entry: a line is
    /// then at worst posted twice, never skipped.
    failed_at: Mutex<HashMap<Bytes, usize>>,
}

impl SlackSink {
    pub fn new(cfg: &SlackConfig) -> Result<Arc<Self>> {
        let url = reqwest::Url::parse(cfg.webhook_url.expose_secret())
            .map_err(|e| anyhow!("slack webhook_url is not a url: {e}"))?;
        if url.scheme() != "https" {
            bail!("slack webhook_url must be https");
        }
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::none())
            .build()?;
        Ok(Arc::new(Self {
            client,
            url: cfg.webhook_url.clone(),
            text: cfg.text.clone(),
            failed_at: Mutex::default(),
        }))
    }

    /// Ok once Slack has the message, or has refused it: posting it again
    /// would be refused again. Err when another try can succeed: a 429, after
    /// the wait its `retry-after` asks for, a 5xx, a timeout or no connection.
    async fn post(&self, text: &str) -> Result<()> {
        let body =
            serde_json::json!({ "text": text, "unfurl_links": false, "unfurl_media": false });
        let r = self
            .client
            .post(self.url.expose_secret())
            .json(&body)
            .send()
            .await
            .map_err(|e| anyhow!("slack post failed: {}", e.without_url()))?;
        let status = r.status();
        if status.is_success() {
            SINK_OBJECTS_TOTAL.inc();
            SINK_BYTES_TOTAL.inc_by(text.len() as u64);
            return Ok(());
        }
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            let wait = r
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse().ok())
                .unwrap_or(1);
            tokio::time::sleep(Duration::from_secs(wait)).await;
            bail!("slack post failed: {status}, waited the {wait}s it asked");
        }
        if status.is_server_error() {
            bail!("slack post failed: {status}");
        }
        tracing::warn!("slack post failed: {status}");
        Ok(())
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
        let payload = req.payload.freeze();
        let start = self.failed_at.lock().remove(&payload).unwrap_or(0);
        for (i, line) in crate::inference::lines(&payload).enumerate().skip(start) {
            let v = match serde_json::from_slice::<Value>(line) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("slack sink: skipping non-JSON line: {e}");
                    continue;
                }
            };
            if let Err(e) = self.post(&render(&self.text, &v)).await {
                self.failed_at.lock().insert(payload.clone(), i);
                return Err(e);
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
        assert_eq!(
            render(
                "{_norm.kind|eventName|events.0.name}",
                &json!({"events": [{"name": "EMAIL_FORWARDING_OUT_OF_DOMAIN"}]})
            ),
            "EMAIL_FORWARDING_OUT_OF_DOMAIN"
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

    #[test]
    fn a_webhook_url_that_does_not_parse_is_refused() {
        let cfg = SlackConfig {
            webhook_url: "https://hooks.slack.com:99999/x".into(),
            text: "t".into(),
        };
        let err = SlackSink::new(&cfg).err().expect("refused").to_string();
        // the url is a secret: the error does not show it
        assert!(!err.contains("hooks.slack.com"), "{err}");
    }

    /// How the stub webhook answers a post.
    #[derive(Clone, Copy)]
    enum Reply {
        Status(u16),
        /// 429, `retry-after: 1`
        Later,
        /// nothing before the client gives up
        Hang,
    }

    /// Each post the stub got: its text, the status it answered (0 for none)
    /// and when.
    type Posts = Arc<std::sync::Mutex<Vec<(String, u16, std::time::Instant)>>>;

    /// A Slack webhook on localhost: each post gets the next of `replies`, then
    /// 200.
    async fn webhook(replies: Vec<Reply>) -> (String, Posts) {
        use axum::{http::StatusCode, response::IntoResponse, Json};
        let posts = Posts::default();
        let replies = Arc::new(std::sync::Mutex::new(std::collections::VecDeque::from(
            replies,
        )));
        let handler = {
            let posts = Arc::clone(&posts);
            move |Json(body): Json<Value>| {
                let (posts, replies) = (Arc::clone(&posts), Arc::clone(&replies));
                async move {
                    let text = body["text"].as_str().unwrap_or_default().to_string();
                    let reply = replies.lock().unwrap().pop_front();
                    let status = match reply.unwrap_or(Reply::Status(200)) {
                        Reply::Status(s) => s,
                        Reply::Later => 429,
                        Reply::Hang => 0,
                    };
                    let now = std::time::Instant::now();
                    posts.lock().unwrap().push((text, status, now));
                    match reply {
                        Some(Reply::Later) => {
                            (StatusCode::TOO_MANY_REQUESTS, [("retry-after", "1")]).into_response()
                        }
                        Some(Reply::Hang) => {
                            tokio::time::sleep(Duration::from_secs(30)).await;
                            StatusCode::OK.into_response()
                        }
                        _ => StatusCode::from_u16(status).unwrap().into_response(),
                    }
                }
            }
        };
        let app = axum::Router::new().route("/", axum::routing::post(handler));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await });
        (url, posts)
    }

    /// Records how many posts the stub had got when the write was acked.
    struct AckedAt(Posts, std::sync::Mutex<Option<usize>>);

    #[async_trait]
    impl crate::worker::Ack for AckedAt {
        async fn ack(&self) -> Result<()> {
            *self.1.lock().unwrap() = Some(self.0.lock().unwrap().len());
            Ok(())
        }
    }

    /// Writes `payload` to a slack sink through a sink manager, as a node's
    /// edge does; the post count at its ack, if it was acked.
    async fn send(url: &str, posts: &Posts, payload: &str) -> Option<usize> {
        // the stub is http, which `new` refuses; a short timeout keeps a hung post quick
        let sink = Arc::new(SlackSink {
            client: reqwest::Client::builder()
                .timeout(Duration::from_millis(300))
                .build()
                .unwrap(),
            url: url.into(),
            text: "{who}".into(),
            failed_at: Mutex::default(),
        });
        let name: Arc<str> = Arc::from("alerts");
        let manager = crate::sinks::manager::SinkManager::for_test(
            vec![(Arc::clone(&name), sink as Arc<dyn Sink>)],
            1,
        );
        let ack = Arc::new(AckedAt(Arc::clone(posts), Default::default()));
        manager
            .enqueue(name, bytes::BytesMut::from(payload), vec![ack.clone()])
            .await
            .unwrap();
        manager.join().await.unwrap();
        let acked_at = ack.1.lock().unwrap().take();
        acked_at
    }

    fn seen(posts: &Posts) -> Vec<(String, u16)> {
        let posts = posts.lock().unwrap();
        posts.iter().map(|(t, s, _)| (t.clone(), *s)).collect()
    }

    #[tokio::test]
    async fn a_failed_post_is_retried_and_acked_once_slack_has_it() {
        let (url, posts) = webhook(vec![
            Reply::Status(200),
            Reply::Status(500),
            Reply::Hang,
            Reply::Later,
            Reply::Status(503),
        ])
        .await;
        let acked_at = send(&url, &posts, "{\"who\":\"a\"}\n{\"who\":\"b\"}\n").await;
        let s = |t: &str, s| (t.to_string(), s);
        // `a` went out first time and is not posted again; the write is acked
        // after `b`'s 200, the sixth post
        assert_eq!(
            (seen(&posts), acked_at),
            (
                vec![
                    s("a", 200),
                    s("b", 500),
                    s("b", 0),
                    s("b", 429),
                    s("b", 503),
                    s("b", 200)
                ],
                Some(6)
            )
        );
        let posts = posts.lock().unwrap();
        assert!(
            posts[4].2 - posts[3].2 >= Duration::from_secs(1),
            "the post after a 429 waits its retry-after"
        );
    }
}
