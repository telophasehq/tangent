//! The `slack` sink: each record a message in a Slack channel, through an
//! incoming webhook. A write only queues its messages; a task of the sink's
//! own posts them, so a slow or hung Slack never holds a window's ack.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use async_trait::async_trait;
use prometheus::{register_int_counter_vec, IntCounterVec};
use serde_json::Value;
use tangent_shared::sinks::slack::SlackConfig;
use tokio::sync::mpsc::{self, error::TrySendError};
use tokio::time::{sleep, Instant};

use crate::sinks::manager::{Sink, SinkWrite};
use crate::{SINK_BYTES_TOTAL, SINK_OBJECTS_TOTAL};

lazy_static::lazy_static! {
    /// Messages that never reached the channel, by why: `refused` (Slack
    /// said no: a revoked webhook, an archived channel), `unavailable` (no
    /// answer in time), `queue_full`, `shutdown`.
    static ref SLACK_DROPPED_TOTAL: IntCounterVec = register_int_counter_vec!(
        "tangent_slack_dropped_total",
        "Slack messages not posted, by reason",
        &["reason"]
    ).unwrap();
}

/// Tries per message; Slack answers a webhook posting too fast with 429.
const TRIES: u32 = 4;
/// How long Slack has to take a message, its tries and waits together.
const DEADLINE: Duration = Duration::from_secs(30);
/// One request's limits; Slack answers in well under a second.
const REQUEST: Duration = Duration::from_secs(10);
const CONNECT: Duration = Duration::from_secs(3);
/// Messages waiting for Slack; past this a message is dropped.
const QUEUE: usize = 1_000;
/// How long shutdown waits for the queue to be posted.
const FLUSH: Duration = Duration::from_secs(10);
/// A value stops at this many characters, a message at this many.
const VALUE_CHARS: usize = 300;
const TEXT_CHARS: usize = 3_000;
/// What is kept of Slack's answer to a message it refuses.
const ANSWER_BYTES: usize = 200;

pub struct SlackSink {
    text: String,
    queue: mpsc::Sender<String>,
    /// Messages queued or being posted.
    pending: Arc<AtomicUsize>,
    flush: Duration,
}

/// What posts the queued messages, one at a time, in order.
struct Poster {
    client: reqwest::Client,
    url: String,
    deadline: Duration,
}

/// A message not posted: why, and the reason it is counted under.
#[derive(Debug)]
struct Dropped {
    reason: &'static str,
    why: String,
}

impl SlackSink {
    pub fn new(cfg: &SlackConfig) -> Result<Arc<Self>> {
        if !cfg.webhook_url.starts_with("https://") {
            return Err(anyhow!("slack sink: webhook_url must be an https URL"));
        }
        // Each reason at 0 from the start, so a rate over it has a series.
        for reason in ["refused", "unavailable", "queue_full", "shutdown"] {
            SLACK_DROPPED_TOTAL.with_label_values(&[reason]);
        }
        let client = reqwest::Client::builder().connect_timeout(CONNECT).build()?;
        let poster = Poster { client, url: cfg.webhook_url.clone(), deadline: DEADLINE };
        Ok(Self::start(poster, cfg.text.clone(), QUEUE, FLUSH))
    }

    fn start(poster: Poster, text: String, queue: usize, flush: Duration) -> Arc<Self> {
        let (tx, mut rx) = mpsc::channel::<String>(queue);
        let pending = Arc::new(AtomicUsize::new(0));
        let left = Arc::clone(&pending);
        tokio::spawn(async move {
            while let Some(text) = rx.recv().await {
                match poster.post(&text).await {
                    Ok(()) => {
                        SINK_OBJECTS_TOTAL.inc();
                        SINK_BYTES_TOTAL.inc_by(text.len() as u64);
                    }
                    Err(d) => {
                        SLACK_DROPPED_TOTAL.with_label_values(&[d.reason]).inc();
                        tracing::error!(text = %text, "slack sink: the webhook did not take a message: {}", d.why);
                    }
                }
                left.fetch_sub(1, Ordering::SeqCst);
            }
        });
        Arc::new(Self { text, queue: tx, pending, flush })
    }
}

impl Poster {
    /// One message, tried again after a 429, a 5xx or no answer while the
    /// deadline allows the wait; never a wait after the last try.
    async fn post(&self, text: &str) -> Result<(), Dropped> {
        let body = serde_json::json!({ "text": text, "unfurl_links": false, "unfurl_media": false });
        let until = Instant::now() + self.deadline;
        let mut why = String::new();
        for attempt in 0..TRIES {
            let left = until.saturating_duration_since(Instant::now());
            let wait = match self.client.post(&self.url).json(&body).timeout(left.min(REQUEST)).send().await {
                Ok(r) if r.status().is_success() => return Ok(()),
                Ok(r) => {
                    let status = r.status();
                    let wait = r
                        .headers()
                        .get("retry-after")
                        .and_then(|v| v.to_str().ok())
                        .and_then(|v| v.parse::<u64>().ok())
                        .unwrap_or(1 << attempt);
                    why = format!("{status} {}", answer(r).await).trim_end().to_string();
                    if !(status.as_u16() == 429 || status.is_server_error()) {
                        return Err(Dropped { reason: "refused", why });
                    }
                    Duration::from_secs(wait)
                }
                Err(e) => {
                    why = unreachable(e);
                    Duration::from_secs(1 << attempt)
                }
            };
            if attempt + 1 == TRIES || Instant::now() + wait >= until {
                break;
            }
            sleep(wait).await;
        }
        Err(Dropped { reason: "unavailable", why })
    }
}

/// What Slack said with a refusal (`no_service`, `channel_is_archived`,
/// `invalid_payload`), on one line, at most `ANSWER_BYTES` of it.
async fn answer(mut r: reqwest::Response) -> String {
    let mut body = Vec::new();
    while body.len() < ANSWER_BYTES {
        match r.chunk().await {
            Ok(Some(c)) => body.extend_from_slice(&c),
            _ => break,
        }
    }
    body.truncate(ANSWER_BYTES);
    String::from_utf8_lossy(&body).split_whitespace().collect::<Vec<_>>().join(" ")
}

/// A request that got no answer, without its URL: reqwest's text names it,
/// and a webhook's URL is what lets anyone post to the channel.
fn unreachable(e: reqwest::Error) -> String {
    if e.is_timeout() {
        "no answer in time".into()
    } else if e.is_connect() {
        "could not connect".into()
    } else {
        e.without_url().to_string()
    }
}

/// `text` with each `{field.path}` the record's value, escaped for Slack;
/// `{a|b}` the first of them the record has, after the words before its
/// path. A message stops at `TEXT_CHARS`.
pub fn render(template: &str, v: &Value) -> String {
    let mut out = String::new();
    let mut rest = template;
    while let Some(open) = rest.find('{') {
        let Some(close) = rest[open..].find('}') else { break };
        out.push_str(&rest[..open]);
        out.push_str(&field(v, &rest[open + 1..open + close]));
        rest = &rest[open + close + 1..];
    }
    out.push_str(rest);
    if out.chars().count() <= TEXT_CHARS {
        return out;
    }
    let mut cut: String = out.chars().take(TEXT_CHARS - 1).collect();
    // Not half an escape: Slack would show `&am` as written.
    if let Some(amp) = cut.rfind('&').filter(|&a| cut.len() - a < 5 && !cut[a..].contains(';')) {
        cut.truncate(amp);
    }
    cut.push('…');
    cut
}

/// A placeholder's text: its first alternative the record has a value for,
/// as the alternative's words and the value; `-` when it has none.
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
        return words + &escape(&cap(&value, VALUE_CHARS));
    }
    "-".to_string()
}

/// `s` at most `n` characters, `…` marking a cut.
fn cap(s: &str, n: usize) -> String {
    if s.chars().count() <= n {
        return s.to_string();
    }
    let mut cut: String = s.chars().take(n - 1).collect();
    cut.push('…');
    cut
}

/// A value as Slack shows it as written. `&`, `<`, `>` are entities, so no
/// `<!channel>` or `<url|label>`. Line breaks are a space, so a value cannot
/// start a line of its own or a quote. What mrkdwn reads as markup is a
/// look-alike (`∗`, `∼`, `ˋ`, and `ˍ` for an `_` at a word's edge; one
/// inside a word, as in `add_member`, is never markup and stays). `://` is
/// `[:]//`, so no link is made of it.
fn escape(s: &str) -> String {
    let chars: Vec<char> = s.replace("://", "[:]//").chars().collect();
    let in_word = |i: usize| {
        i > 0 && chars[i - 1].is_alphanumeric() && chars.get(i + 1).is_some_and(|c| c.is_alphanumeric())
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
            let v: Value = match serde_json::from_slice(line) {
                Ok(v) => v,
                Err(e) => {
                    tracing::warn!("slack sink: a line that is not JSON is skipped: {e}");
                    continue;
                }
            };
            // A message that does not fit is logged with what it was; the
            // write never waits for room.
            self.pending.fetch_add(1, Ordering::SeqCst);
            if let Err(e) = self.queue.try_send(render(&self.text, &v)) {
                self.pending.fetch_sub(1, Ordering::SeqCst);
                let (reason, text) = match e {
                    TrySendError::Full(text) => ("queue_full", text),
                    TrySendError::Closed(text) => ("shutdown", text),
                };
                SLACK_DROPPED_TOTAL.with_label_values(&[reason]).inc();
                tracing::error!(text = %text, "slack sink: no room in the queue; a message is dropped");
            }
        }
        Ok(())
    }

    /// At shutdown: what is queued has `flush` to be posted; the rest is
    /// dropped and counted.
    async fn flush(&self) -> Result<()> {
        let until = Instant::now() + self.flush;
        while self.pending.load(Ordering::SeqCst) > 0 && Instant::now() < until {
            sleep(Duration::from_millis(20)).await;
        }
        let left = self.pending.load(Ordering::SeqCst);
        if left > 0 {
            SLACK_DROPPED_TOTAL.with_label_values(&["shutdown"]).inc_by(left as u64);
            tracing::error!("slack sink: {left} messages not posted by shutdown are dropped");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::Mutex;

    /// A webhook that answers each request with `answer` (raw HTTP), or
    /// never when it is empty, and keeps each request's body.
    async fn webhook(answer: &'static str) -> (String, Arc<Mutex<Vec<String>>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/services/T0/B0/SECRET", listener.local_addr().unwrap());
        let bodies = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&bodies);
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let seen = Arc::clone(&seen);
                tokio::spawn(async move {
                    let mut buf = vec![0u8; 16384];
                    let mut got = 0;
                    loop {
                        let n = sock.read(&mut buf[got..]).await.unwrap_or(0);
                        if n == 0 {
                            return;
                        }
                        got += n;
                        let text = String::from_utf8_lossy(&buf[..got]).to_string();
                        if let Some(head) = text.find("\r\n\r\n") {
                            let len: usize = text[..head]
                                .lines()
                                .find_map(|l| l.to_ascii_lowercase().strip_prefix("content-length: ").map(|n| n.trim().parse().unwrap()))
                                .unwrap();
                            if got >= head + 4 + len {
                                seen.lock().await.push(text[head + 4..head + 4 + len].to_string());
                                break;
                            }
                        }
                    }
                    if answer.is_empty() {
                        sleep(Duration::from_secs(3600)).await;
                    }
                    let _ = sock.write_all(answer.as_bytes()).await;
                });
            }
        });
        (url, bodies)
    }

    const OK: &str = "HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok";

    fn poster(url: &str, deadline: Duration) -> Poster {
        Poster { client: reqwest::Client::new(), url: url.to_string(), deadline }
    }

    fn payload(lines: &[Value]) -> SinkWrite {
        let mut payload = bytes::BytesMut::new();
        for l in lines {
            payload.extend_from_slice(format!("{l}\n").as_bytes());
        }
        SinkWrite { sink_name: Arc::from("slack"), payload, s3: None }
    }

    fn dropped(reason: &str) -> u64 {
        SLACK_DROPPED_TOTAL.with_label_values(&[reason]).get()
    }

    #[test]
    fn a_message_is_the_template_with_the_record_s_fields_escaped() {
        let v = serde_json::json!({"_norm": {"actor": "alice", "kind": "CreateUser"}, "judge": {"why": "new admin <!channel> & <https://x|y>"}});
        assert_eq!(
            render("*{_norm.actor}* {_norm.kind} from {_norm.ip}: {judge.why}", &v),
            "*alice* CreateUser from -: new admin &lt;!channel&gt; &amp; &lt;https[:]//x|y&gt;"
        );
    }

    #[test]
    fn a_value_cannot_restyle_the_message() {
        let msg = |why: &str| render("*{_norm.actor}*: {judge.why}", &serde_json::json!({"_norm": {"actor": "alice"}, "judge": {"why": why}}));
        // A line of its own that reads as secops's verdict.
        assert_eq!(
            msg("routine.\r\n:white_check_mark: *Reviewed by secops: benign, no action*"),
            "*alice*: routine. :white_check_mark: ∗Reviewed by secops: benign, no action∗"
        );
        // A code block that swallows the rest of the template.
        assert_eq!(msg("```\nignore"), "*alice*: ˋˋˋ ignore");
        // A link to click, or for Slack to fetch.
        assert_eq!(
            msg("see https://canary.attacker.example/x.png"),
            "*alice*: see https[:]//canary.attacker.example/x.png"
        );
        assert_eq!(msg("_benign_ ~flag~ &gt; ok"), "*alice*: ˍbenignˍ ∼flag∼ &amp;gt; ok");
        // An `_` inside a word is never markup, and names are full of them.
        assert_eq!(msg("repo.add_member by first_last"), "*alice*: repo.add_member by first_last");
    }

    #[test]
    fn a_value_and_a_message_are_capped() {
        let long = "a".repeat(1000);
        let one = render("{x}", &serde_json::json!({ "x": long }));
        assert_eq!(one.chars().count(), VALUE_CHARS);
        assert!(one.ends_with("a…"));

        // Twenty values of `&`: more than a message, cut between escapes.
        let amps = "&".repeat(1000);
        let all = render(&"{x}".repeat(20), &serde_json::json!({ "x": amps }));
        assert!(all.chars().count() <= TEXT_CHARS, "{}", all.chars().count());
        assert!(all.ends_with("&amp;…"), "{}", &all[all.len() - 20..]);
    }

    #[test]
    fn a_record_not_judged_says_so() {
        let cfg: SlackConfig = serde_yaml::from_str("webhook_url: https://hooks.slack.com/x").unwrap();
        let rec = |judge: Value| serde_json::json!({"_norm": {"actor": "alice", "kind": "CreateUser"}, "judge": judge});
        assert_eq!(render(&cfg.text, &rec(serde_json::json!({"flag": true, "why": "new admin"}))), "alice CreateUser: new admin");
        assert_eq!(
            render(&cfg.text, &rec(serde_json::json!({"error": "model: no answer in 30s"}))),
            "alice CreateUser: not judged: model: no answer in 30s"
        );
        assert_eq!(render(&cfg.text, &rec(serde_json::json!({}))), "alice CreateUser: -");
        // The first alternative the record has wins; words go with theirs.
        let v = serde_json::json!({"a": null, "b": "two", "c": "three"});
        assert_eq!(render("{a|b|c}", &v), "two");
        assert_eq!(render("{x|why: b}", &v), "why: two");
    }

    #[tokio::test]
    async fn each_record_is_posted_as_one_message_without_unfurling() {
        let (url, bodies) = webhook(OK).await;
        let sink = SlackSink::start(poster(&url, DEADLINE), "{_norm.actor}: {judge.why}".into(), 10, FLUSH);
        let lines = [
            serde_json::json!({"_norm": {"actor": "alice"}, "judge": {"why": "one"}}),
            serde_json::json!({"_norm": {"actor": "bob"}, "judge": {"why": "two"}}),
        ];
        sink.write(payload(&lines)).await.unwrap();
        sink.flush().await.unwrap();
        assert_eq!(
            *bodies.lock().await,
            [
                r#"{"text":"alice: one","unfurl_links":false,"unfurl_media":false}"#,
                r#"{"text":"bob: two","unfurl_links":false,"unfurl_media":false}"#
            ]
        );
    }

    #[tokio::test]
    async fn a_refused_message_is_dropped_at_once_with_slack_s_answer() {
        let (url, bodies) = webhook("HTTP/1.1 404 Not Found\r\ncontent-length: 10\r\nconnection: close\r\n\r\nno_service").await;
        let d = poster(&url, DEADLINE).post("x").await.unwrap_err();
        assert_eq!(d.reason, "refused");
        assert_eq!(d.why, "404 Not Found no_service");
        assert_eq!(bodies.lock().await.len(), 1);

        let before = dropped("refused");
        let sink = SlackSink::start(poster(&url, DEADLINE), "{x}".into(), 10, FLUSH);
        sink.write(payload(&[serde_json::json!({"x": 1})])).await.unwrap();
        sink.flush().await.unwrap();
        assert!(dropped("refused") > before);
    }

    #[tokio::test]
    async fn a_wait_past_the_deadline_is_not_waited() {
        let (url, bodies) = webhook("HTTP/1.1 503 Service Unavailable\r\nretry-after: 60\r\ncontent-length: 0\r\nconnection: close\r\n\r\n").await;
        let start = Instant::now();
        let d = poster(&url, Duration::from_secs(5)).post("x").await.unwrap_err();
        assert_eq!(d.reason, "unavailable");
        assert!(d.why.starts_with("503"), "{}", d.why);
        assert!(start.elapsed() < Duration::from_secs(2));
        assert_eq!(bodies.lock().await.len(), 1);
    }

    #[tokio::test]
    async fn a_failed_request_does_not_name_the_webhook() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/services/T0/B0/SECRET", listener.local_addr().unwrap());
        drop(listener);
        let d = poster(&url, Duration::from_millis(500)).post("x").await.unwrap_err();
        assert_eq!(d.reason, "unavailable");
        assert!(!d.why.contains("SECRET") && !d.why.contains("/services/"), "{}", d.why);
    }

    #[tokio::test]
    async fn a_hung_slack_holds_no_write() {
        let (url, _) = webhook("").await;
        let d = poster(&url, Duration::from_millis(300)).post("x").await.unwrap_err();
        assert_eq!((d.reason, d.why.as_str()), ("unavailable", "no answer in time"));

        // Room for two: the write returns at once, the other three are
        // dropped, and shutdown gives up on what is still queued.
        let (full, shut) = (dropped("queue_full"), dropped("shutdown"));
        let sink = SlackSink::start(poster(&url, DEADLINE), "{x}".into(), 2, Duration::from_millis(200));
        let start = Instant::now();
        let lines: Vec<Value> = (0..5).map(|i| serde_json::json!({ "x": i })).collect();
        sink.write(payload(&lines)).await.unwrap();
        assert!(start.elapsed() < Duration::from_millis(100));
        assert!(dropped("queue_full") >= full + 3);
        sink.flush().await.unwrap();
        assert!(start.elapsed() < Duration::from_secs(2));
        assert!(dropped("shutdown") >= shut + 2);
    }

    #[test]
    fn a_webhook_is_https() {
        let cfg = SlackConfig { webhook_url: "http://hooks.slack.com/x".into(), text: "t".into() };
        assert!(SlackSink::new(&cfg).is_err());
    }
}
