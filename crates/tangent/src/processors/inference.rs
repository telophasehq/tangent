//! Ask a model about each record through an OpenAI-compatible chat endpoint.
//! The system prompt is a file; the user message is the record (or selected
//! fields) as pretty JSON; the JSON object in the reply is added to the
//! record. Requests run with bounded concurrency off the hot path. A failure,
//! or a reply without the `forward_if` field, passes the record through with
//! an error note or drops it, as configured; nothing is lost silently.

use anyhow::{anyhow, bail, Context, Result};
use bytes::BytesMut;
use futures::FutureExt;
use serde_json::{json, Map, Value};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tangent_shared::dag::NodeRef;
use tangent_shared::processors::inference::{InferenceConfig, OnError};
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinSet;

use crate::processors::{lines, lookup};
use crate::router::Router;
use crate::worker::Record;

struct Ctx {
    name: Arc<str>,
    from: NodeRef,
    cfg: InferenceConfig,
    prompt: String,
    client: reqwest::Client,
    router: Arc<Router>,
}

impl Ctx {
    fn user_message(&self, v: &Value) -> String {
        if self.cfg.fields.is_empty() {
            return serde_json::to_string_pretty(v).unwrap_or_default();
        }
        let mut m = Map::new();
        for f in &self.cfg.fields {
            if let Some(x) = lookup(v, f) {
                m.insert(f.clone(), x.clone());
            }
        }
        serde_json::to_string_pretty(&Value::Object(m)).unwrap_or_default()
    }

    async fn ask(&self, user: String) -> Result<Value> {
        let mut body = json!({
            "model": self.cfg.model,
            "messages": [
                {"role": "system", "content": self.prompt},
                {"role": "user", "content": user},
            ],
            "temperature": self.cfg.temperature,
            "max_tokens": self.cfg.max_tokens,
        });
        if let Value::Object(b) = &mut body {
            for (k, v) in &self.cfg.extra {
                b.insert(k.clone(), v.clone());
            }
        }
        let url = format!("{}/chat/completions", self.cfg.url.trim_end_matches('/'));
        let mut req = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .body(serde_json::to_vec(&body)?);
        if let Some(key) = &self.cfg.api_key {
            req = req.bearer_auth(key);
        }
        let resp = req.send().await.with_context(|| format!("POST {url}"))?;
        let status = resp.status();
        let text = resp.text().await.context("reading reply")?;
        if !status.is_success() {
            bail!("HTTP {status}: {}", head(&text, 200));
        }
        let v: Value = serde_json::from_str(&text)
            .with_context(|| format!("reply is not JSON: {}", head(&text, 200)))?;
        if let Some(e) = v.get("error") {
            bail!("server error: {e}");
        }
        let content = v["choices"][0]["message"]["content"]
            .as_str()
            .ok_or_else(|| {
                let finish = v["choices"][0]["finish_reason"].as_str().unwrap_or("?");
                anyhow!("reply has no content (finish_reason={finish}); the model may have spent its budget thinking")
            })?;
        let reply = parse_reply(content);
        if let Some(path) = &self.cfg.forward_if {
            if lookup(&reply, path).is_none() {
                bail!("reply has no {path:?}: {}", head(content, 200));
            }
        }
        Ok(reply)
    }

    /// Does this reply let the record through?
    fn forwards(&self, reply: &Value) -> bool {
        match &self.cfg.forward_if {
            None => true,
            Some(path) => lookup(reply, path).is_some_and(truthy),
        }
    }

    async fn handle(&self, rec: Record) -> Result<()> {
        let mut out: Vec<BytesMut> = Vec::new();
        let payload = rec.payload.into_bytes();
        for line in lines(&payload) {
            let Ok(v) = serde_json::from_slice::<Value>(line) else {
                tracing::warn!(processor = %self.name, "record is not JSON; dropping");
                continue;
            };
            let start = Instant::now();
            let (reply, forward) = match self.ask(self.user_message(&v)).await {
                Ok(reply) => {
                    let forward = self.forwards(&reply);
                    (reply, forward)
                }
                Err(e) => {
                    tracing::warn!(processor = %self.name, "inference failed: {e:#}");
                    match self.cfg.on_error {
                        OnError::Drop => continue,
                        OnError::Pass => (json!({"error": format!("{e:#}")}), true),
                    }
                }
            };
            tracing::debug!(
                processor = %self.name,
                ms = start.elapsed().as_millis() as u64,
                forward,
                "inference"
            );
            if !forward {
                continue;
            }
            let mut obj = match v {
                Value::Object(m) => m,
                other => {
                    let mut m = Map::new();
                    m.insert("value".into(), other);
                    m
                }
            };
            obj.insert(self.cfg.output.clone(), reply);
            let mut bytes = serde_json::to_vec(&Value::Object(obj))?;
            bytes.push(b'\n');
            out.push(BytesMut::from(&bytes[..]));
        }
        if out.is_empty() {
            if let Some(a) = rec.ack {
                let _ = a.ack().await;
            }
        } else {
            self.router
                .forward(&self.from, out, rec.ack.into_iter().collect())
                .await?;
        }
        Ok(())
    }
}

fn truthy(v: &Value) -> bool {
    match v {
        Value::Bool(b) => *b,
        Value::String(s) => matches!(
            s.trim().to_ascii_lowercase().as_str(),
            "true" | "yes" | "y" | "1"
        ),
        Value::Number(n) => n.as_f64().unwrap_or(0.0) != 0.0,
        Value::Null => false,
        Value::Array(a) => !a.is_empty(),
        Value::Object(o) => !o.is_empty(),
    }
}

fn head(s: &str, n: usize) -> String {
    let mut t: String = s.chars().take(n).collect();
    if t.len() < s.len() {
        t.push('…');
    }
    t
}

/// The first JSON object in the reply, after any <think>…</think> or
/// <reasoning>…</reasoning> block and inside any code fence; a reply with no
/// object becomes `{"text": ...}`.
pub fn parse_reply(content: &str) -> Value {
    // Reasoning models think out loud first; the answer comes after the tag.
    let s = ["</think>", "</reasoning>"]
        .iter()
        .filter_map(|tag| content.rfind(tag).map(|i| i + tag.len()))
        .max()
        .map_or(content, |i| &content[i..]);
    if let Some(start) = s.find('{') {
        if let Some(obj) = balanced_object(&s[start..]) {
            if let Ok(v @ Value::Object(_)) = serde_json::from_str::<Value>(obj) {
                return v;
            }
        }
    }
    json!({ "text": s.trim() })
}

/// `s` starts at `{`; the slice up to its matching `}`, string-aware.
fn balanced_object(s: &str) -> Option<&str> {
    let (mut depth, mut in_str, mut esc) = (0i32, false, false);
    for (i, c) in s.char_indices() {
        if in_str {
            if esc {
                esc = false;
            } else if c == '\\' {
                esc = true;
            } else if c == '"' {
                in_str = false;
            }
            continue;
        }
        match c {
            '"' => in_str = true,
            '{' => depth += 1,
            '}' => {
                depth -= 1;
                if depth == 0 {
                    return Some(&s[..i + c.len_utf8()]);
                }
            }
            _ => {}
        }
    }
    None
}

pub async fn run(
    name: Arc<str>,
    cfg: InferenceConfig,
    config_dir: &Path,
    mut rx: mpsc::Receiver<Record>,
    router: Arc<Router>,
) -> Result<()> {
    let prompt_path = if cfg.prompt_file.is_absolute() {
        cfg.prompt_file.clone()
    } else {
        config_dir.join(&cfg.prompt_file)
    };
    let prompt = std::fs::read_to_string(&prompt_path)
        .with_context(|| format!("reading prompt {}", prompt_path.display()))?;
    if prompt.trim().is_empty() {
        bail!("prompt {} is empty", prompt_path.display());
    }
    let client = reqwest::Client::builder()
        .timeout(Duration::from_millis(cfg.timeout_ms))
        .build()?;
    let concurrency = cfg.concurrency.max(1);
    let ctx = Arc::new(Ctx {
        from: NodeRef::Processor {
            name: Arc::clone(&name),
        },
        name,
        cfg,
        prompt,
        client,
        router,
    });
    tracing::info!(
        processor = %ctx.name,
        url = %ctx.cfg.url,
        model = %ctx.cfg.model,
        forward_if = ctx.cfg.forward_if.as_deref().unwrap_or("-"),
        "inference ready"
    );

    let sem = Arc::new(Semaphore::new(concurrency));
    let mut js: JoinSet<()> = JoinSet::new();
    while let Some(rec) = rx.recv().await {
        // Reap finished requests without blocking on them.
        while let Some(Some(res)) = js.join_next().now_or_never() {
            if let Err(e) = res {
                tracing::warn!(processor = %ctx.name, "inference task failed: {e}");
            }
        }
        let permit = sem.clone().acquire_owned().await?;
        let ctx = Arc::clone(&ctx);
        js.spawn(async move {
            let _permit = permit;
            if let Err(e) = ctx.handle(rec).await {
                tracing::warn!(processor = %ctx.name, "forwarding failed: {e:#}");
            }
        });
    }
    // Input closed: finish what is in flight before the sinks shut down.
    while let Some(res) = js.join_next().await {
        if let Err(e) = res {
            tracing::warn!(processor = %ctx.name, "inference task failed: {e}");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn replies_parse() {
        let v = parse_reply(
            "<think>hmm</think>\nHere:\n```json\n{\"flag\": false, \"why\": \"has {braces}\"}\n```",
        );
        assert_eq!(v["flag"], false);
        assert_eq!(v["why"], "has {braces}");
        assert_eq!(parse_reply("no json at all")["text"], "no json at all");
        assert_eq!(parse_reply("{\"a\": ")["text"], "{\"a\":");
        assert_eq!(parse_reply("[1, 2]")["text"], "[1, 2]");
        // gpt-oss style: the reasoning quotes the input before the answer.
        let v = parse_reply(
            "<reasoning>The input {\"event\": \"x\"} looks fine.</reasoning>{\"flag\": false}",
        );
        assert_eq!(v["flag"], false);
        assert!(v.get("event").is_none(), "{v}");
    }

    #[test]
    fn truthiness_follows_what_models_actually_write() {
        for yes in [
            json!(true),
            json!("yes"),
            json!("True"),
            json!(1),
            json!({"a": 1}),
        ] {
            assert!(truthy(&yes), "{yes}");
        }
        for no in [
            json!(false),
            json!("no"),
            json!(0),
            json!(null),
            json!([]),
            json!(""),
        ] {
            assert!(!truthy(&no), "{no}");
        }
    }
}
