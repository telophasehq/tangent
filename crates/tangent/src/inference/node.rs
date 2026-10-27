//! Ask a model about each record. The model is reached through rig: any
//! OpenAI-compatible chat endpoint, or Amazon Bedrock's Converse API. The
//! system prompt is a file; the user message is the record (or selected
//! fields) as pretty JSON; the reply is added to the record under `_<name>`.
//! Requests run with bounded concurrency off the hot path. The node adds and
//! forwards, never drops: a failure lands as `{"error": ...}` in the same
//! field, and a plugin downstream decides what goes on.

use anyhow::{anyhow, bail, Context, Result};
use bytes::BytesMut;
use futures::FutureExt;
use once_cell::sync::Lazy;
use regex::Regex;
use rig_core::completion::request::CompletionRequest;
use rig_core::completion::AssistantContent;
use rig_core::operation::Completion;
use rig_core::providers::openai::{OpenAIConfig, Route};
use rig_core::DynModel;
use serde_json::{json, Map, Value};
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tangent_shared::dag::NodeRef;
use tangent_shared::inference::{Answer, Backend, InferenceConfig};
use tokio::sync::{mpsc, Semaphore};
use tokio::task::JoinSet;

use super::{lines, lookup};
use crate::router::Router;
use crate::worker::Record;

struct Ctx {
    name: Arc<str>,
    /// Where the reply lands on the record.
    output: String,
    from: NodeRef,
    cfg: InferenceConfig,
    prompt: String,
    model: DynModel<Completion>,
    router: Arc<Router>,
}

/// The model `cfg` points at, behind rig's erased handle.
fn model(cfg: &InferenceConfig) -> Result<DynModel<Completion>> {
    Ok(match cfg.backend {
        Backend::Openai => {
            // Local servers accept any key; a hosted one needs the real thing.
            let key = cfg.api_key.clone().unwrap_or_else(|| "none".into());
            OpenAIConfig::new(key)
                .with_base_url(cfg.url.trim_end_matches('/'))
                .with_route(Route::Chat)
                .client()
                .chat(&cfg.model)
                .erase()
        }
        Backend::Bedrock => {
            let mut runtime = rig_bedrock::client::BedrockRuntime::builder();
            if let Some(region) = &cfg.region {
                runtime = runtime.region(region);
            }
            if let Some(profile) = &cfg.profile {
                runtime = runtime.profile_name(profile);
            }
            runtime.build().completion(&cfg.model).erase()
        }
    })
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
        let mut request = CompletionRequest::new(user)
            .preamble(self.prompt.clone())
            .temperature(self.cfg.temperature)
            .max_tokens(u64::from(self.cfg.max_tokens));
        if !self.cfg.extra.is_empty() {
            request = request.additional_params(Value::Object(self.cfg.extra.clone()));
        }
        let reply = tokio::time::timeout(
            Duration::from_millis(self.cfg.timeout_ms),
            self.model.call(request),
        )
        .await
        .map_err(|_| anyhow!("no reply within {} ms", self.cfg.timeout_ms))?
        .map_err(|e| anyhow!("{e}"))?;

        // Reasoning comes back as its own block, so the text is the answer.
        let text: String = reply
            .choice
            .iter()
            .filter_map(|c| match c {
                AssistantContent::Text(t) => Some(t.text.as_str()),
                _ => None,
            })
            .collect::<Vec<_>>()
            .join("");
        if text.trim().is_empty() {
            bail!(
                "reply has no text (finish: {:?}; the model may have spent its budget thinking)",
                reply.finish_reason()
            );
        }
        match self.cfg.answer {
            Answer::Json => Ok(parse_reply(&text)),
            Answer::YesNo => match yes_no(&text) {
                Some(yes) => Ok(json!({"answer": if yes { "yes" } else { "no" }})),
                None => bail!("reply is not yes or no: {}", head(&text, 80)),
            },
        }
    }

    /// One record through the model: the reply, or the error, added under
    /// `output`.
    async fn annotate(&self, v: Value) -> Result<BytesMut> {
        let start = Instant::now();
        let reply = match self.ask(self.user_message(&v)).await {
            Ok(reply) => reply,
            Err(e) => {
                tracing::warn!(node = %self.name, "inference failed: {e:#}");
                json!({"error": format!("{e:#}")})
            }
        };
        tracing::debug!(
            node = %self.name,
            ms = start.elapsed().as_millis() as u64,
            "inference"
        );
        let mut obj = match v {
            Value::Object(m) => m,
            other => {
                let mut m = Map::new();
                m.insert("value".into(), other);
                m
            }
        };
        obj.insert(self.output.clone(), reply);
        let mut bytes = serde_json::to_vec(&Value::Object(obj))?;
        bytes.push(b'\n');
        Ok(BytesMut::from(&bytes[..]))
    }

    /// A frame through the model. A frame from a plugin holds a whole batch,
    /// so every line is in flight at once, each under one of the node's
    /// `concurrency` permits, and the frame goes on as one unit when the last
    /// reply is in.
    async fn handle(&self, rec: Record, sem: Arc<Semaphore>) -> Result<()> {
        let payload = rec.payload.into_bytes();
        let mut records = Vec::new();
        for line in lines(&payload) {
            match serde_json::from_slice::<Value>(line) {
                Ok(v) => records.push(v),
                Err(_) => tracing::warn!(node = %self.name, "record is not JSON; dropping"),
            }
        }
        let replies = futures::future::join_all(records.into_iter().map(|v| {
            let sem = Arc::clone(&sem);
            async move {
                let _permit = sem
                    .acquire_owned()
                    .await
                    .map_err(|_| anyhow!("inference node closed"))?;
                self.annotate(v).await
            }
        }))
        .await;
        let mut out: Vec<BytesMut> = Vec::with_capacity(replies.len());
        for r in replies {
            out.push(r?);
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
    let s = answer_part(content);
    if let Some(start) = s.find('{') {
        if let Some(obj) = balanced_object(&s[start..]) {
            if let Ok(v @ Value::Object(_)) = serde_json::from_str::<Value>(obj) {
                return v;
            }
            if let Some(v) = salvage(obj) {
                return v;
            }
        }
    }
    json!({ "text": s.trim() })
}

/// The answer: whatever follows the last reasoning block, if any. Reasoning
/// models think out loud first; the answer comes after the tag (gpt-oss in
/// harmony markup: after `<|channel|>final<|message|>`).
fn answer_part(content: &str) -> &str {
    ["</think>", "</reasoning>", "<|channel|>final<|message|>"]
        .iter()
        .filter_map(|tag| content.rfind(tag).map(|i| i + tag.len()))
        .max()
        .map_or(content, |i| &content[i..])
}

/// A one-word verdict: the first word of the answer, however it is dressed
/// ("Yes.", "**no**", "YES, look at it"). None when it is neither.
pub fn yes_no(content: &str) -> Option<bool> {
    let word: String = answer_part(content)
        .trim_start_matches(|c: char| !c.is_alphanumeric())
        .chars()
        .take_while(|c| c.is_alphanumeric())
        .collect::<String>()
        .to_ascii_lowercase();
    match word.as_str() {
        "yes" | "y" | "true" | "1" => Some(true),
        "no" | "n" | "false" | "0" => Some(false),
        _ => None,
    }
}

/// A small model drops a key name or a quote now and then. Keep every
/// well-formed `"key": value` pair, which is usually the verdict, and the
/// raw text for whoever reads the record.
fn salvage(obj: &str) -> Option<Value> {
    static PAIR: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r#""([A-Za-z_][A-Za-z0-9_]*)"\s*:\s*(true|false|null|-?\d+(?:\.\d+)?|"(?:[^"\\]|\\.)*")"#)
            .expect("salvage pattern")
    });
    let mut m = Map::new();
    for cap in PAIR.captures_iter(obj) {
        let value = serde_json::from_str::<Value>(&cap[2]).unwrap_or(Value::Null);
        m.entry(cap[1].to_string()).or_insert(value);
    }
    if m.is_empty() {
        return None;
    }
    m.entry("text".to_string())
        .or_insert_with(|| Value::String(obj.trim().to_string()));
    Some(Value::Object(m))
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
    in_flight: Arc<AtomicUsize>,
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
    let model = model(&cfg)?;
    let concurrency = cfg.concurrency.max(1);
    let output = cfg.output.clone().unwrap_or_else(|| format!("_{name}"));
    let ctx = Arc::new(Ctx {
        from: NodeRef::Inference {
            name: Arc::clone(&name),
        },
        name,
        output,
        cfg,
        prompt,
        model,
        router,
    });
    tracing::info!(
        node = %ctx.name,
        backend = ?ctx.cfg.backend,
        model = %ctx.cfg.model,
        "inference ready"
    );

    let sem = Arc::new(Semaphore::new(concurrency));
    let mut js: JoinSet<()> = JoinSet::new();
    while let Some(rec) = rx.recv().await {
        // Reap finished frames without blocking on them.
        while let Some(Some(res)) = js.join_next().now_or_never() {
            if let Err(e) = res {
                tracing::warn!(node = %ctx.name, "inference task failed: {e}");
            }
        }
        let (ctx, sem, in_flight) = (Arc::clone(&ctx), Arc::clone(&sem), Arc::clone(&in_flight));
        js.spawn(async move {
            if let Err(e) = ctx.handle(rec, sem).await {
                tracing::warn!(node = %ctx.name, "forwarding failed: {e:#}");
            }
            // Forwarded (or lost): no longer in the DAG on this node's account.
            in_flight.fetch_sub(1, Ordering::AcqRel);
        });
    }
    // Input closed: finish what is in flight before the sinks shut down.
    while let Some(res) = js.join_next().await {
        if let Err(e) = res {
            tracing::warn!(node = %ctx.name, "inference task failed: {e}");
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
        // A small model dropped a key name: the well-formed pairs survive.
        let v = parse_reply(
            r#"{"flag": true, "rotate_iam_key sent 48KB to transfer.sh", "why": "a generic file host"}"#,
        );
        assert_eq!(v["flag"], true);
        assert_eq!(v["why"], "a generic file host");
        assert!(v["text"].as_str().unwrap().contains("48KB"));
        assert_eq!(
            parse_reply("{not json at all}")["text"],
            "{not json at all}"
        );
        // gpt-oss through mlx: harmony channels inline, the answer in `final`.
        let v = parse_reply(
            "<|channel|>analysis<|message|>Input {\"x\": 1}; routine.<|end|><|start|>assistant<|channel|>final<|message|>{\"flag\": false}<|return|>",
        );
        assert_eq!(v["flag"], false);
        assert!(v.get("x").is_none(), "{v}");
    }

    #[test]
    fn one_word_verdicts() {
        for yes in [
            "yes",
            "Yes.",
            "**YES**",
            "YES, look at it",
            "<think>hmm</think>\nyes",
            "<|channel|>analysis<|message|>Odd destination.<|end|><|start|>assistant<|channel|>final<|message|>yes",
            "y",
        ] {
            assert_eq!(yes_no(yes), Some(true), "{yes:?}");
        }
        for no in ["no", "No\n", "**no**", "n", "No, routine"] {
            assert_eq!(yes_no(no), Some(false), "{no:?}");
        }
        for neither in ["maybe", "", "{\"flag\": true}", "yesterday"] {
            assert_eq!(yes_no(neither), None, "{neither:?}");
        }
    }
}
