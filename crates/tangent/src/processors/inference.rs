//! Ask a model about each record. The model is reached through rig: any
//! OpenAI-compatible chat endpoint, or Amazon Bedrock's Converse API. The
//! system prompt is a file; the user message is the record (or selected
//! fields) as pretty JSON; the JSON object in the reply is added to the
//! record. Requests run with bounded concurrency off the hot path. A failure,
//! or a reply without the `forward_if` field, passes the record through with
//! an error note or drops it, as configured; nothing is lost silently.

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
use std::sync::Arc;
use std::time::{Duration, Instant};
use tangent_shared::dag::NodeRef;
use tangent_shared::processors::inference::{Answer, Backend, InferenceConfig, OnError};
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

    async fn ask(&self, user: String) -> Result<(Value, bool)> {
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
            Answer::Json => {
                let parsed = parse_reply(&text);
                if let Some(path) = &self.cfg.forward_if {
                    if lookup(&parsed, path).is_none() {
                        bail!("reply has no {path:?}: {}", head(&text, 200));
                    }
                }
                let forward = self.forwards(&parsed);
                Ok((parsed, forward))
            }
            Answer::YesNo => {
                let Some(yes) = yes_no(&text) else {
                    bail!("reply is not yes or no: {}", head(&text, 80));
                };
                Ok((json!({"answer": if yes { "yes" } else { "no" }}), yes))
            }
        }
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
                Ok(decided) => decided,
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
    let ctx = Arc::new(Ctx {
        from: NodeRef::Processor {
            name: Arc::clone(&name),
        },
        name,
        cfg,
        prompt,
        model,
        router,
    });
    tracing::info!(
        processor = %ctx.name,
        backend = ?ctx.cfg.backend,
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
