//! Inference nodes: a model reads each record. A node lives inside each
//! worker and runs in DAG order with the plugins: the worker hands it the
//! records that reached it, waits for the replies (`concurrency` at a time),
//! and carries the annotated records on to the node's outputs. The system
//! prompt is a file; the user message is the record (or selected fields) as
//! pretty JSON; the reply is added to the record under the node's name. The
//! node adds and forwards, never drops: a failure lands as `{"error": ...}`
//! in the same field, and a plugin downstream decides what goes on.
//!
//! The reply is read as JSON and nothing else. The backend is asked to make
//! it so: a schema (`schema_file`) through rig's structured output, or JSON
//! mode on an OpenAI-compatible server. A server that ignores both (mlx_lm)
//! is held to the prompt, and a reply that is not a JSON object is the error.

use anyhow::{anyhow, bail, Context, Result};
use rig_core::completion::request::CompletionRequest;
use rig_core::completion::AssistantContent;
use rig_core::operation::Completion;
use rig_core::providers::openai::{OpenAIConfig, Route};
use rig_core::{schemars, DynModel};
use serde_json::{json, Map, Value};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tangent_shared::inference::{Backend, InferenceConfig};
use tokio::sync::Semaphore;

pub struct InferenceNode {
    pub name: Arc<str>,
    cfg: InferenceConfig,
    prompt: String,
    /// The shape the backend is asked to enforce, from `schema_file`.
    schema: Option<schemars::Schema>,
    model: DynModel<Completion>,
    /// `concurrency` permits: requests in flight from this worker.
    sem: Semaphore,
}

fn resolve(config_dir: &Path, file: &Path) -> PathBuf {
    if file.is_absolute() {
        file.to_path_buf()
    } else {
        config_dir.join(file)
    }
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
            if let Some(region) = &cfg.aws.region {
                runtime = runtime.region(region);
            }
            if let Some(profile) = &cfg.aws.profile {
                runtime = runtime.profile_name(profile);
            }
            runtime.build().completion(&cfg.model).erase()
        }
    })
}

impl InferenceNode {
    pub fn new(name: Arc<str>, cfg: InferenceConfig, config_dir: &Path) -> Result<Self> {
        let prompt_path = resolve(config_dir, &cfg.prompt_file);
        let prompt = std::fs::read_to_string(&prompt_path)
            .with_context(|| format!("reading prompt {}", prompt_path.display()))?;
        if prompt.trim().is_empty() {
            bail!("prompt {} is empty", prompt_path.display());
        }
        let schema = match &cfg.schema_file {
            Some(file) => {
                let path = resolve(config_dir, file);
                let text = std::fs::read_to_string(&path)
                    .with_context(|| format!("reading schema {}", path.display()))?;
                let value: Value = serde_json::from_str(&text)
                    .with_context(|| format!("schema {} is not JSON", path.display()))?;
                Some(
                    schemars::Schema::try_from(value)
                        .map_err(|e| anyhow!("schema {}: {e}", path.display()))?,
                )
            }
            None => None,
        };
        let model = model(&cfg)?;
        let sem = Semaphore::new(cfg.concurrency.max(1));
        tracing::info!(
            node = %name,
            backend = ?cfg.backend,
            model = %cfg.model,
            "inference ready"
        );
        Ok(Self {
            name,
            cfg,
            prompt,
            schema,
            model,
            sem,
        })
    }

    /// Every record through the model, under the node's permits: one request
    /// per record, or per `batch` of them; the annotated records, in the
    /// order given.
    pub async fn annotate_all(&self, records: Vec<Value>) -> Vec<Value> {
        if self.cfg.batch <= 1 {
            return futures::future::join_all(records.into_iter().map(|v| async move {
                let _permit = self.sem.acquire().await.expect("semaphore is never closed");
                self.annotate(v).await
            }))
            .await;
        }
        let answered = futures::future::join_all(self.chunks(records).into_iter().map(|chunk| async move {
            let _permit = self.sem.acquire().await.expect("semaphore is never closed");
            self.annotate_batch(chunk).await
        }))
        .await;
        answered.into_iter().flatten().collect()
    }

    /// One record through the model: the reply, or the error, added under the
    /// node's name.
    pub async fn annotate(&self, v: Value) -> Value {
        let start = Instant::now();
        let reply = match self.ask(self.user_message(&v), None).await {
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
        attach(&self.name, v, reply)
    }

    /// Several records through the model in one request: each gets its
    /// answer, or the error, under the node's name, in the order given.
    async fn annotate_batch(&self, records: Vec<Value>) -> Vec<Value> {
        let n = records.len();
        let start = Instant::now();
        let user = records
            .iter()
            .enumerate()
            .map(|(k, v)| self.batch_line(k, v))
            .collect::<Vec<_>>()
            .join("\n");
        let answers = match self.ask(user, Some(n)).await {
            Ok(reply) => spread(reply, n),
            Err(e) => {
                tracing::warn!(node = %self.name, records = n, "inference failed: {e:#}");
                vec![json!({"error": format!("{e:#}")}); n]
            }
        };
        tracing::debug!(
            node = %self.name,
            records = n,
            ms = start.elapsed().as_millis() as u64,
            "inference"
        );
        records
            .into_iter()
            .zip(answers)
            .map(|(v, a)| attach(&self.name, v, a))
            .collect()
    }

    /// Records in request-sized runs: at most `batch` of them and about
    /// `batch_bytes` of JSON each; a record larger than that goes alone.
    fn chunks(&self, records: Vec<Value>) -> Vec<Vec<Value>> {
        let (max_n, max_bytes) = (self.cfg.batch.max(1), self.cfg.batch_bytes.max(1));
        let mut out = Vec::new();
        let mut cur: Vec<Value> = Vec::new();
        let mut bytes = 0usize;
        for v in records {
            let size = self.view(&v).to_string().len();
            if !cur.is_empty() && (cur.len() >= max_n || bytes + size > max_bytes) {
                out.push(std::mem::take(&mut cur));
                bytes = 0;
            }
            bytes += size;
            cur.push(v);
        }
        if !cur.is_empty() {
            out.push(cur);
        }
        out
    }

    /// The record as the model sees it: whole, or the listed `fields`.
    fn view(&self, v: &Value) -> Value {
        if self.cfg.fields.is_empty() {
            return v.clone();
        }
        let mut m = Map::new();
        for f in &self.cfg.fields {
            if let Some(x) = lookup(v, f) {
                m.insert(f.clone(), x.clone());
            }
        }
        Value::Object(m)
    }

    fn user_message(&self, v: &Value) -> String {
        serde_json::to_string_pretty(&self.view(v)).unwrap_or_default()
    }

    /// One line of a batched request: the record's view with its index first.
    fn batch_line(&self, k: usize, v: &Value) -> String {
        let mut m = Map::new();
        m.insert("i".into(), json!(k));
        if let Value::Object(fields) = self.view(v) {
            m.extend(fields);
        }
        serde_json::to_string(&Value::Object(m)).unwrap_or_default()
    }

    /// The request for one record (`batch` is None) or for a batch of
    /// `n` records: the same prompt, plus how to answer them all at once; the
    /// schema, if any, wrapped around each answer.
    fn request(&self, user: String, batch: Option<usize>) -> CompletionRequest {
        let preamble = match batch {
            None => self.prompt.clone(),
            Some(n) => format!(
                "{}\n\nThis request carries {n} records, one JSON object per line, each with its index `i` (0 to {}). \
                 Reply with one JSON object, {{\"answers\": [...]}}, holding exactly {n} answers in the same order: \
                 answers[k] is your answer for the record with i = k, shaped as described above, and nothing else.",
                self.prompt.trim_end(),
                n.saturating_sub(1)
            ),
        };
        // `max_tokens` is per record: a batch of n may say n times as much.
        let budget = u64::from(self.cfg.max_tokens) * batch.unwrap_or(1) as u64;
        let mut request = CompletionRequest::new(user)
            .preamble(preamble)
            .max_tokens(budget);
        if let Some(t) = self.cfg.temperature {
            request = request.temperature(t);
        }
        let mut params = Map::new();
        match (&self.schema, self.cfg.backend) {
            (Some(schema), _) => {
                let schema = match batch {
                    None => schema.clone(),
                    Some(n) => answers_schema(schema, n),
                };
                request = request.output_schema(schema);
            }
            // JSON mode: any object. Converse has no such mode without a schema.
            (None, Backend::Openai) => {
                params.insert("response_format".into(), json!({"type": "json_object"}));
            }
            (None, Backend::Bedrock) => {}
        }
        for (k, v) in &self.cfg.extra {
            params.insert(k.clone(), v.clone());
        }
        if !params.is_empty() {
            request = request.additional_params(Value::Object(params));
        }
        request
    }

    /// The reply as a JSON object, after `retries` more tries for a request
    /// that got no reply.
    async fn ask(&self, user: String, batch: Option<usize>) -> Result<Value> {
        let attempts = self.cfg.retries.saturating_add(1);
        let mut attempt = 0;
        let text = loop {
            attempt += 1;
            match self.call(user.clone(), batch).await {
                Ok(text) => break text,
                Err(e) if attempt < attempts => {
                    let wait = Duration::from_secs(1 << (attempt - 1).min(6));
                    tracing::warn!(
                        node = %self.name,
                        attempt,
                        "inference failed, retrying in {wait:?}: {e:#}"
                    );
                    tokio::time::sleep(wait).await;
                }
                Err(e) => return Err(e),
            }
        };
        let value: Value = serde_json::from_str(text.trim())
            .map_err(|e| anyhow!("reply is not JSON ({e}): {}", head(&text, 200)))?;
        if !value.is_object() {
            bail!("reply is not a JSON object: {}", head(&text, 200));
        }
        Ok(value)
    }

    /// One request: the reply's text, or why there is none. How the model
    /// stopped (`stop`, `length`) is logged, since a reply cut short reads as
    /// broken JSON and the reason matters.
    async fn call(&self, user: String, batch: Option<usize>) -> Result<String> {
        let reply = tokio::time::timeout(
            Duration::from_millis(self.cfg.timeout_ms),
            self.model.call(self.request(user, batch)),
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
        tracing::debug!(
            node = %self.name,
            finish = ?reply.finish_reason(),
            chars = text.len(),
            "reply"
        );
        if let Some(finish) = reply.finish_reason() {
            if format!("{finish:?}").to_ascii_lowercase().contains("length") {
                tracing::warn!(
                    node = %self.name,
                    chars = text.len(),
                    "reply cut off at the token budget (max_tokens {} per record)",
                    self.cfg.max_tokens
                );
            }
        }
        Ok(text)
    }
}

/// The reply, or the error, under the node's name on the record.
fn attach(name: &str, v: Value, reply: Value) -> Value {
    let mut obj = match v {
        Value::Object(m) => m,
        other => {
            let mut m = Map::new();
            m.insert("value".into(), other);
            m
        }
    };
    obj.insert(name.to_string(), reply);
    Value::Object(obj)
}

/// A batched reply's `answers`, one per record: by each answer's `i` when
/// they all carry one, else by position. A record whose answer is missing or
/// not an object gets the error instead.
fn spread(reply: Value, n: usize) -> Vec<Value> {
    let Some(items) = reply.get("answers").and_then(Value::as_array).cloned() else {
        let err = format!("reply has no `answers` array: {}", head(&reply.to_string(), 200));
        return vec![json!({"error": err}); n];
    };
    let indexed = !items.is_empty()
        && items
            .iter()
            .all(|a| a.get("i").and_then(Value::as_u64).is_some());
    let mut slots: Vec<Option<Value>> = vec![None; n];
    for (pos, a) in items.into_iter().enumerate() {
        let k = if indexed {
            a["i"].as_u64().unwrap_or(u64::MAX) as usize
        } else {
            pos
        };
        if k < n && slots[k].is_none() {
            slots[k] = Some(a);
        }
    }
    slots
        .into_iter()
        .enumerate()
        .map(|(k, slot)| match slot {
            Some(Value::Object(mut m)) => {
                m.remove("i");
                Value::Object(m)
            }
            Some(other) => json!({"error": format!("answer {k} is not a JSON object: {}", head(&other.to_string(), 200))}),
            None => json!({"error": format!("no answer for record {k} in a reply to {n}")}),
        })
        .collect()
}

/// `{"answers": [item, item, ...]}`, exactly `n` of them, each `item`.
fn answers_schema(item: &schemars::Schema, n: usize) -> schemars::Schema {
    let item = serde_json::to_value(item).unwrap_or(Value::Null);
    let wrapped = json!({
        "title": "Answers",
        "type": "object",
        "properties": {
            "answers": {"type": "array", "items": item, "minItems": n, "maxItems": n}
        },
        "required": ["answers"],
        "additionalProperties": false
    });
    schemars::Schema::try_from(wrapped).expect("a schema wrapped in an object is a schema")
}

fn head(s: &str, n: usize) -> String {
    let mut t: String = s.trim().chars().take(n).collect();
    if t.len() < s.trim().len() {
        t.push('…');
    }
    t
}

/// Dot-path lookup into a JSON value: "a.b.c".
pub fn lookup<'a>(v: &'a Value, path: &str) -> Option<&'a Value> {
    if let Some(direct) = v.get(path) {
        return Some(direct);
    }
    let mut cur = v;
    for seg in path.split('.') {
        cur = cur.get(seg)?;
    }
    Some(cur)
}

/// A frame may carry several NDJSON lines (a plugin returns one blob per
/// batch); yield each non-empty line.
pub fn lines(payload: &[u8]) -> impl Iterator<Item = &[u8]> {
    payload
        .split(|b| *b == b'\n')
        .filter(|l| !l.iter().all(u8::is_ascii_whitespace))
}

/// An OpenAI-compatible chat server in a few lines, for tests here and in the
/// worker: it flags any request whose body mentions "bad", answers in prose
/// to one that mentions "prose", fails the first two requests that mention
/// "flaky" with a 500, and records every request body.
#[cfg(test)]
pub(crate) mod stub {
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::sync::Mutex;

    pub async fn model() -> (String, Arc<Mutex<Vec<String>>>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/v1", listener.local_addr().unwrap());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let log = Arc::clone(&seen);
        let flaky_failures = Arc::new(AtomicUsize::new(0));
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                let log = Arc::clone(&log);
                let flaky_failures = Arc::clone(&flaky_failures);
                tokio::spawn(async move {
                    let mut buf = Vec::new();
                    let mut tmp = [0u8; 4096];
                    let (body_start, body_len) = loop {
                        let n = sock.read(&mut tmp).await.unwrap_or(0);
                        if n == 0 {
                            return;
                        }
                        buf.extend_from_slice(&tmp[..n]);
                        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
                            let head = String::from_utf8_lossy(&buf[..pos]).to_ascii_lowercase();
                            let len = head
                                .lines()
                                .find_map(|l| l.strip_prefix("content-length:"))
                                .and_then(|v| v.trim().parse::<usize>().ok())
                                .unwrap_or(0);
                            break (pos + 4, len);
                        }
                    };
                    while buf.len() < body_start + body_len {
                        let n = sock.read(&mut tmp).await.unwrap_or(0);
                        if n == 0 {
                            break;
                        }
                        buf.extend_from_slice(&tmp[..n]);
                    }
                    let body = String::from_utf8_lossy(&buf[body_start..]).into_owned();
                    log.lock().await.push(body.clone());
                    if body.contains("flaky") && flaky_failures.fetch_add(1, Ordering::SeqCst) < 2 {
                        let resp = "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                        let _ = sock.write_all(resp.as_bytes()).await;
                        let _ = sock.shutdown().await;
                        return;
                    }
                    let verdict = |text: &str| {
                        if text.contains("bad") {
                            json!({"flag": true, "one_line": "bad thing"})
                        } else {
                            json!({"flag": false, "one_line": "fine"})
                        }
                    };
                    // A message's content is a string or a list of text parts.
                    let text_of = |c: &serde_json::Value| -> String {
                        match c {
                            serde_json::Value::String(s) => s.clone(),
                            serde_json::Value::Array(parts) => parts
                                .iter()
                                .filter_map(|p| p["text"].as_str())
                                .collect::<Vec<_>>()
                                .join(""),
                            _ => String::new(),
                        }
                    };
                    let req: serde_json::Value = serde_json::from_str(&body).unwrap_or_default();
                    let batched = text_of(&req["messages"][0]["content"]).contains("This request carries");
                    let content = if body.contains("prose") {
                        "Looks fine to me, nothing to flag.".to_string()
                    } else if batched {
                        // One answer per line of the user message; "short"
                        // anywhere in the batch drops the last answer.
                        let user = text_of(&req["messages"][1]["content"]);
                        let mut answers: Vec<_> = user.lines().map(verdict).collect();
                        if user.contains("short") {
                            answers.pop();
                        }
                        json!({"answers": answers}).to_string()
                    } else {
                        verdict(&body).to_string()
                    };
                    let reply = json!({
                        "choices": [{"message": {"content": content}, "finish_reason": "stop"}]
                    })
                    .to_string();
                    let resp = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                        reply.len(),
                        reply
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                    let _ = sock.shutdown().await;
                });
            }
        });
        (url, seen)
    }

    /// A node config against `url`, with the prompt (and a schema, when
    /// given) written under a fresh directory the caller removes.
    pub fn config(url: &str) -> tangent_shared::inference::InferenceConfig {
        let mut extra = serde_json::Map::new();
        extra.insert(
            "chat_template_kwargs".into(),
            json!({"enable_thinking": false}),
        );
        tangent_shared::inference::InferenceConfig {
            backend: tangent_shared::inference::Backend::Openai,
            url: url.to_string(),
            api_key: None,
            model: "stub".into(),
            aws: Default::default(),
            prompt_file: "judge.md".into(),
            schema_file: None,
            fields: Vec::new(),
            batch: 1,
            batch_bytes: 32_768,
            concurrency: 2,
            timeout_ms: 5_000,
            retries: 0,
            max_tokens: 50,
            temperature: None,
            extra,
        }
    }

    pub fn dir(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "tangent-inference-{tag}-{}-{}",
            std::process::id(),
            std::thread::current()
                .name()
                .unwrap_or("t")
                .replace("::", "-")
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            dir.join("judge.md"),
            "You are a test judge. Reply in JSON.\n",
        )
        .unwrap();
        dir
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn annotates_every_record_with_the_verdict() {
        let (url, seen) = stub::model().await;
        let dir = stub::dir("verdict");
        let node = InferenceNode::new(Arc::from("judge"), stub::config(&url), &dir).unwrap();
        let outs = node
            .annotate_all(vec![
                json!({"id": 1, "msg": "all good"}),
                json!({"id": 2, "msg": "bad login"}),
                json!({"id": 3, "msg": "also fine"}),
            ])
            .await;
        // Every record comes back, in order, with the verdict under the node's name.
        assert_eq!(outs.len(), 3);
        assert_eq!(
            outs.iter()
                .map(|o| o["id"].as_i64().unwrap())
                .collect::<Vec<_>>(),
            [1, 2, 3]
        );
        assert_eq!(outs[1]["judge"]["flag"], true);
        assert_eq!(outs[1]["judge"]["one_line"], "bad thing");
        assert_eq!(outs[0]["judge"]["flag"], false);
        assert_eq!(outs[2]["judge"]["flag"], false);

        let seen = seen.lock().await;
        assert_eq!(seen.len(), 3, "one request per record");
        for body in seen.iter() {
            let req: Value = serde_json::from_str(body).unwrap();
            assert_eq!(req["model"], "stub");
            assert_eq!(req["messages"][0]["role"], "system");
            assert!(
                req["messages"][0]["content"]
                    .to_string()
                    .contains("You are a test judge."),
                "{}",
                req["messages"][0]
            );
            assert_eq!(req["messages"][1]["role"], "user");
            assert_eq!(
                req["chat_template_kwargs"]["enable_thinking"], false,
                "extra is merged"
            );
            assert_eq!(req["max_tokens"], 50);
            assert_eq!(
                req["response_format"]["type"], "json_object",
                "JSON mode without a schema"
            );
            assert!(
                req.get("temperature").is_none(),
                "temperature is sent only when set: {req}"
            );
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn a_schema_is_sent_as_the_response_format() {
        let (url, seen) = stub::model().await;
        let dir = stub::dir("schema");
        std::fs::write(
            dir.join("verdict.json"),
            r#"{"title": "Verdict", "type": "object", "properties": {"flag": {"type": "boolean"}}, "required": ["flag"], "additionalProperties": false}"#,
        )
        .unwrap();
        let mut cfg = stub::config(&url);
        cfg.schema_file = Some("verdict.json".into());
        cfg.temperature = Some(0.0);
        let node = InferenceNode::new(Arc::from("judge"), cfg, &dir).unwrap();
        let out = node.annotate(json!({"id": 1, "msg": "bad"})).await;
        assert_eq!(out["judge"]["flag"], true);

        let seen = seen.lock().await;
        let req: Value = serde_json::from_str(&seen[0]).unwrap();
        assert_eq!(req["response_format"]["type"], "json_schema", "{req}");
        assert_eq!(req["response_format"]["json_schema"]["name"], "Verdict");
        assert_eq!(
            req["response_format"]["json_schema"]["schema"]["properties"]["flag"]["type"],
            "boolean"
        );
        assert_eq!(req["temperature"], 0.0);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn a_reply_that_is_not_json_is_the_error() {
        let (url, seen) = stub::model().await;
        let dir = stub::dir("prose");
        let node = InferenceNode::new(Arc::from("judge"), stub::config(&url), &dir).unwrap();
        let out = node.annotate(json!({"id": 7, "msg": "prose please"})).await;
        assert_eq!(out["id"], 7);
        let err = out["judge"]["error"].as_str().unwrap();
        assert!(
            err.contains("not JSON") && err.contains("Looks fine"),
            "{err}"
        );
        assert_eq!(
            seen.lock().await.len(),
            1,
            "a reply that came back is not retried"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn a_request_that_got_no_reply_is_retried() {
        let (url, seen) = stub::model().await;
        let dir = stub::dir("flaky");
        let mut cfg = stub::config(&url);
        cfg.retries = 2;
        let node = InferenceNode::new(Arc::from("judge"), cfg, &dir).unwrap();
        // Two 500s, then the verdict: three requests, one answer.
        let out = node
            .annotate(json!({"id": 3, "msg": "flaky bad link"}))
            .await;
        assert_eq!(out["judge"]["flag"], true, "{out}");
        assert_eq!(seen.lock().await.len(), 3);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn an_unreachable_model_leaves_an_error_on_the_record() {
        // Nothing listens on port 1. The record still goes on, with the error
        // where the verdict would be, so nothing is lost silently.
        let dir = stub::dir("dead");
        let node = InferenceNode::new(
            Arc::from("judge"),
            stub::config("http://127.0.0.1:1/v1"),
            &dir,
        )
        .unwrap();
        let out = node.annotate(json!({"id": 9})).await;
        assert_eq!(out["id"], 9);
        assert!(!out["judge"]["error"].as_str().unwrap().is_empty(), "{out}");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn a_batch_shares_one_request_and_each_record_gets_its_answer() {
        let (url, seen) = stub::model().await;
        let dir = stub::dir("batch");
        let mut cfg = stub::config(&url);
        cfg.batch = 2;
        let node = InferenceNode::new(Arc::from("judge"), cfg, &dir).unwrap();
        let outs = node
            .annotate_all(vec![
                json!({"id": 1, "msg": "fine"}),
                json!({"id": 2, "msg": "bad"}),
                json!({"id": 3, "msg": "fine"}),
                json!({"id": 4, "msg": "fine"}),
                json!({"id": 5, "msg": "bad again"}),
            ])
            .await;
        assert_eq!(
            outs.iter().map(|o| o["id"].as_i64().unwrap()).collect::<Vec<_>>(),
            [1, 2, 3, 4, 5],
            "order kept"
        );
        assert_eq!(
            outs.iter().map(|o| o["judge"]["flag"].as_bool().unwrap()).collect::<Vec<_>>(),
            [false, true, false, false, true],
            "each record has its own answer: {outs:?}"
        );
        assert!(outs[1]["judge"].get("i").is_none(), "the index is not part of the answer");

        let seen = seen.lock().await;
        assert_eq!(seen.len(), 3, "five records, two per request: 2 + 2 + 1");
        let req: Value = serde_json::from_str(&seen[0]).unwrap();
        let system = req["messages"][0]["content"].to_string();
        assert!(system.contains("You are a test judge."), "the prompt comes first: {system}");
        assert!(system.contains("2 records"), "{system}");
        let user = match &req["messages"][1]["content"] {
            Value::String(s) => s.clone(),
            other => other["0"]["text"].as_str().or(other[0]["text"].as_str()).unwrap_or_default().to_string(),
        };
        let lines: Vec<Value> = user.lines().map(|l| serde_json::from_str(l).unwrap()).collect();
        assert_eq!(lines.len(), 2);
        assert_eq!(req["max_tokens"], 100, "max_tokens is per record: 50 x 2");
        assert_eq!(lines[0]["i"], 0);
        assert_eq!(lines[1]["i"], 1);
        assert_eq!(lines[1]["id"], 2);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn a_short_answer_list_leaves_errors_on_the_rest() {
        let (url, _seen) = stub::model().await;
        let dir = stub::dir("short");
        let mut cfg = stub::config(&url);
        cfg.batch = 3;
        let node = InferenceNode::new(Arc::from("judge"), cfg, &dir).unwrap();
        let outs = node
            .annotate_all(vec![
                json!({"id": 1, "msg": "bad"}),
                json!({"id": 2, "msg": "short reply please"}),
                json!({"id": 3, "msg": "fine"}),
            ])
            .await;
        assert_eq!(outs[0]["judge"]["flag"], true);
        assert_eq!(outs[1]["judge"]["flag"], false);
        let err = outs[2]["judge"]["error"].as_str().unwrap();
        assert!(err.contains("no answer for record 2"), "{err}");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn a_batch_wraps_the_schema_around_each_answer() {
        let (url, seen) = stub::model().await;
        let dir = stub::dir("batch-schema");
        std::fs::write(
            dir.join("verdict.json"),
            r#"{"title": "Verdict", "type": "object", "properties": {"flag": {"type": "boolean"}}, "required": ["flag"], "additionalProperties": false}"#,
        )
        .unwrap();
        let mut cfg = stub::config(&url);
        cfg.schema_file = Some("verdict.json".into());
        cfg.batch = 4;
        let node = InferenceNode::new(Arc::from("judge"), cfg, &dir).unwrap();
        let outs = node
            .annotate_all(vec![json!({"id": 1, "msg": "bad"}), json!({"id": 2})])
            .await;
        assert_eq!(outs[0]["judge"]["flag"], true);
        let seen = seen.lock().await;
        let req: Value = serde_json::from_str(&seen[0]).unwrap();
        let schema = &req["response_format"]["json_schema"]["schema"];
        assert_eq!(req["response_format"]["json_schema"]["name"], "Answers");
        assert_eq!(schema["properties"]["answers"]["items"]["properties"]["flag"]["type"], "boolean", "{schema}");
        assert_eq!(schema["properties"]["answers"]["minItems"], 2);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn batch_bytes_caps_a_request() {
        let (url, seen) = stub::model().await;
        let dir = stub::dir("batch-bytes");
        let mut cfg = stub::config(&url);
        cfg.batch = 10;
        cfg.batch_bytes = 80;
        let node = InferenceNode::new(Arc::from("judge"), cfg, &dir).unwrap();
        // ~45 bytes each: two fit under 80, the third starts a new request.
        let outs = node
            .annotate_all((1..=5).map(|i| json!({"id": i, "msg": "a record of some length"})).collect())
            .await;
        assert_eq!(outs.len(), 5);
        assert_eq!(seen.lock().await.len(), 3, "2 + 2 + 1 by bytes");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn a_prompt_file_that_is_missing_is_an_error_at_startup() {
        let dir = stub::dir("missing");
        let mut cfg = stub::config("http://127.0.0.1:1/v1");
        cfg.prompt_file = "nope.md".into();
        let err = InferenceNode::new(Arc::from("judge"), cfg, &dir)
            .err()
            .unwrap();
        assert!(format!("{err:#}").contains("nope.md"), "{err:#}");
        let _ = std::fs::remove_dir_all(dir);
    }
}
