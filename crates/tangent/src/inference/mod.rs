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
use secrecy::ExposeSecret;
use serde_json::{json, Map, Value};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tangent_shared::inference::{Backend, ContextConfig, InferenceConfig};

use crate::state::{Entry, StateStore, StateValue};
use tokio::sync::Semaphore;

/// A request: its partition, the records' places, the records, and their rows (`_key`).
type Job = (Arc<str>, Vec<usize>, Vec<Value>, Option<HashSet<String>>);

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
            let key = cfg.api_key.as_ref().map_or("none", |k| k.expose_secret());
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

    #[cfg(test)]
    pub async fn annotate_all(&self, records: Vec<Value>) -> Vec<Value> {
        let part: Arc<str> = Arc::from("");
        self.annotate_in(records.into_iter().map(|v| (v, Arc::clone(&part))).collect(), None)
            .await
    }

    /// Grouped by partition and buffer, in requests of `batch`; the records in the order given.
    pub async fn annotate_in(
        &self,
        records: Vec<(Value, Arc<str>)>,
        store: Option<Arc<dyn StateStore>>,
    ) -> Vec<Value> {
        let n = records.len();
        // the row each record was read from (`_key`), kept from the model
        let mut rows: Vec<Option<String>> = Vec::with_capacity(n);
        let mut groups: Vec<(_, Vec<(usize, Value)>)> = Vec::new();
        for (i, (mut v, part)) in records.into_iter().enumerate() {
            rows.push(match v.as_object_mut().and_then(|m| m.remove("_key")) {
                Some(Value::String(k)) => Some(k),
                _ => None,
            });
            let window = v
                .get("_buffer")
                .and_then(|s| s.get("from"))
                .and_then(Value::as_str)
                .unwrap_or("")
                .to_string();
            let key = (part, window);
            match groups.iter_mut().find(|(k, _)| *k == key) {
                Some((_, g)) => g.push((i, v)),
                None => groups.push((key, vec![(i, v)])),
            }
        }
        let mut jobs: Vec<Job> = Vec::new();
        for ((part, _), group) in groups {
            let (ids, values): (Vec<usize>, Vec<Value>) = group.into_iter().unzip();
            let mut at = 0;
            for chunk in self.chunks(values) {
                let these = ids[at..at + chunk.len()].to_vec();
                at += chunk.len();
                let own = these.iter().map(|&i| rows[i].clone()).collect();
                jobs.push((Arc::clone(&part), these, chunk, own));
            }
        }
        let done = futures::future::join_all(jobs.into_iter().map(|(part, ids, chunk, own)| {
            let store = store.clone();
            async move {
                let _permit = self.sem.acquire().await.expect("semaphore is never closed");
                let context = match &store {
                    Some(st) => self.context_for(st, &part, &chunk, own).await,
                    None => None,
                };
                let out = if self.cfg.batch <= 1 {
                    let v = chunk.into_iter().next().expect("a chunk of one");
                    vec![self.annotate_one(v, context).await]
                } else {
                    self.annotate_batch(chunk, context).await
                };
                (ids, out)
            }
        }))
        .await;
        let mut slots: Vec<Option<Value>> = (0..n).map(|_| None).collect();
        for (ids, out) in done {
            for (i, v) in ids.into_iter().zip(out) {
                slots[i] = Some(v);
            }
        }
        slots.into_iter().map(|s| s.unwrap_or(Value::Null)).collect()
    }

    #[cfg(test)]
    pub async fn annotate(&self, v: Value) -> Value {
        self.annotate_one(v, None).await
    }

    async fn annotate_one(&self, v: Value, context: Option<String>) -> Value {
        let start = Instant::now();
        let user = match context {
            Some(c) => format!("{c}\n\nThe record:\n{}", self.user_message(&v)),
            None => self.user_message(&v),
        };
        let reply = match self.ask(user, None).await {
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

    /// Several records in one request; an unreadable reply is asked again in halves.
    async fn annotate_batch(&self, records: Vec<Value>, context: Option<String>) -> Vec<Value> {
        let n = records.len();
        let start = Instant::now();
        let lines = records
            .iter()
            .enumerate()
            .map(|(k, v)| self.batch_line(k, v))
            .collect::<Vec<_>>()
            .join("\n");
        let user = match &context {
            Some(c) => format!("{c}\n\nThe records:\n{lines}"),
            None => lines,
        };
        let answers = match self.ask(user, Some(n)).await {
            Ok(reply) => spread(reply, n),
            // a failed request would fail the same in halves
            Err(e) if n > 1 && e.to_string().starts_with("reply ") => {
                tracing::warn!(
                    node = %self.name,
                    records = n,
                    "batched reply unreadable, asking again as two halves: {e:#}"
                );
                let mut records = records;
                let tail = records.split_off(n / 2);
                let mut out = Box::pin(self.annotate_batch(records, context.clone())).await;
                out.extend(Box::pin(self.annotate_batch(tail, context)).await);
                return out;
            }
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

    /// One view per scope and key: its latest records before the earliest of these, then the rest of their buffer.
    async fn context_for(
        &self,
        store: &Arc<dyn StateStore>,
        part: &str,
        records: &[Value],
        own: Option<HashSet<String>>,
    ) -> Option<String> {
        let cfg = self.cfg.context.as_ref()?;
        let times: Vec<i64> = records
            .iter()
            .filter_map(|v| crate::windows::time_of_value(v, crate::windows::TIME_FIELD))
            .collect();
        let (before, latest) = (*times.iter().min()?, *times.iter().max()?);
        let bound = crate::windows::ev_bound(before);
        // The rest of their buffer: from the earliest record on, the
        // request's own rows left out, which only their keys tell apart.
        let buffer_end = records
            .iter()
            .filter_map(|v| crate::windows::time_of_value(v, "_buffer.to"))
            .max();
        let rest_end = own
            .is_some()
            .then(|| crate::windows::ev_bound(buffer_end.unwrap_or(i64::MIN).max(latest + 1)));
        // The request's own records are shown as themselves.
        let mut shown: HashSet<String> = own.unwrap_or_default();
        let whose = cfg
            .scopes
            .iter()
            .map(|(scope, field)| format!("`{field}` for {scope}"))
            .collect::<Vec<_>>()
            .join(", ");
        let mut lines = vec![match &rest_end {
            None => format!(
                "History before these records, one JSON object per line: `scope` and `key` say whose \
                 (a record's own fields name them: {whose}), `recent` is their latest {n} records before these, \
                 however long ago, oldest first:",
                n = cfg.max_events
            ),
            Some(_) => format!(
                "History before these records and the rest of their buffer, one JSON object per line: `scope` \
                 and `key` say whose (a record's own fields name them: {whose}); `recent` is their latest {n} \
                 records before these, however long ago, and `rest_of_buffer` the rest of their buffer, not judged \
                 here: their first {n} records other than these from the earliest of these to {until}; each oldest \
                 first:",
                n = cfg.max_events,
                until = if buffer_end.is_some() { "the end of the buffer" } else { "the latest of these" },
            ),
        }];
        let mut bytes = lines[0].len();
        for (scope, field) in &cfg.scopes {
            // busiest first
            let mut keys: Vec<(String, usize)> = Vec::new();
            for v in records {
                let Some(k) = lookup(v, field).and_then(crate::windows::scope_value) else { continue };
                match keys.iter_mut().find(|(have, _)| *have == k) {
                    Some((_, n)) => *n += 1,
                    None => keys.push((k, 1)),
                }
            }
            keys.sort_by_key(|k| std::cmp::Reverse(k.1));
            'keys: for (key, _) in keys {
                // a scope that is the partition reads its rows, any other the index
                let whole = key == part;
                let (ns, prefix) = if whole {
                    (crate::windows::EVENTS_NS, String::new())
                } else {
                    (crate::windows::INDEX_NS, crate::windows::index_prefix(scope, &key))
                };
                // newest first before these; then, oldest first, the rest of their buffer
                for rest in [false, true] {
                    let limit = cfg.max_events + shown.len();
                    let entries = match (rest, &rest_end) {
                        (false, _) => {
                            store.range_rev(ns, part, &format!("{prefix}ev:"), &format!("{prefix}{bound}"), limit).await
                        }
                        (true, Some(end)) => {
                            store.range(ns, part, &format!("{prefix}{bound}"), &format!("{prefix}{end}"), limit).await
                        }
                        (true, None) => break,
                    };
                    let entries = match entries {
                        Ok(entries) => entries,
                        Err(e) => {
                            tracing::warn!(node = %self.name, "reading context: {e:#}");
                            return None;
                        }
                    };
                    let mut view = self.view_rows(store, cfg, part, entries, whole, &mut shown).await;
                    if view.is_empty() {
                        continue;
                    }
                    if !rest {
                        view.reverse();
                    }
                    let name = if rest { "rest_of_buffer" } else { "recent" };
                    let line = json!({
                        "scope": scope,
                        "key": key,
                        "from": crate::windows::iso(view[0].0),
                        "to": crate::windows::iso(view[view.len() - 1].0),
                        name: view.into_iter().map(|(_, v)| v).collect::<Vec<_>>(),
                    })
                    .to_string();
                    if bytes + line.len() > 40_000 && lines.len() > 1 {
                        break 'keys;
                    }
                    bytes += line.len();
                    lines.push(line);
                }
            }
        }
        (lines.len() > 1).then(|| lines.join("\n"))
    }

    /// A view's (time, history line)s until it is full; a record already shown is skipped.
    async fn view_rows(
        &self,
        store: &Arc<dyn StateStore>,
        cfg: &ContextConfig,
        part: &str,
        entries: Vec<Entry>,
        whole: bool,
        shown: &mut HashSet<String>,
    ) -> Vec<(i64, Value)> {
        let mut out = Vec::new();
        for e in entries {
            if out.len() >= cfg.max_events {
                break;
            }
            let StateValue::Str(value) = e.value else { continue };
            // a partition's row is the record; an index entry names its row
            let (row, text) = if whole { (e.key, Some(value)) } else { (value, None) };
            if shown.contains(&row) {
                continue;
            }
            let text = match text {
                Some(text) => text,
                None => match store.get(crate::windows::EVENTS_NS, part, &row).await {
                    Ok(Some(StateValue::Str(text))) => text,
                    _ => continue,
                },
            };
            let (Ok(v), Some(ts)) = (serde_json::from_str::<Value>(&text), crate::windows::ts_of_key(&row)) else { continue };
            shown.insert(row);
            out.push((ts, self.history_line(&v, cfg)));
        }
        out
    }

    fn history_line(&self, v: &Value, cfg: &ContextConfig) -> Value {
        if cfg.fields.is_empty() {
            return v.clone();
        }
        let mut m = Map::new();
        for f in &cfg.fields {
            if let Some(x) = lookup(v, f) {
                if !x.is_null() {
                    m.insert(f.clone(), x.clone());
                }
            }
        }
        Value::Object(m)
    }

    /// At most `batch` records and about `batch_bytes` per request.
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

    fn batch_line(&self, k: usize, v: &Value) -> String {
        let mut m = Map::new();
        m.insert("i".into(), json!(k));
        if let Value::Object(fields) = self.view(v) {
            m.extend(fields);
        }
        serde_json::to_string(&Value::Object(m)).unwrap_or_default()
    }

    fn request(&self, user: String, batch: Option<usize>) -> CompletionRequest {
        let preamble = match batch {
            None => format!(
                "{}\n\nReply with the record's answer, one JSON object, and nothing else: no code fence, \
                 no text before or after it.",
                self.prompt.trim_end()
            ),
            Some(n) => format!(
                "{}\n\nThis request carries {n} records, one JSON object per line, each with its index `i` (0 to {last}). \
                 Reply with one JSON object, {{\"answers\": [...]}}, holding exactly {n} answers, one per record in \
                 the same order, each the record's answer with its index first: {{\"i\": k, ...}}. Keep going through \
                 every record: the reply is not finished until the answer with \"i\": {last} is written, then close \
                 the array and the object. The reply is the JSON object itself: no code fence, no text before or \
                 after it.",
                self.prompt.trim_end(),
                last = n.saturating_sub(1)
            ),
        };
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
                    Some(_) => answers_schema(schema),
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

    /// One request: the reply's text, or why there is none.
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

/// Answers by their `i` when all carry one, else by position.
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

/// `{"answers": [...]}`, each answer with its record's index `i`.
fn answers_schema(item: &schemars::Schema) -> schemars::Schema {
    let mut item = serde_json::to_value(item).unwrap_or(Value::Null);
    let defs: Vec<(&str, Value)> = ["$defs", "definitions"]
        .into_iter()
        .filter_map(|k| Some((k, item.as_object_mut()?.remove(k)?)))
        .collect();
    if item.get("properties").is_some() {
        indexed(&mut item);
    }
    if let Some(shapes) = item.get_mut("anyOf").and_then(Value::as_array_mut) {
        for shape in shapes.iter_mut() {
            if shape.get("properties").is_some() || shape.get("type").and_then(Value::as_str) == Some("object") {
                indexed(shape);
            }
        }
    }
    let mut wrapped = json!({
        "title": "Answers",
        "type": "object",
        "properties": {
            "answers": {"type": "array", "items": item}
        },
        "required": ["answers"],
        "additionalProperties": false
    });
    for (k, d) in defs {
        wrapped[k] = d;
    }
    schemars::Schema::try_from(wrapped).expect("a schema wrapped in an object is a schema")
}

/// `i` first in the shape's properties, and required.
fn indexed(shape: &mut Value) {
    let Some(obj) = shape.as_object_mut() else {
        return;
    };
    let mut props = Map::new();
    props.insert("i".into(), json!({"type": "integer", "description": "the record's index"}));
    if let Some(Value::Object(old)) = obj.remove("properties") {
        props.extend(old.into_iter().filter(|(k, _)| k != "i"));
    }
    obj.insert("properties".into(), Value::Object(props));
    let mut required = vec![json!("i")];
    if let Some(Value::Array(old)) = obj.remove("required") {
        required.extend(old.into_iter().filter(|r| r != "i"));
    }
    obj.insert("required".into(), Value::Array(required));
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
                    // a string or a list of text parts
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
                    } else if batched && text_of(&req["messages"][1]["content"]).matches("loop").count() >= 3 {
                        // three "loop" records degenerate
                        "{\"answers\": [{\"flag\": false, \"one_line\": \"a7a7a7a7a7a7a7a7".to_string()
                    } else if batched {
                        // one answer per record line
                        let answers: Vec<_> = text_of(&req["messages"][1]["content"])
                            .lines()
                            .filter(|l| l.starts_with("{\"i\":"))
                            .map(verdict)
                            .collect();
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
            context: None,
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
    use std::collections::BTreeMap;

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
    async fn an_unreadable_batched_reply_is_asked_again_in_halves() {
        let (url, seen) = stub::model().await;
        let dir = stub::dir("halves");
        let mut cfg = stub::config(&url);
        cfg.batch = 8;
        let node = InferenceNode::new(Arc::from("judge"), cfg, &dir).unwrap();
        let outs = node
            .annotate_all((1..=4).map(|i| json!({"id": i, "msg": format!("loop {i} bad")})).collect())
            .await;
        assert_eq!(outs.len(), 4);
        assert!(outs.iter().all(|o| o["judge"]["flag"] == true), "every record got its answer: {outs:?}");
        assert_eq!(seen.lock().await.len(), 3, "one failed request, then one per half");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn a_request_is_read_with_the_history_before_it_from_the_store() {
        use crate::state::lru::LruStore;
        use tangent_shared::runtime::StateConfig;
        let (url, seen) = stub::model().await;
        let dir = stub::dir("context");
        let store: Arc<dyn StateStore> = Arc::new(LruStore::new(StateConfig::default()).unwrap());
        let t0 = 1_793_106_000_000i64; // 2026-10-27T13:00:00Z
        let mut recent = BTreeMap::new();
        recent.insert("actor".to_string(), "_norm.actor".to_string());
        recent.insert("org".to_string(), "_norm.org".to_string());
        let sink = crate::sinks::store::StoreSink::new(
            &tangent_shared::sinks::store::StoreSinkConfig { retain_secs: 2_592_000, recent, novel: Default::default(), follow_secs: None },
            Arc::clone(&store),
            Some(tangent_shared::runtime::PartitionConfig { key: vec!["_norm.org".into()], separator: "|".into(), retain_ms: None, time: None }),
        )
        .unwrap();
        let mut payload = bytes::BytesMut::new();
        for (ts, actor, kind) in [
            (t0 - 14 * 86_400_000, "a", "weeks.ago"),
            (t0 - 3 * 86_400_000, "a", "days.ago"),
            (t0 - 3_600_000, "a", "login"),
            (t0 - 600_000, "a", "token.create"),
            (t0 - 300_000, "b", "push"),
            (t0 - 60_000, "c", "export"),
            (t0 + 600_000, "a", "later"),
        ] {
            let row = json!({"ts": crate::windows::iso(ts), "_norm": {"actor": actor, "org": "acme", "kind": kind}, "noise": "x"});
            payload.extend_from_slice(format!("{row}\n").as_bytes());
        }
        use crate::sinks::manager::Sink;
        sink.write(crate::sinks::manager::SinkWrite { sink_name: Arc::from("store"), payload, s3: None }).await.unwrap();
        let mut cfg = stub::config(&url);
        cfg.batch = 8;
        let mut scopes = BTreeMap::new();
        scopes.insert("actor".to_string(), "_norm.actor".to_string());
        scopes.insert("org".to_string(), "_norm.org".to_string());
        cfg.context = Some(ContextConfig {
            scopes,
            max_events: 3,
            fields: vec!["ts".into(), "_norm.kind".into()],
        });
        let node = InferenceNode::new(Arc::from("judge"), cfg, &dir).unwrap();
        let part: Arc<str> = Arc::from("acme");
        let outs = node
            .annotate_in(
                vec![
                    (json!({"ts": crate::windows::iso(t0), "_norm": {"actor": "a", "org": "acme"}, "msg": "bad"}), Arc::clone(&part)),
                    (json!({"ts": crate::windows::iso(t0 + 1000), "_norm": {"actor": "b", "org": "acme"}}), Arc::clone(&part)),
                ],
                Some(Arc::clone(&store)),
            )
            .await;
        assert_eq!(outs[0]["judge"]["flag"], true, "{outs:?}");
        let seen = seen.lock().await;
        assert_eq!(seen.len(), 1);
        let req: Value = serde_json::from_str(&seen[0]).unwrap();
        let user = match &req["messages"][1]["content"] {
            Value::String(s) => s.clone(),
            other => other[0]["text"].as_str().unwrap_or_default().to_string(),
        };
        let views: Vec<Value> = user.lines().filter(|l| l.starts_with("{\"scope\"")).map(|l| serde_json::from_str(l).unwrap()).collect();
        let a = views.iter().find(|v| v["scope"] == "actor" && v["key"] == "a").unwrap_or_else(|| panic!("a's view in: {user}"));
        let a_kinds: Vec<&str> = a["recent"].as_array().unwrap().iter().map(|r| r["_norm.kind"].as_str().unwrap()).collect();
        assert_eq!(a_kinds, ["days.ago", "login", "token.create"], "a's latest three before the request, however long ago, oldest first: {a}");
        assert!(a["recent"][0].get("noise").is_none(), "only the configured fields");
        let org = views.iter().find(|v| v["scope"] == "org").expect("the org's view");
        let kinds: Vec<&str> = org["recent"].as_array().unwrap().iter().map(|r| r["_norm.kind"].as_str().unwrap()).collect();
        assert_eq!(kinds, ["weeks.ago", "export"], "the org's latest, without what the actors' views showed: {org}");
        assert!(!user.contains("later"), "nothing at or after the earliest record");
        let _ = std::fs::remove_dir_all(dir);
    }

    /// A `novel` filter hands the judge only some of a buffer's records: the
    /// rest of it (what came between and after them) is shown beside them,
    /// and the row keys that tell the two apart never reach the model.
    #[tokio::test]
    async fn a_request_is_read_with_the_rest_of_its_buffer() {
        use crate::sinks::manager::Sink;
        use crate::state::lru::LruStore;
        use tangent_shared::runtime::StateConfig;
        let (url, seen) = stub::model().await;
        let dir = stub::dir("rest-of-buffer");
        let store: Arc<dyn StateStore> = Arc::new(LruStore::new(StateConfig::default()).unwrap());
        let mut recent = BTreeMap::new();
        recent.insert("actor".to_string(), "_norm.actor".to_string());
        recent.insert("org".to_string(), "_norm.org".to_string());
        let sink = crate::sinks::store::StoreSink::new(
            &tangent_shared::sinks::store::StoreSinkConfig { retain_secs: 2_592_000, recent, novel: Default::default(), follow_secs: None },
            Arc::clone(&store),
            Some(tangent_shared::runtime::PartitionConfig { key: vec!["_norm.org".into()], separator: "|".into(), retain_ms: None, time: None }),
        )
        .unwrap();
        // The buffer is 13:00 to 13:05. a creates a user (judged), then does
        // four things it has done before; c does something new (judged); b
        // goes on after the last judged record, and again in the next buffer.
        let t0 = 1_793_106_000_000i64; // 2026-10-27T13:00:00Z
        let mut payload = bytes::BytesMut::new();
        for (ts, actor, kind) in [
            (t0 - 3_600_000, "a", "login"),
            (t0 - 60_000, "d", "export"),
            (t0, "a", "create.user"),
            (t0 + 10_000, "a", "attach.policy"),
            (t0 + 20_000, "a", "create.key"),
            (t0 + 30_000, "a", "list.users"),
            (t0 + 40_000, "a", "get.secret"),
            (t0 + 60_000, "c", "new.thing"),
            (t0 + 120_000, "b", "tail"),
            (t0 + 400_000, "b", "next.buffer"),
        ] {
            let row = json!({"ts": crate::windows::iso(ts), "_norm": {"actor": actor, "org": "acme", "kind": kind}});
            payload.extend_from_slice(format!("{row}\n").as_bytes());
        }
        sink.write(crate::sinks::manager::SinkWrite { sink_name: Arc::from("store"), payload, s3: None }).await.unwrap();
        // The judged records as the store source emits them, after the filter.
        let buffer = json!({"from": crate::windows::iso(t0), "to": crate::windows::iso(t0 + 300_000)});
        let judged: Vec<Value> = store
            .range("events", "acme", &crate::windows::ev_bound(t0), &crate::windows::ev_bound(t0 + 300_000), 100)
            .await
            .unwrap()
            .into_iter()
            .filter_map(|e| {
                let StateValue::Str(text) = e.value else { return None };
                let mut v: Value = serde_json::from_str(&text).unwrap();
                ["create.user", "new.thing"].contains(&v["_norm"]["kind"].as_str()?).then(|| {
                    v["_key"] = json!(e.key);
                    v["_buffer"] = buffer.clone();
                    v
                })
            })
            .collect();
        assert_eq!(judged.len(), 2);

        let mut cfg = stub::config(&url);
        cfg.batch = 8;
        let mut scopes = BTreeMap::new();
        scopes.insert("actor".to_string(), "_norm.actor".to_string());
        scopes.insert("org".to_string(), "_norm.org".to_string());
        cfg.context = Some(ContextConfig {
            scopes,
            max_events: 3,
            fields: vec!["ts".into(), "_norm.kind".into()],
        });
        let node = InferenceNode::new(Arc::from("judge"), cfg, &dir).unwrap();
        let part: Arc<str> = Arc::from("acme");
        // A request's views: scope, key, which view, and its records' kinds.
        let views = |body: &str| -> Vec<(String, String, String, Vec<String>)> {
            let req: Value = serde_json::from_str(body).unwrap();
            let user = match &req["messages"][1]["content"] {
                Value::String(s) => s.clone(),
                other => other[0]["text"].as_str().unwrap_or_default().to_string(),
            };
            user.lines()
                .filter(|l| l.starts_with("{\"scope\""))
                .map(|l| {
                    let v: Value = serde_json::from_str(l).unwrap();
                    let which = if v.get("rest_of_buffer").is_some() { "rest_of_buffer" } else { "recent" };
                    let kinds = v[which].as_array().unwrap().iter().map(|r| r["_norm.kind"].as_str().unwrap().to_string()).collect();
                    (v["scope"].as_str().unwrap().to_string(), v["key"].as_str().unwrap().to_string(), which.to_string(), kinds)
                })
                .collect()
        };
        let view = |scope: &str, key: &str, which: &str, kinds: &[&str]| {
            (scope.to_string(), key.to_string(), which.to_string(), kinds.iter().map(|k| k.to_string()).collect::<Vec<_>>())
        };
        let outs = node
            .annotate_in(judged.iter().map(|v| (v.clone(), Arc::clone(&part))).collect(), Some(Arc::clone(&store)))
            .await;
        assert!(outs.iter().all(|o| o.get("_key").is_none() && o.get("_buffer").is_some()), "the row key goes no further: {outs:?}");
        let body = seen.lock().await[0].clone();
        assert!(!body.contains("_key") && !body.contains("ev:"), "the row keys never reach the model: {body}");
        assert_eq!(body.matches("create.user").count(), 1, "a judged record is not shown again: {body}");
        assert_eq!(body.matches("new.thing").count(), 1, "{body}");
        assert_eq!(
            views(&body),
            [
                view("actor", "a", "recent", &["login"]),
                // The first three of a's four, oldest first.
                view("actor", "a", "rest_of_buffer", &["attach.policy", "create.key", "list.users"]),
                view("org", "acme", "recent", &["export"]),
                // What a's view did not show, and b's after the last judged
                // record; not the next buffer's.
                view("org", "acme", "rest_of_buffer", &["get.secret", "tail"]),
            ],
            "{body}"
        );

        // Late records have no buffer: the rest ends at the latest of them.
        let late: Vec<(Value, Arc<str>)> = judged
            .iter()
            .map(|v| {
                let mut v = v.clone();
                v.as_object_mut().unwrap().remove("_buffer");
                v["_late"] = json!(true);
                (v, Arc::clone(&part))
            })
            .collect();
        node.annotate_in(late, Some(Arc::clone(&store))).await;
        let body = seen.lock().await[1].clone();
        assert!(views(&body).contains(&view("org", "acme", "rest_of_buffer", &["get.secret"])), "{body}");
        assert!(body.contains("to the latest of these"), "{body}");

        // Records that do not say their rows have no rest of buffer: their
        // own rows could not be told from the rest.
        let unkeyed: Vec<(Value, Arc<str>)> = judged
            .iter()
            .map(|v| {
                let mut v = v.clone();
                v.as_object_mut().unwrap().remove("_key");
                (v, Arc::clone(&part))
            })
            .collect();
        node.annotate_in(unkeyed, Some(Arc::clone(&store))).await;
        let body = seen.lock().await[2].clone();
        assert!(!body.contains("rest_of_buffer"), "{body}");
        assert!(body.contains("History before these records, one JSON object per line"), "the history as before: {body}");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn spread_errors_missing_answers() {
        let out = spread(json!({"answers": [{"i": 1, "flag": true}, {"i": 0, "flag": false}]}), 3);
        assert_eq!(out[0], json!({"flag": false}));
        assert_eq!(out[1], json!({"flag": true}));
        assert!(out[2]["error"].as_str().unwrap().contains("no answer for record 2"));
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
        assert!(schema["properties"]["answers"].get("minItems").is_none(), "no array bounds: backends reject them");
        let item = &schema["properties"]["answers"]["items"];
        assert_eq!(item["properties"]["i"]["type"], "integer", "each answer carries its record's index: {item}");
        assert!(item["required"].as_array().unwrap().iter().any(|r| r == "i"), "{item}");
        let first = item["properties"].as_object().unwrap().keys().next().unwrap();
        assert_eq!(first, "i", "the index comes before the answer: {item}");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn an_answer_of_two_shapes_gets_the_index_in_each() {
        let verdict: schemars::Schema = serde_json::from_value(json!({
            "title": "Verdict",
            "anyOf": [
                {"type": "object", "properties": {"flag": {"type": "boolean", "const": false}},
                 "required": ["flag"], "additionalProperties": false},
                {"type": "object", "properties": {"flag": {"type": "boolean", "const": true}, "why": {"type": "string"}},
                 "required": ["flag", "why"], "additionalProperties": false}
            ]
        }))
        .unwrap();
        let schema = serde_json::to_value(answers_schema(&verdict)).unwrap();
        let shapes = schema["properties"]["answers"]["items"]["anyOf"].as_array().unwrap();
        assert_eq!(shapes.len(), 2);
        for shape in shapes {
            let props: Vec<&String> = shape["properties"].as_object().unwrap().keys().collect();
            assert_eq!(props[0], "i", "{shape}");
            assert_eq!(shape["required"][0], "i", "{shape}");
            assert_eq!(shape["additionalProperties"], false, "{shape}");
        }
        assert_eq!(shapes[0]["properties"].as_object().unwrap().len(), 2, "a pass is the index and the flag");
        assert_eq!(shapes[1]["required"], json!(["i", "flag", "why"]));
    }

    #[test]
    fn chunks_by_count_and_bytes() {
        let dir = stub::dir("chunks");
        let mut cfg = stub::config("http://127.0.0.1:1/v1");
        cfg.batch = 3;
        cfg.batch_bytes = 80;
        let node = InferenceNode::new(Arc::from("judge"), cfg, &dir).unwrap();
        let lens = |v: Value| node.chunks(vec![v; 5]).iter().map(Vec::len).collect::<Vec<_>>();
        assert_eq!(lens(json!({"id": 1})), [3, 2]);
        assert_eq!(lens(json!({"id": 1, "msg": "a record of some length"})), [2, 2, 1]);
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
