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
use tangent_shared::inference::{Backend, ContextConfig, InferenceConfig};

use crate::state::{StateStore, StateValue};
use tokio::sync::Semaphore;

/// Records judged together: one partition, one window.
type GroupKey = (Arc<str>, String);

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
        if cfg.context.as_ref().is_some_and(|c| !c.window.is_empty()) {
            bail!(
                "inference node {name}: context.window is gone; a scope's history is its latest records however \
                 long ago (context.max_events), kept by the store sink's `recent`"
            );
        }
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

    /// Every record through the model, under the node's permits; the
    /// annotated records, in the order given. No partition, no context.
    pub async fn annotate_all(&self, records: Vec<Value>) -> Vec<Value> {
        let part: Arc<str> = Arc::from("");
        self.annotate_in(records.into_iter().map(|v| (v, Arc::clone(&part))).collect(), None)
            .await
    }

    /// Records with their partitions through the model: grouped by
    /// partition and by buffer (`_buffer.from`, when the records came from a
    /// `store` source), each group in requests of `batch`, each request read
    /// with the history before its earliest record when `context` is set and
    /// a store is given. The annotated records, in the order given.
    pub async fn annotate_in(
        &self,
        records: Vec<(Value, Arc<str>)>,
        store: Option<Arc<dyn StateStore>>,
    ) -> Vec<Value> {
        let n = records.len();
        let mut groups: Vec<(GroupKey, Vec<(usize, Value)>)> = Vec::new();
        for (i, (v, part)) in records.into_iter().enumerate() {
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
        let mut jobs: Vec<(Arc<str>, Vec<usize>, Vec<Value>)> = Vec::new();
        for ((part, _), group) in groups {
            let (ids, values): (Vec<usize>, Vec<Value>) = group.into_iter().unzip();
            let chunks = if self.cfg.batch <= 1 {
                values.into_iter().map(|v| vec![v]).collect()
            } else {
                self.chunks(values)
            };
            let mut at = 0;
            for chunk in chunks {
                let these = ids[at..at + chunk.len()].to_vec();
                at += chunk.len();
                jobs.push((Arc::clone(&part), these, chunk));
            }
        }
        let done = futures::future::join_all(jobs.into_iter().map(|(part, ids, chunk)| {
            let store = store.clone();
            async move {
                let _permit = self.sem.acquire().await.expect("semaphore is never closed");
                let context = match &store {
                    Some(st) => self.context_for(st, &part, &chunk).await,
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

    /// One record through the model: the reply, or the error, added under the
    /// node's name.
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

    /// Several records through the model in one request: each gets its
    /// answer, or the error, under the node's name, in the order given. A
    /// reply that cannot be read (a model that loops until its budget, or
    /// stops partway) is not the records' fault: the batch is split in two
    /// and each half asked again with the same context, down to single
    /// records, which fail on their own terms.
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
            // Only a reply that came back and cannot be read is the batch's
            // fault; a request that failed outright fails the same in halves.
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

    /// The history a request is read with: one view per scope and key (the
    /// keys the request's records carry, busiest first), each the latest
    /// `max_events` records before the request's earliest one, however long
    /// ago, read newest first from the `store` sink's index (or, for a scope
    /// that is the partition, the partition's rows); each line the configured
    /// fields, a record shown once across views. None without `context`, a
    /// time, or history.
    async fn context_for(
        &self,
        store: &Arc<dyn StateStore>,
        part: &str,
        records: &[Value],
    ) -> Option<String> {
        let cfg = self.cfg.context.as_ref()?;
        let before = records
            .iter()
            .filter_map(|v| crate::windows::time_of_value(v, &cfg.time))
            .min()?;
        // What came before the request's records: the store also holds what
        // arrived while their buffer waited to be judged.
        let bound = crate::windows::ev_bound(before);
        let mut shown: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut lines = vec![format!(
            "History before these records, one JSON object per line: `scope` and `key` say whose \
             (a record's own fields name them: {}), `recent` is their latest {} records before these, \
             however long ago, oldest first:",
            cfg.scopes
                .iter()
                .map(|(scope, field)| format!("`{field}` for {scope}"))
                .collect::<Vec<_>>()
                .join(", "),
            cfg.max_events
        )];
        let mut bytes = lines[0].len();
        for (scope, field) in &cfg.scopes {
            // The keys this request's records carry, busiest first.
            let mut keys: Vec<(String, usize)> = Vec::new();
            for v in records {
                let Some(k) = lookup(v, field).and_then(crate::windows::scope_value) else { continue };
                match keys.iter_mut().find(|(have, _)| *have == k) {
                    Some((_, n)) => *n += 1,
                    None => keys.push((k, 1)),
                }
            }
            keys.sort_by_key(|k| std::cmp::Reverse(k.1));
            for (key, _) in keys {
                // Newest first, before the request's records: a scope that is
                // the partition itself (the org of an org-partitioned store)
                // reads the partition's rows, any other its index.
                let whole = key == part;
                let (ns, from, to) = if whole {
                    (cfg.namespace.clone(), "ev:".to_string(), bound.clone())
                } else {
                    let prefix = crate::windows::index_prefix(scope, &key);
                    (crate::windows::index_ns(&cfg.namespace), format!("{prefix}ev:"), format!("{prefix}{bound}"))
                };
                let entries = match store.range_rev(&ns, part, &from, &to, cfg.max_events + shown.len()).await {
                    Ok(entries) => entries,
                    Err(e) => {
                        tracing::warn!(node = %self.name, "reading context: {e:#}");
                        return None;
                    }
                };
                // Until the view is full; a record another view showed, or one
                // past its retention, is skipped.
                let mut recent: Vec<(i64, Value)> = Vec::new();
                for e in entries {
                    if recent.len() >= cfg.max_events {
                        break;
                    }
                    let (row, text) = if whole {
                        let StateValue::Str(text) = e.value else { continue };
                        (e.key, text)
                    } else {
                        let StateValue::Str(row) = e.value else { continue };
                        let Ok(Some(StateValue::Str(text))) = store.get(&cfg.namespace, part, &row).await else { continue };
                        (row, text)
                    };
                    if shown.contains(&row) {
                        continue;
                    }
                    let (Ok(v), Some(ts)) = (serde_json::from_str::<Value>(&text), crate::windows::ts_of_key(&row)) else { continue };
                    shown.insert(row);
                    recent.push((ts, self.history_line(&v, cfg)));
                }
                if recent.is_empty() {
                    continue;
                }
                recent.reverse();
                let line = json!({
                    "scope": scope,
                    "key": key,
                    "from": crate::windows::iso(recent[0].0),
                    "to": crate::windows::iso(recent[recent.len() - 1].0),
                    "recent": recent.into_iter().map(|(_, v)| v).collect::<Vec<_>>(),
                })
                .to_string();
                if bytes + line.len() > cfg.max_bytes && lines.len() > 1 {
                    break;
                }
                bytes += line.len();
                lines.push(line);
            }
        }
        (lines.len() > 1).then(|| lines.join("\n"))
    }

    /// A stored record as one line of history: the configured fields, or
    /// all of it.
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

    /// Records in request-sized runs: at most `batch` of them and about
    /// `batch_bytes` of JSON each; a record larger than that goes alone.
    fn chunks(&self, records: Vec<Value>) -> Vec<Vec<Value>> {
        // As many as the request's reply budget has room to answer.
        let by_budget = (self.cfg.max_request_tokens / self.cfg.max_tokens.max(1)).max(1) as usize;
        let (max_n, max_bytes) = (self.cfg.batch.max(1).min(by_budget), self.cfg.batch_bytes.max(1));
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
    /// `n` records. The prompt says what a record's answer is; the node says
    /// how the reply carries it: the answer alone, or every answer in
    /// `{"answers": [...]}` behind its record's index. The schema, if any,
    /// is wrapped the same way.
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
        // `max_tokens` is per record: a batch of n may say n times as much,
        // up to the request's budget.
        let budget = (u64::from(self.cfg.max_tokens) * batch.unwrap_or(1) as u64)
            .min(u64::from(self.cfg.max_request_tokens.max(self.cfg.max_tokens)));
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

/// `{"answers": [item, item, ...]}`, each `item` plus its record's index
/// `i`, so an answer says which record it is for and a skipped record
/// costs only itself (by position, every answer after it would land on
/// the wrong record). The count is in the prompt, not the schema: backends
/// that enforce schemas reject array bounds (Bedrock's structured output
/// does), and a short reply is caught when the answers are spread anyway.
/// An answer that takes one of several shapes (`anyOf`, `oneOf`: a pass
/// that is the flag alone, a flag that says why) gets the index in each, and
/// in its own properties if it has them; a shape that only refers to a
/// definition (`$ref`) gets a copy of it to hold the index, and the
/// definitions stay at the root, where references point. A schema with no
/// properties and no shapes is left as it is: any object.
fn answers_schema(item: &schemars::Schema, _n: usize) -> schemars::Schema {
    let mut item = serde_json::to_value(item).unwrap_or(Value::Null);
    let mut defs: Vec<(&str, Value)> = Vec::new();
    if let Some(obj) = item.as_object_mut() {
        for k in ["$defs", "definitions"] {
            if let Some(d) = obj.remove(k) {
                defs.push((k, d));
            }
        }
    }
    inline_ref(&mut item, &defs);
    if item.get("properties").is_some() {
        indexed(&mut item);
    }
    for k in ["anyOf", "oneOf"] {
        if let Some(shapes) = item.get_mut(k).and_then(Value::as_array_mut) {
            for shape in shapes.iter_mut() {
                inline_ref(shape, &defs);
                if shape.get("properties").is_some() || shape.get("type").and_then(Value::as_str) == Some("object") {
                    indexed(shape);
                }
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

/// A shape that is only a reference to a local definition (`#/$defs/X`),
/// replaced by a copy of it (keeping what else the shape says), so the index
/// can be added to it.
fn inline_ref(shape: &mut Value, defs: &[(&str, Value)]) {
    let Some(r) = shape.get("$ref").and_then(Value::as_str).map(str::to_string) else { return };
    for (k, d) in defs {
        let Some(def) = r.strip_prefix(&format!("#/{k}/")).and_then(|name| d.get(name)) else { continue };
        let mut copy = def.clone();
        if let (Some(c), Some(own)) = (copy.as_object_mut(), shape.as_object()) {
            for (key, v) in own {
                if key != "$ref" {
                    c.entry(key.clone()).or_insert_with(|| v.clone());
                }
            }
        }
        *shape = copy;
        return;
    }
}

/// An answer's shape with the record's index as its first property, and
/// required: the model writes which record it answers before the answer.
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
                    } else if batched && text_of(&req["messages"][1]["content"]).matches("loop").count() >= 3 {
                        // Three or more "loop" records in one batch: the
                        // model degenerates, as small ones do.
                        "{\"answers\": [{\"flag\": false, \"one_line\": \"a7a7a7a7a7a7a7a7".to_string()
                    } else if batched {
                        // One answer per line of the user message; "short"
                        // anywhere in the batch drops the last answer.
                        let user = text_of(&req["messages"][1]["content"]);
                        // Only the record lines; context lines and headers are not records.
                        let mut answers: Vec<_> = user
                            .lines()
                            .filter(|l| l.starts_with("{\"i\":"))
                            .map(verdict)
                            .collect();
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
            context: None,
            concurrency: 2,
            timeout_ms: 5_000,
            retries: 0,
            max_tokens: 50,
            max_request_tokens: 32_000,
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
        // Four "loop" records in one batch degenerate; two per half do not.
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
        // History in the partition, written as the store sink writes it: four
        // of a's (one two weeks back, one three days back), one of b's, one of
        // another org member's, and one after the request's records (not
        // history).
        let t0 = 1_793_106_000_000i64; // 2026-10-27T13:00:00Z
        let mut recent = BTreeMap::new();
        recent.insert("actor".to_string(), "_norm.actor".to_string());
        recent.insert("org".to_string(), "_norm.org".to_string());
        let sink = crate::sinks::store::StoreSink::new(
            &tangent_shared::sinks::store::StoreSinkConfig { namespace: "events".into(), retain: "30d".into(), time: "ts".into(), recent },
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
        // A busy stretch after the request's records, as a buffer waiting
        // to be judged sees: six hundred of a's. Its history is untouched.
        for i in 0..600i64 {
            let row = json!({"ts": crate::windows::iso(t0 + 60_000 + i * 1000), "_norm": {"actor": "a", "org": "acme", "kind": "later.burst"}});
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
            namespace: "events".into(),
            scopes,
            window: BTreeMap::new(),
            max_events: 3,
            max_bytes: 40_000,
            fields: vec!["ts".into(), "_norm.kind".into()],
            time: "ts".into(),
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
        assert!(outs.iter().all(|o| o.get("_window").is_none()), "nothing is attached to the records");
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
        assert!(schema["properties"]["answers"].get("minItems").is_none(), "no array bounds: backends reject them");
        let item = &schema["properties"]["answers"]["items"];
        assert_eq!(item["properties"]["i"]["type"], "integer", "each answer carries its record's index: {item}");
        assert!(item["required"].as_array().unwrap().iter().any(|r| r == "i"), "{item}");
        let first = item["properties"].as_object().unwrap().keys().next().unwrap();
        assert_eq!(first, "i", "the index comes before the answer: {item}");
        let _ = std::fs::remove_dir_all(dir);
    }

    /// Shapes beyond the pack's own: properties beside anyOf, references to
    /// definitions, oneOf and anyOf together, and no structure at all.
    #[test]
    fn the_index_reaches_every_shape_an_answer_can_take() {
        let schema = |v: Value| -> Value {
            let s: schemars::Schema = serde_json::from_value(v).unwrap();
            serde_json::to_value(answers_schema(&s, 2)).unwrap()
        };
        // Shared properties at the top, anyOf only constraining.
        let s = schema(json!({"type": "object", "properties": {"flag": {"type": "boolean"}, "why": {"type": "string"}},
                              "additionalProperties": false, "anyOf": [{"required": ["flag"]}]}));
        let item = &s["properties"]["answers"]["items"];
        assert!(item["properties"].get("i").is_some(), "the top level admits i: {item}");
        // A branch that is a reference: inlined, indexed; definitions at the root.
        let s = schema(json!({"$defs": {"Pass": {"type": "object", "properties": {"flag": {"const": false}}, "additionalProperties": false}},
                              "anyOf": [{"$ref": "#/$defs/Pass"}, {"type": "object", "properties": {"flag": {"const": true}}}]}));
        let shapes = s["properties"]["answers"]["items"]["anyOf"].as_array().unwrap();
        assert!(shapes.iter().all(|b| b["properties"].get("i").is_some()), "{s}");
        assert!(s.get("$defs").is_some(), "definitions stay where references point: {s}");
        // Both anyOf and oneOf.
        let s = schema(json!({"anyOf": [{"type": "object", "properties": {"a": {}}}], "oneOf": [{"type": "object", "properties": {"b": {}}}]}));
        let item = &s["properties"]["answers"]["items"];
        assert!(item["anyOf"][0]["properties"].get("i").is_some() && item["oneOf"][0]["properties"].get("i").is_some(), "{item}");
        // No structure: left free-form.
        let s = schema(json!({"description": "anything"}));
        assert!(s["properties"]["answers"]["items"].get("properties").is_none(), "{s}");
    }

    /// A pass that is the flag alone and a flag that says why: each shape
    /// gets the index, first, and keeps what it allows.
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
        let schema = serde_json::to_value(answers_schema(&verdict, 3)).unwrap();
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

    #[tokio::test]
    async fn a_request_takes_no_more_records_than_its_reply_budget_answers() {
        let (url, seen) = stub::model().await;
        let dir = stub::dir("reply-budget");
        let mut cfg = stub::config(&url);
        cfg.batch = 10;
        cfg.max_tokens = 50;
        cfg.max_request_tokens = 100;
        let node = InferenceNode::new(Arc::from("judge"), cfg, &dir).unwrap();
        let outs = node.annotate_all((1..=5).map(|i| json!({"id": i})).collect()).await;
        assert_eq!(outs.len(), 5);
        let seen = seen.lock().await;
        assert_eq!(seen.len(), 3, "two records per request: 2 + 2 + 1");
        for body in seen.iter() {
            let req: Value = serde_json::from_str(body).unwrap();
            assert!(req["max_tokens"].as_u64().unwrap() <= 100, "{req}");
        }
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
