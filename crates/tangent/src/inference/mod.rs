//! Inference nodes: a model reads each record. A node lives inside each
//! worker and runs in DAG order with the plugins: the worker hands it the
//! records that reached it, waits for the replies (`concurrency` at a time),
//! and carries the annotated records on to the node's outputs. The system
//! prompt is a file; the user message is the record (or selected fields) as
//! pretty JSON; the reply is added to the record under the node's name. The
//! node adds and forwards, never drops: a failure lands as `{"error": ...}`
//! in the same field, and a plugin downstream decides what goes on. A stop
//! is not a failure: a signal ends the node's calls, and the records still
//! waiting on one get no answer at all ([`Stopped`]).
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
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tangent_shared::inference::{Backend, ContextConfig, InferenceConfig};

use crate::state::{Entry, StateStore, StateValue};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

/// Records judged together: one partition, one window.
type GroupKey = (Arc<str>, String);

/// A request: its partition, the records' places in the order given, the
/// records, and the rows they were read from when each says (`_key`).
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
    /// Cancelled at a stop: no call starts or is tried again after it, and
    /// one under way ends.
    stop: CancellationToken,
}

/// A stop ended the model call before it answered. The records get no
/// answer rather than an error: an error reads downstream as a record the
/// model could not judge, and at a stop every record in flight would be one.
#[derive(Debug)]
pub struct Stopped;

impl std::fmt::Display for Stopped {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("stopped before the model answered")
    }
}

impl std::error::Error for Stopped {}

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
            stop: CancellationToken::new(),
        })
    }

    /// The node's calls end when `stop` is cancelled, rather than finish or
    /// try again: see [`Stopped`].
    pub fn stops_with(mut self, stop: CancellationToken) -> Self {
        self.stop = stop;
        self
    }

    /// Every record through the model, under the node's permits; the
    /// annotated records, in the order given. No partition, no context.
    pub async fn annotate_all(&self, records: Vec<Value>) -> Result<Vec<Value>, Stopped> {
        let part: Arc<str> = Arc::from("");
        self.annotate_in(records.into_iter().map(|v| (v, Arc::clone(&part))).collect(), None)
            .await
    }

    /// Records with their partitions through the model: grouped by
    /// partition and by buffer (`_buffer.from`, when the records came from a
    /// `store` source), each group in requests of `batch`, each request read
    /// with the history before its earliest record, and the rest of its
    /// buffer, when `context` is set and a store is given. The annotated
    /// records, in the order given; none when a stop ended a call.
    pub async fn annotate_in(
        &self,
        records: Vec<(Value, Arc<str>)>,
        store: Option<Arc<dyn StateStore>>,
    ) -> Result<Vec<Value>, Stopped> {
        let n = records.len();
        // The row a `store` source read each record from (`_key`) tells a
        // request's own records from the rest of their buffer. The model,
        // and what comes after the node, see the record without it.
        let mut rows: Vec<Option<String>> = Vec::with_capacity(n);
        let mut groups: Vec<(GroupKey, Vec<(usize, Value)>)> = Vec::new();
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
            let chunks = if self.cfg.batch <= 1 {
                values.into_iter().map(|v| vec![v]).collect()
            } else {
                self.chunks(values)
            };
            let mut at = 0;
            for chunk in chunks {
                let these = ids[at..at + chunk.len()].to_vec();
                at += chunk.len();
                let own = these.iter().map(|&i| rows[i].clone()).collect();
                jobs.push((Arc::clone(&part), these, chunk, own));
            }
        }
        let done = futures::future::join_all(jobs.into_iter().map(|(part, ids, chunk, own)| {
            let store = store.clone();
            async move {
                let _permit = tokio::select! {
                    biased;
                    () = self.stop.cancelled() => return Err(Stopped),
                    permit = self.sem.acquire() => permit.expect("semaphore is never closed"),
                };
                let context = match &store {
                    Some(st) => self.context_for(st, &part, &chunk, own).await,
                    None => None,
                };
                let out = if self.cfg.batch <= 1 {
                    let v = chunk.into_iter().next().expect("a chunk of one");
                    vec![self.annotate_one(v, context).await?]
                } else {
                    self.annotate_batch(chunk, context).await?
                };
                Ok((ids, out))
            }
        }))
        .await;
        let mut slots: Vec<Option<Value>> = (0..n).map(|_| None).collect();
        for job in done {
            let (ids, out) = job?;
            for (i, v) in ids.into_iter().zip(out) {
                slots[i] = Some(v);
            }
        }
        Ok(slots.into_iter().map(|s| s.unwrap_or(Value::Null)).collect())
    }

    /// One record through the model: the reply, or the error, added under the
    /// node's name.
    pub async fn annotate(&self, v: Value) -> Result<Value, Stopped> {
        self.annotate_one(v, None).await
    }

    async fn annotate_one(&self, v: Value, context: Option<String>) -> Result<Value, Stopped> {
        let start = Instant::now();
        let user = match context {
            Some(c) => format!("{c}\n\nThe record:\n{}", self.user_message(&v)),
            None => self.user_message(&v),
        };
        let reply = match self.ask(user, None).await {
            Ok(reply) => reply,
            Err(e) if e.is::<Stopped>() => return Err(Stopped),
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
        Ok(attach(&self.name, v, reply))
    }

    /// Several records through the model in one request: each gets its
    /// answer, or the error, under the node's name, in the order given. A
    /// reply that cannot be read (a model that loops until its budget, or
    /// stops partway) is not the records' fault: the batch is split in two
    /// and each half asked again with the same context, down to single
    /// records, which fail on their own terms.
    async fn annotate_batch(
        &self,
        records: Vec<Value>,
        context: Option<String>,
    ) -> Result<Vec<Value>, Stopped> {
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
            Err(e) if e.is::<Stopped>() => return Err(Stopped),
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
                let mut out = Box::pin(self.annotate_batch(records, context.clone())).await?;
                out.extend(Box::pin(self.annotate_batch(tail, context)).await?);
                return Ok(out);
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
        Ok(records
            .into_iter()
            .zip(answers)
            .map(|(v, a)| attach(&self.name, v, a))
            .collect())
    }

    /// The history a request is read with: one view per scope and key (the
    /// keys the request's records carry, busiest first), each the latest
    /// `max_events` records before the request's earliest one, however long
    /// ago, read newest first from the `store` sink's index (or, for a scope
    /// that is the partition, the partition's rows). When the request knows
    /// its own rows (`own`, from the records' `_key`), each key also gets the
    /// rest of its buffer: its first `max_events` records from the earliest
    /// one to the buffer's end (`_buffer.to`, else the latest record) that
    /// are not the request's. Each line the configured fields, a record shown
    /// once across views. None without `context`, a time, or history.
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
            .filter_map(|v| crate::windows::time_of_value(v, &cfg.time))
            .collect();
        let (before, latest) = (*times.iter().min()?, *times.iter().max()?);
        // What came before the request's records: the store also holds what
        // arrived while their buffer waited to be judged.
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
            'keys: for (key, _) in keys {
                // A scope that is the partition itself (the org of an
                // org-partitioned store) reads the partition's rows, any
                // other its index.
                let whole = key == part;
                let (ns, prefix) = if whole {
                    (cfg.namespace.clone(), String::new())
                } else {
                    (crate::windows::index_ns(&cfg.namespace), crate::windows::index_prefix(scope, &key))
                };
                // Newest first, before the request's records; then, oldest
                // first, the rest of their buffer.
                for rest in [false, true] {
                    let limit = cfg.max_events + shown.len();
                    let entries = match (rest, &rest_end) {
                        (false, _) => {
                            store.range_rev(&ns, part, &format!("{prefix}ev:"), &format!("{prefix}{bound}"), limit).await
                        }
                        (true, Some(end)) => {
                            store.range(&ns, part, &format!("{prefix}{bound}"), &format!("{prefix}{end}"), limit).await
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
                    if bytes + line.len() > cfg.max_bytes && lines.len() > 1 {
                        break 'keys;
                    }
                    bytes += line.len();
                    lines.push(line);
                }
            }
        }
        (lines.len() > 1).then(|| lines.join("\n"))
    }

    /// A view's records from `entries`, in the order read (the partition's
    /// rows when `whole`, else index entries naming them), as (time, history
    /// line), until the view is full; a record already shown, or one past
    /// its retention, is skipped.
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
            // A partition's row is the record; an index entry names its row.
            let (row, text) = if whole { (e.key, Some(value)) } else { (value, None) };
            if shown.contains(&row) {
                continue;
            }
            let text = match text {
                Some(text) => text,
                None => match store.get(&cfg.namespace, part, &row).await {
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
    /// that got no reply. A stop ends the wait for the reply, or for the next
    /// try, with [`Stopped`]; after one, no call starts.
    async fn ask(&self, user: String, batch: Option<usize>) -> Result<Value> {
        let attempts = self.cfg.retries.saturating_add(1);
        let mut attempt = 0;
        let text = loop {
            attempt += 1;
            let reply = tokio::select! {
                biased;
                () = self.stop.cancelled() => return Err(Stopped.into()),
                reply = self.call(user.clone(), batch) => reply,
            };
            match reply {
                Ok(text) => break text,
                Err(e) if attempt < attempts => {
                    let wait = Duration::from_secs(1 << (attempt - 1).min(6));
                    tracing::warn!(
                        node = %self.name,
                        attempt,
                        "inference failed, retrying in {wait:?}: {e:#}"
                    );
                    tokio::select! {
                        biased;
                        () = self.stop.cancelled() => return Err(Stopped.into()),
                        () = tokio::time::sleep(wait) => {}
                    }
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

    /// A model that takes each request and never answers: its url, and how
    /// many requests it has taken.
    pub async fn silent() -> (String, Arc<AtomicUsize>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/v1", listener.local_addr().unwrap());
        let taken = Arc::new(AtomicUsize::new(0));
        let count = Arc::clone(&taken);
        tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                count.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async move {
                    let mut tmp = [0u8; 4096];
                    while sock.read(&mut tmp).await.unwrap_or(0) > 0 {}
                });
            }
        });
        (url, taken)
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
            batch_age_ms: 1_000,
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
            .await.unwrap();
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
        let out = node.annotate(json!({"id": 1, "msg": "bad"})).await.unwrap();
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
        let out = node.annotate(json!({"id": 7, "msg": "prose please"})).await.unwrap();
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
            .await.unwrap();
        assert_eq!(out["judge"]["flag"], true, "{out}");
        assert_eq!(seen.lock().await.len(), 3);
        let _ = std::fs::remove_dir_all(dir);
    }

    /// A stop ends a call that waits on the model, and after it no call
    /// starts: the records get no answer, not an error.
    #[tokio::test]
    async fn a_stop_ends_the_calls_and_nothing_is_asked_after() {
        let (url, taken) = stub::silent().await;
        let dir = stub::dir("stop");
        let mut cfg = stub::config(&url);
        cfg.timeout_ms = 60_000;
        cfg.retries = 2;
        cfg.concurrency = 1;
        let stop = CancellationToken::new();
        let node = Arc::new(
            InferenceNode::new(Arc::from("judge"), cfg, &dir)
                .unwrap()
                .stops_with(stop.clone()),
        );
        let judging = tokio::spawn({
            let node = Arc::clone(&node);
            async move { node.annotate_all(vec![json!({"id": 1}), json!({"id": 2})]).await }
        });
        // The first record's call is under way; the second waits for the permit.
        wait_for_a_request(&taken).await;
        stop.cancel();
        let out = tokio::time::timeout(Duration::from_secs(2), judging)
            .await
            .expect("the stop ends the wait for the reply")
            .unwrap();
        assert!(matches!(out, Err(Stopped)), "{out:?}");
        assert!(matches!(node.annotate(json!({"id": 3})).await, Err(Stopped)));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert_eq!(
            taken.load(std::sync::atomic::Ordering::SeqCst),
            1,
            "no call for the records that waited or came after"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    async fn wait_for_a_request(taken: &std::sync::atomic::AtomicUsize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while taken.load(std::sync::atomic::Ordering::SeqCst) == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("the model is asked");
    }

    /// A request of several records ends the same way.
    #[tokio::test]
    async fn a_stop_ends_a_batched_request() {
        let (url, taken) = stub::silent().await;
        let dir = stub::dir("stop-batch");
        let mut cfg = stub::config(&url);
        cfg.timeout_ms = 60_000;
        cfg.batch = 4;
        let stop = CancellationToken::new();
        let node = InferenceNode::new(Arc::from("judge"), cfg, &dir)
            .unwrap()
            .stops_with(stop.clone());
        let part: Arc<str> = Arc::from("acme");
        let records = (1..=3).map(|i| (json!({"id": i}), Arc::clone(&part))).collect();
        tokio::spawn({
            let (stop, taken) = (stop.clone(), Arc::clone(&taken));
            async move {
                wait_for_a_request(&taken).await;
                stop.cancel();
            }
        });
        let out = tokio::time::timeout(Duration::from_secs(5), node.annotate_in(records, None))
            .await
            .expect("the stop ends the wait for the reply");
        assert!(matches!(out, Err(Stopped)), "{out:?}");
        assert_eq!(taken.load(std::sync::atomic::Ordering::SeqCst), 1, "one request for the three");
        let _ = std::fs::remove_dir_all(dir);
    }

    /// A stop ends the wait for a permit: with every permit held, no call
    /// starts and the records get no answer.
    #[tokio::test]
    async fn a_stop_ends_the_wait_for_a_permit() {
        let (url, taken) = stub::silent().await;
        let dir = stub::dir("stop-permit");
        let mut cfg = stub::config(&url);
        cfg.concurrency = 1;
        let stop = CancellationToken::new();
        let node = Arc::new(
            InferenceNode::new(Arc::from("judge"), cfg, &dir)
                .unwrap()
                .stops_with(stop.clone()),
        );
        let held = node.sem.acquire().await.unwrap();
        let judging = tokio::spawn({
            let node = Arc::clone(&node);
            async move { node.annotate_all(vec![json!({"id": 1})]).await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!judging.is_finished(), "it waits for the permit");
        stop.cancel();
        let out = tokio::time::timeout(Duration::from_secs(2), judging)
            .await
            .expect("the stop ends the wait for the permit")
            .unwrap();
        assert!(matches!(out, Err(Stopped)), "{out:?}");
        drop(held);
        assert_eq!(taken.load(std::sync::atomic::Ordering::SeqCst), 0);
        let _ = std::fs::remove_dir_all(dir);
    }

    /// A stop also ends the wait before the next try.
    #[tokio::test]
    async fn a_stop_ends_the_wait_between_tries() {
        let dir = stub::dir("stop-retry");
        let mut cfg = stub::config("http://127.0.0.1:1/v1");
        cfg.retries = 6; // 63 s of waits between the tries
        let stop = CancellationToken::new();
        let node = InferenceNode::new(Arc::from("judge"), cfg, &dir)
            .unwrap()
            .stops_with(stop.clone());
        tokio::spawn({
            let stop = stop.clone();
            async move {
                tokio::time::sleep(Duration::from_millis(200)).await;
                stop.cancel();
            }
        });
        let started = Instant::now();
        let out = node.annotate(json!({"id": 1})).await;
        assert!(matches!(out, Err(Stopped)), "{out:?}");
        assert!(started.elapsed() < Duration::from_secs(1), "{:?}", started.elapsed());
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
        let out = node.annotate(json!({"id": 9})).await.unwrap();
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
            .await.unwrap();
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
            .await.unwrap();
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
            &tangent_shared::sinks::store::StoreSinkConfig { namespace: "events".into(), retain: "30d".into(), time: "ts".into(), recent, novel: Default::default(), follow: None },
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
            .await.unwrap();
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
            &tangent_shared::sinks::store::StoreSinkConfig { namespace: "events".into(), retain: "30d".into(), time: "ts".into(), recent, novel: Default::default(), follow: None },
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
            .await.unwrap();
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
        node.annotate_in(late, Some(Arc::clone(&store))).await.unwrap();
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
        node.annotate_in(unkeyed, Some(Arc::clone(&store))).await.unwrap();
        let body = seen.lock().await[2].clone();
        assert!(!body.contains("rest_of_buffer"), "{body}");
        assert!(body.contains("History before these records, one JSON object per line"), "the history as before: {body}");
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
            .await.unwrap();
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
            .await.unwrap();
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
        let outs = node.annotate_all((1..=5).map(|i| json!({"id": i})).collect()).await.unwrap();
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
            .await.unwrap();
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
