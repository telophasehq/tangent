use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::path::PathBuf;

/// Ask a model about each record. The node sends `prompt_file` as the system
/// message and the record (or the listed `fields`) as the user message, and
/// adds the JSON object the model replies with to the record under `output`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct InferenceConfig {
    /// Where the model is served.
    #[serde(default)]
    pub backend: Backend,

    /// Base URL of the chat completions API (`openai` backend).
    #[serde(default = "default_url")]
    pub url: String,

    /// Model id as the backend knows it, e.g. `mlx-community/GLM-4.7-Flash-4bit`
    /// or `us.anthropic.claude-haiku-4-5-20251001-v1:0`.
    pub model: String,

    /// AWS region for the `bedrock` backend; the SDK's default when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,

    /// AWS profile for the `bedrock` backend; the default credential chain
    /// when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,

    /// System prompt (relative to the config file).
    pub prompt_file: PathBuf,

    /// What the model is asked to reply with.
    #[serde(default)]
    pub answer: Answer,

    /// Record fields to show the model; the whole record when empty.
    #[serde(default)]
    pub fields: Vec<String>,

    /// Name of the object added to the record with the model's reply.
    #[serde(default = "default_output")]
    pub output: String,

    /// Forward a record only when this field of the reply is true (for
    /// example `flag`); the rest are dropped. A reply without the field is
    /// treated like a failed request (see `on_error`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub forward_if: Option<String>,

    /// Requests in flight at once.
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,

    /// A request past this is a failure (see `on_error`); it also frees the
    /// concurrency slot it held. 30 s by default; a local model may need more.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,

    #[serde(default = "default_max_tokens")]
    pub max_tokens: u32,

    #[serde(default)]
    pub temperature: f64,

    /// Extra request parameters merged into the body, e.g.
    /// `chat_template_kwargs: {enable_thinking: false}` for mlx_lm.server or
    /// `reasoning_effort: low` for gpt-oss.
    #[serde(default)]
    pub extra: Map<String, Value>,

    /// Bearer token for the `openai` backend, e.g. "${OPENAI_API_KEY}".
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub api_key: Option<String>,

    /// What happens to a record when the model cannot be asked, or replies
    /// without the `forward_if` field.
    #[serde(default)]
    pub on_error: OnError,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Backend {
    /// An OpenAI-compatible chat completions endpoint at `url`: OpenAI,
    /// mlx_lm.server, llama-server, vLLM, Bedrock's OpenAI endpoint, ...
    #[default]
    Openai,
    /// Amazon Bedrock through the Converse API, with the AWS credential chain.
    Bedrock,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Answer {
    /// A JSON object, merged into the record under `output`; `forward_if`
    /// names the field that lets a record through.
    #[default]
    Json,
    /// One word: `yes` forwards the record, `no` drops it, anything else is a
    /// failure (see `on_error`). The word lands under `output` as
    /// `{"answer": ...}`. Cheap enough to read a whole stream: a first pass
    /// in front of a slower model.
    YesNo,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, Default, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum OnError {
    /// Forward the record with `{"error": ...}` under `output`.
    #[default]
    Pass,
    /// Drop the record.
    Drop,
}

fn default_url() -> String {
    "http://localhost:8080/v1".into()
}
fn default_output() -> String {
    "_inference".into()
}
const fn default_concurrency() -> usize {
    1
}
const fn default_timeout_ms() -> u64 {
    30_000
}
const fn default_max_tokens() -> u32 {
    2_000
}
