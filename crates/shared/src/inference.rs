use secrecy::SecretString;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::path::PathBuf;
pub use tangent_compact::{CompactConfig, SourceSpec};

/// Ask a model about each record. The node sends `prompt_file` as the system
/// message and the records it is handed (or their listed `fields`), a JSON
/// or logfmt line each, as the user message, and adds each record's answer to it under
/// the node's name. It adds and forwards; it never drops. What happens next
/// is a plugin's call: a plugin whose `probe` selects on `<node name>.flag`
/// is that filter.
///
/// The node runs inside each worker, in DAG order with the plugins, so a
/// partition's records reach the model and leave it in order.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct InferenceConfig {
    /// Where the model is served.
    #[serde(default)]
    pub backend: Backend,

    /// Base URL of the chat completions API (`openai` backend).
    #[serde(default = "default_url")]
    pub url: String,

    /// Bearer token for the `openai` backend, e.g. "${OPENAI_API_KEY}".
    #[serde(default, skip_serializing)]
    pub api_key: Option<SecretString>,

    /// Model id as the backend knows it, e.g. `mlx-community/GLM-4.7-Flash-4bit`
    /// or `us.anthropic.claude-haiku-4-5-20251001-v1:0`.
    pub model: String,

    /// AWS settings for the `bedrock` backend.
    #[serde(default)]
    pub aws: AwsConfig,

    /// System prompt (relative to the file that names it).
    pub prompt_file: PathBuf,

    /// A JSON Schema (relative to the file that names it) the reply must satisfy,
    /// enforced by the backend: `response_format` on an OpenAI-compatible
    /// server, structured output on Bedrock. Without one, an `openai` server
    /// is asked for JSON mode (any object) and a `bedrock` model is held to
    /// the prompt alone. Either way the node reads the reply as JSON and
    /// nothing else: a reply that is not a JSON object lands as the error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_file: Option<PathBuf>,

    /// Record fields to show the model; the whole record when empty.
    #[serde(default)]
    pub fields: Vec<String>,

    #[serde(default = "default_batch_bytes")]
    pub batch_bytes: usize, // a request past this is split

    /// How the records are written into the user message: `ndjson`, a JSON
    /// line each (the default), or `logfmt`, the same records, every field
    /// and value, a logfmt line each in about half the tokens, also split at
    /// `budget_tokens`.
    #[serde(default)]
    pub encoding: Encoding,

    #[serde(default)]
    pub group_by: Vec<String>, // a request holds one value of these

    /// Requests in flight at once, per worker: the model sees at most
    /// `runtime.workers` times this from one node.
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,

    /// A request past this is a failure; it also frees the concurrency slot it
    /// held. 30 s by default; a local model may need more.
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: u64,

    /// Attempts after the first for a request that got no reply: a timeout, a
    /// connection or server error, an empty reply. They back off 1 s, 2 s,
    /// 4 s, ... A reply that came back but is not JSON is not retried; it
    /// lands as the error.
    #[serde(default = "default_retries")]
    pub retries: u32,

    #[serde(default = "default_max_tokens")]
    pub max_tokens: u32, // per record

    /// Sent only when set; the model's default otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,

    /// Extra request parameters merged into the body, e.g.
    /// `chat_template_kwargs: {enable_thinking: false}` for mlx_lm.server or
    /// `reasoning_effort: low` for gpt-oss. These win over what the node sets.
    #[serde(default)]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum Encoding {
    #[default]
    Ndjson,
    Logfmt(CompactConfig),
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

/// The `bedrock` backend's AWS settings.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct AwsConfig {
    /// Region; the SDK's default when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    /// Profile; the default credential chain when unset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub profile: Option<String>,
}

fn default_url() -> String {
    "http://localhost:8080/v1".into()
}
const fn default_concurrency() -> usize {
    1
}
const fn default_batch_bytes() -> usize {
    32_768
}
const fn default_timeout_ms() -> u64 {
    30_000
}
const fn default_retries() -> u32 {
    2
}
const fn default_max_tokens() -> u32 {
    2_000
}
