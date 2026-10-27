//! Native DAG nodes, implemented in the runtime: work a sandboxed plugin
//! cannot do well, such as asking a model about a record.

use serde::{Deserialize, Serialize};

pub mod inference;

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProcessorConfig {
    Inference(inference::InferenceConfig),
}
