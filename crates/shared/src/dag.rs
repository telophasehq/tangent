use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[serde(tag = "kind", rename_all = "lowercase")]
pub enum NodeRef {
    Source {
        name: String,
    },
    Plugin {
        name: String,
    },
    Sink {
        name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        key_prefix: Option<String>,
    },
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Edge {
    pub from: NodeRef,
    pub to: Vec<NodeRef>,
}
