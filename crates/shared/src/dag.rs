use std::sync::Arc;

use serde::{Deserialize, Serialize};

/// A node as the runtime routes between them: its kind and its name. A
/// config names a node by its name alone (see [`Edge`]); the kind is that of
/// the map defining it, `sources`, `plugins`, `inference` or `sinks`.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum NodeRef {
    Source {
        name: Arc<str>,
    },
    Plugin {
        name: Arc<str>,
    },
    /// An inference node: a model reads each record (see `inference`).
    Inference {
        name: Arc<str>,
    },
    Sink {
        name: Arc<str>,
    },
}

/// An edge as a config writes it: a node's name and the names it sends to.
/// A name is one node: no source, plugin, inference node and sink share
/// one.
#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(deny_unknown_fields)]
pub struct Edge {
    pub from: Arc<str>,
    pub to: Vec<Arc<str>>,
}
