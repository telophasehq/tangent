use ahash::HashMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;

use crate::enrichment::TableConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginConfig {
    pub module_type: String,
    pub path: PathBuf,

    #[serde(default)]
    pub tests: Vec<PluginTests>,

    #[serde(default)]
    pub config: HashMap<String, Value>,

    /// Namespace of this plugin's state. Unset, every plugin shares one
    /// (`plugins`); set it to give this plugin, or a group of plugins, keys
    /// of their own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_namespace: Option<String>,

    /// A directory of files the plugin may read, relative to the file that names it,
    /// mounted read-only in the guest at `/data`. For reference data too big
    /// for `config`: an IP database, published address ranges, an org profile.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<PathBuf>,

    /// When a call fails (the guest traps or returns an error), its records
    /// are dropped; false passes them on unchanged, as if the plugin were
    /// not there.
    #[serde(default = "default_true")]
    pub drop_on_error: bool,
}

const fn default_true() -> bool {
    true
}

impl Default for PluginConfig {
    fn default() -> Self {
        Self {
            module_type: String::new(),
            path: PathBuf::new(),
            tests: Vec::new(),
            config: HashMap::default(),
            state_namespace: None,
            data: None,
            drop_on_error: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginTests {
    pub input: PathBuf,
    pub expected: PathBuf,

    /// Tables this test reads in place of the config's of the same name: the
    /// data its expected output was written against, so the fixture means the
    /// same in every config that includes it.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub enrichment_tables: BTreeMap<Arc<str>, TableConfig>,
}
