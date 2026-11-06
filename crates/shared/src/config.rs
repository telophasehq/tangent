use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use crate::dag::{Edge, NodeRef};
use crate::sinks::common::{Encoding, SinkConfig, SinkKind};
use crate::sources::common::SourceConfig;

pub mod dag;
pub mod enrichment;
pub mod include;
pub mod inference;
pub mod plugins;
pub mod runtime;
pub mod sinks;
pub mod sources;
pub mod wasm_engine;

#[derive(Debug, Deserialize, Serialize)]
pub struct Config {
    #[serde(default)]
    pub runtime: runtime::RuntimeConfig,

    #[serde(default)]
    pub sources: std::collections::BTreeMap<Arc<str>, SourceConfig>,

    #[serde(default)]
    pub sinks: std::collections::BTreeMap<Arc<str>, SinkConfig>,

    #[serde(default)]
    pub plugins: std::collections::BTreeMap<Arc<str>, plugins::PluginConfig>,

    /// Inference nodes, each with its own model and prompt, named in the DAG
    /// as any node is.
    #[serde(default)]
    pub inference: std::collections::BTreeMap<Arc<str>, inference::InferenceConfig>,

    /// Reference data loaded once per process and shared by every plugin,
    /// each looked up by its name.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub enrichment_tables: std::collections::BTreeMap<Arc<str>, enrichment::TableConfig>,

    #[serde(default)]
    pub dag: Vec<Edge>,
}

/// A config and the files it was read from.
pub struct Loaded {
    pub config: Config,
    /// Every file merged, each after the files it includes; the one named
    /// last.
    pub files: Vec<PathBuf>,
}

impl Config {
    pub fn from_file(path: &PathBuf) -> Result<Self> {
        Ok(Self::load(path)?.config)
    }

    /// The config at `path`, with what it includes (see [`include`]). A file
    /// without `include:` is read as it always was, but that a top-level key
    /// tangent does not know is an error (one starting `x-` aside).
    pub fn load(path: &Path) -> Result<Loaded> {
        let read = include::read(path)?;
        if !read.map.contains_key("include") {
            let mut config: Config = serde_yaml::from_str(&read.text)
                .with_context(|| format!("parsing YAML {}", path.display()))?;
            config.inference.values_mut().for_each(|n| n.default_url());
            config
                .check_names()
                .with_context(|| format!("loading {}", path.display()))?;
            return Ok(Loaded {
                config,
                files: vec![path.to_path_buf()],
            });
        }

        let merged = include::merge(path, read)?;
        let mut config: Config =
            serde_path_to_error::deserialize(serde_yaml::Deserializer::from_str(&merged.text))
                .map_err(|e| merged.explain(e))
                .with_context(|| format!("loading {}", path.display()))?;
        config.dag = dedupe(std::mem::take(&mut config.dag));
        config.inference.values_mut().for_each(|n| n.default_url());
        config
            .check_names()
            .with_context(|| format!("loading {}", path.display()))?;
        Ok(Loaded {
            config,
            files: merged.files,
        })
    }

    pub const fn batch_age_ms(&self) -> Duration {
        Duration::from_millis(self.runtime.batch_age)
    }

    pub const fn batch_size_kb(&self) -> usize {
        self.runtime.batch_size << 10
    }

    /// The node a name names: whichever of `sources`, `plugins`, `inference`
    /// and `sinks` defines it (they share no name: [`Self::check_names`]).
    pub fn node(&self, name: &str) -> Option<NodeRef> {
        if let Some((n, _)) = self.sources.get_key_value(name) {
            return Some(NodeRef::Source {
                name: Arc::clone(n),
            });
        }
        if let Some((n, _)) = self.plugins.get_key_value(name) {
            return Some(NodeRef::Plugin {
                name: Arc::clone(n),
            });
        }
        if let Some((n, _)) = self.inference.get_key_value(name) {
            return Some(NodeRef::Inference {
                name: Arc::clone(n),
            });
        }
        self.sinks.get_key_value(name).map(|(n, _)| NodeRef::Sink {
            name: Arc::clone(n),
        })
    }

    /// A name is one node, so that an edge can name a node without its
    /// kind: no source, plugin, inference node and sink share one. And it is
    /// letters, digits, `_` and `-`: a plugin's names its `.cwasm` file in
    /// plugins_path, and an inference node's is the field its reply goes in,
    /// which a `.` would split: `judge.v2.flag` is judge/v2/flag.
    pub fn check_names(&self) -> Result<()> {
        let mut bad: Vec<String> = Vec::new();
        let mut kinds: std::collections::BTreeMap<&str, Vec<&str>> = Default::default();
        let maps: [(&str, Vec<&Arc<str>>); 4] = [
            ("source", self.sources.keys().collect()),
            ("plugin", self.plugins.keys().collect()),
            ("inference node", self.inference.keys().collect()),
            ("sink", self.sinks.keys().collect()),
        ];
        for (kind, names) in maps {
            for n in names {
                if !is_name(n) {
                    bad.push(format!("{kind} `{n}`"));
                }
                kinds.entry(n).or_default().push(kind);
            }
        }
        if !bad.is_empty() {
            anyhow::bail!(
                "a name is letters, digits, `_` and `-` only; rename:\n  - {}",
                bad.join("\n  - ")
            );
        }
        let shared: Vec<String> = kinds
            .iter()
            .filter(|(_, k)| k.len() > 1)
            .map(|(n, k)| format!("`{n}` is a {}", k.join(" and a ")))
            .collect();
        if !shared.is_empty() {
            anyhow::bail!(
                "a name is one node, so an edge can name it; rename one of each:\n  - {}",
                shared.join("\n  - ")
            );
        }
        Ok(())
    }

    pub fn validate(&self) -> Result<()> {
        self.check_names()?;
        let mut missing: Vec<String> = Vec::new();
        let mut cannot: Vec<String> = Vec::new();
        for e in &self.dag {
            match self.node(&e.from) {
                None => missing.push(format!("from `{}` does not exist", e.from)),
                Some(NodeRef::Sink { .. }) => {
                    cannot.push(format!("from `{}`: a sink sends nothing on", e.from))
                }
                Some(_) => {}
            }
            for t in &e.to {
                match self.node(t) {
                    None => missing.push(format!("to `{t}` does not exist")),
                    Some(NodeRef::Source { .. }) => {
                        cannot.push(format!("to `{t}`: a source takes nothing in"))
                    }
                    Some(_) => {}
                }
            }
        }
        if !missing.is_empty() {
            anyhow::bail!(
                "DAG references missing nodes:\n  - {}",
                missing.join("\n  - ")
            );
        }
        cannot.extend(self.cycles());
        if !cannot.is_empty() {
            anyhow::bail!("DAG edges that cannot be:\n  - {}", cannot.join("\n  - "));
        }

        // a file sink appends each write's encoding to one file, which only
        // NDJSON survives: a Parquet, Avro or JSON file after another is not one
        for (name, sink) in &self.sinks {
            if let SinkKind::File(_) = &sink.kind {
                if !matches!(sink.common.encoding, Encoding::NDJSON) {
                    anyhow::bail!(
                        "sink '{name}': a file sink appends to one file, which only NDJSON \
                         survives; write {} with a rolling_file sink",
                        sink.common.encoding.extension()
                    );
                }
            }
        }

        Ok(())
    }

    /// What loads and runs but likely is not meant, for the caller to print
    /// (this crate does not log): a plugin, inference node or sink no edge
    /// reaches never runs, and a source no edge leaves is read and dropped,
    /// its records acked and a polling source's cursor moved past them. A
    /// plugin no edge leaves is fine: it may only write state.
    pub fn warnings(&self) -> Vec<String> {
        let mut warnings = Vec::new();
        for name in self.sources.keys() {
            if !self.dag.iter().any(|e| e.from == *name && !e.to.is_empty()) {
                warnings.push(format!(
                    "source {name}: no edge leaves it; its records are read and dropped"
                ));
            }
        }
        let fed: [(&str, Vec<&Arc<str>>); 3] = [
            ("plugin", self.plugins.keys().collect()),
            ("inference node", self.inference.keys().collect()),
            ("sink", self.sinks.keys().collect()),
        ];
        for (kind, names) in fed {
            for name in names {
                if !self.dag.iter().any(|e| e.to.contains(name)) {
                    warnings.push(format!("{kind} {name}: no edge reaches it; it never runs"));
                }
            }
        }
        warnings
    }

    /// Cycles among the plugins and inference nodes, each as `a -> b -> a`:
    /// one at least wherever the edges go round. A walk sends a node's
    /// records on along every edge until a sink, so a record on a cycle goes
    /// round it for good, through a model each time round. No source or sink
    /// is on one: no edge enters a source or leaves a sink.
    fn cycles(&self) -> Vec<String> {
        let inner = |n: &str| {
            matches!(
                self.node(n),
                Some(NodeRef::Plugin { .. } | NodeRef::Inference { .. })
            )
        };
        let mut outs: BTreeMap<&str, Vec<&str>> = BTreeMap::new();
        for e in self.dag.iter().filter(|e| inner(&e.from)) {
            outs.entry(&e.from)
                .or_default()
                .extend(e.to.iter().map(|t| &**t).filter(|t| inner(t)));
        }
        let mut done: HashSet<&str> = HashSet::new();
        let mut cycles = Vec::new();
        for node in outs.keys() {
            visit(node, &outs, &mut Vec::new(), &mut done, &mut cycles);
        }
        cycles
    }
}

/// Whether `name` can name a node: letters, digits, `_` and `-`, one at
/// least ([`Config::check_names`] says why).
pub fn is_name(name: &str) -> bool {
    !name.is_empty()
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
}

/// A depth-first walk from `node`, `path` the nodes it came through: an
/// edge back to one of them closes a cycle. A node `done` was walked from
/// already.
fn visit<'a>(
    node: &'a str,
    outs: &BTreeMap<&'a str, Vec<&'a str>>,
    path: &mut Vec<&'a str>,
    done: &mut HashSet<&'a str>,
    cycles: &mut Vec<String>,
) {
    if done.contains(node) {
        return;
    }
    if let Some(at) = path.iter().position(|p| *p == node) {
        let mut cycle = path[at..].to_vec();
        cycle.push(node);
        cycles.push(format!("a cycle: {}", cycle.join(" -> ")));
        return;
    }
    path.push(node);
    for to in outs.get(node).into_iter().flatten() {
        visit(to, outs, path, done, cycles);
    }
    path.pop();
    done.insert(node);
}

/// Each edge once: a target a node already sends to is dropped from a later
/// edge, and an edge left with no target goes. The router extends a node's
/// targets with every edge from it, so an edge two files both state would
/// otherwise deliver every record twice.
fn dedupe(dag: Vec<Edge>) -> Vec<Edge> {
    let mut seen: HashSet<(Arc<str>, Arc<str>)> = HashSet::new();
    dag.into_iter()
        .filter_map(|mut e| {
            let had = !e.to.is_empty();
            let from = e.from.clone();
            e.to.retain(|t| seen.insert((from.clone(), t.clone())));
            (!had || !e.to.is_empty()).then_some(e)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(yaml: &str) -> Config {
        serde_yaml::from_str(yaml).unwrap()
    }

    const LINES: &str = "sources:\n  lines: { type: tcp, bind_address: \"127.0.0.1:9\" }\n";

    /// An edge names its nodes, each found in whichever map defines it.
    #[test]
    fn a_name_is_its_node() {
        let cfg = config(&format!(
            "{LINES}sinks:\n  out: {{ type: blackhole }}\ndag:\n  - from: lines\n    to: [out]\n"
        ));
        assert_eq!(
            cfg.node("lines"),
            Some(NodeRef::Source {
                name: "lines".into()
            })
        );
        assert_eq!(cfg.node("out"), Some(NodeRef::Sink { name: "out".into() }));
        assert_eq!(cfg.node("nope"), None);
        cfg.validate().unwrap();
    }

    /// Two kinds sharing a name would make an edge's name two nodes: an
    /// error naming both.
    #[test]
    fn two_kinds_may_not_share_a_name() {
        let cfg = config(
            "sources:\n  out: { type: tcp, bind_address: \"127.0.0.1:9\" }\n\
             sinks:\n  out: { type: blackhole }\ndag:\n  - from: out\n    to: [out]\n",
        );
        let e = format!("{:#}", cfg.validate().unwrap_err());
        assert!(e.contains("`out` is a source and a sink"), "{e}");
    }

    /// A plugin's name is its `.cwasm` file's, and an inference node's the
    /// field its reply goes in: `../x` would name a file outside
    /// plugins_path, and `judge.v2.flag` is judge/v2/flag. An error naming
    /// each name that is not letters, digits, `_` and `-`.
    #[test]
    fn a_name_is_letters_digits_underscores_and_dashes() {
        let cfg = config(&format!(
            "{LINES}plugins:\n  ../x: {{ module_type: rust }}\n  \
             ok_name-2: {{ module_type: rust }}\n\
             inference:\n  judge.v2: {{ model: m, prompt_file: p.txt }}\n\
             sinks:\n  out: {{ type: blackhole }}\n\
             dag:\n  - from: lines\n    to: [../x, ok_name-2, judge.v2, out]\n"
        ));
        let e = format!("{:#}", cfg.validate().unwrap_err());
        assert!(e.contains("plugin `../x`"), "{e}");
        assert!(e.contains("inference node `judge.v2`"), "{e}");
        assert!(!e.contains("ok_name-2"), "{e}");
    }

    /// An edge runs from a node that sends to one that takes: not out of a
    /// sink, not into a source.
    #[test]
    fn no_edge_leaves_a_sink_or_enters_a_source() {
        let cfg = config(&format!(
            "{LINES}sinks:\n  out: {{ type: blackhole }}\ndag:\n  - from: out\n    to: [lines]\n"
        ));
        let e = format!("{:#}", cfg.validate().unwrap_err());
        assert!(e.contains("from `out`: a sink sends nothing on"), "{e}");
        assert!(e.contains("to `lines`: a source takes nothing in"), "{e}");
    }

    /// An s3 sink may name its objects' prefix; without one they go to the
    /// bucket's root.
    #[test]
    fn an_s3_sink_takes_a_key_prefix() {
        let s3 = |extra: &str| match serde_yaml::from_str::<SinkConfig>(&format!(
            "type: s3\nbucket_name: logs\n{extra}"
        ))
        .unwrap()
        .kind
        {
            SinkKind::S3(c) => c.key_prefix,
            other => panic!("{other:?}"),
        };
        assert_eq!(
            s3("key_prefix: cloudtrail/%Y/%m/%d\n").as_deref(),
            Some("cloudtrail/%Y/%m/%d")
        );
        assert_eq!(s3(""), None);
    }

    /// A walk sends a node's records on along every edge until a sink: one
    /// on a cycle goes round it for good, through a model each time. An
    /// error naming the cycle, through an ungrouped plugin, a grouped one or
    /// a model; two edges into one node are no cycle.
    #[test]
    fn the_dag_has_no_cycle() {
        let nodes = "plugins:\n  a: { module_type: rust }\n  \
                     g: { module_type: rust, group_by: [actor] }\n\
                     inference:\n  first: { model: m, prompt_file: p.txt }\n  \
                     second: { model: m, prompt_file: p.txt }\n\
                     sinks:\n  out: { type: blackhole }\n";
        let validate = |dag: &str| {
            config(&format!(
                "{LINES}{nodes}dag:\n  - from: lines\n    to: [a, g, first]\n{dag}"
            ))
            .validate()
        };
        let cycle = |dag: &str| format!("{:#}", validate(dag).unwrap_err());

        let e = cycle("  - from: a\n    to: [a, out]\n");
        assert!(e.contains("a cycle: a -> a"), "{e}");
        let e =
            cycle("  - from: first\n    to: [second]\n  - from: second\n    to: [first, out]\n");
        assert!(e.contains("a cycle: first -> second -> first"), "{e}");
        let e = cycle("  - from: g\n    to: [first]\n  - from: first\n    to: [g, out]\n");
        assert!(e.contains("a cycle: first -> g -> first"), "{e}");

        validate(
            "  - from: a\n    to: [first, second]\n  - from: first\n    to: [second]\n  \
             - from: second\n    to: [out]\n",
        )
        .unwrap();
    }

    /// A node no edge reaches never runs, and a source no edge leaves is
    /// read and dropped: each loads, with a warning naming it. A plugin no
    /// edge leaves is fine: it may only write state.
    #[test]
    fn a_node_no_edge_reaches_is_a_warning() {
        let cfg = config(&format!(
            "{LINES}  idle: {{ type: tcp, bind_address: \"127.0.0.1:9\" }}\n\
             plugins:\n  builder: {{ module_type: rust }}\n  stray: {{ module_type: rust }}\n\
             inference:\n  lonely: {{ model: m, prompt_file: p.txt }}\n\
             sinks:\n  out: {{ type: blackhole }}\n  never: {{ type: blackhole }}\n\
             dag:\n  - from: lines\n    to: [builder, out]\n"
        ));
        cfg.validate().unwrap();
        assert_eq!(
            cfg.warnings(),
            [
                "source idle: no edge leaves it; its records are read and dropped",
                "plugin stray: no edge reaches it; it never runs",
                "inference node lonely: no edge reaches it; it never runs",
                "sink never: no edge reaches it; it never runs",
            ]
        );
    }

    #[test]
    fn a_file_sink_takes_ndjson_only() {
        let file = |encoding: &str| -> Config {
            serde_yaml::from_str(&format!(
                "sources:\n  lines: {{ type: tcp, bind_address: \"127.0.0.1:9\" }}\n\
                 sinks:\n  out: {{ type: file, path: /tmp/out, encoding: {encoding} }}\n\
                 dag:\n  - from: lines\n    \
                 to: [out]\n"
            ))
            .unwrap()
        };
        assert!(file("{ type: ndjson }").validate().is_ok());
        let parquet = "{ type: parquet, schema: { n: int } }";
        for enc in [parquet, "{ type: json }"] {
            let e = file(enc).validate().unwrap_err();
            assert!(e.to_string().contains("rolling_file"), "{enc}: {e}");
        }
    }
}
