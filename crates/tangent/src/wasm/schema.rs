//! What a plugin says its records hold (its `output-schema`, a JSON Schema),
//! and the two checks made with it: the DAG's sinks against the plugins that
//! feed them, when the pipeline is built, and a plugin's records against its
//! own schema, in `tangent plugin test`.
//!
//! Only what those checks need is read: `type`, `properties`, `required`,
//! `items`, `additionalProperties: false`, `anyOf`/`oneOf` (an `Option` of a
//! struct), and `$ref` into the document's own `$defs`. A keyword it does not
//! read constrains nothing here.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use serde_json::Value;
use tangent_shared::dag::NodeRef;
use tangent_shared::sinks::common::{Column, ColumnType, Columns, Encoding, SinkConfig, SinkKind};

/// A plugin's output schema.
#[derive(Debug, Clone)]
pub struct Shape {
    root: Value,
}

/// What a schema says of one field: the JSON types it may have (none: any),
/// and whether every record has it.
#[derive(Debug, PartialEq, Eq)]
pub struct Field {
    pub types: BTreeSet<String>,
    pub required: bool,
}

impl Shape {
    pub fn parse(text: &str) -> Result<Self> {
        let root: Value = serde_json::from_str(text).with_context(|| {
            let start: String = text.chars().take(120).collect();
            format!("output-schema is not JSON: {start}")
        })?;
        if !(root.is_object() || root.is_boolean()) {
            bail!("output-schema is not a JSON Schema: neither an object nor a boolean");
        }
        Ok(Self { root })
    }

    /// The node `$ref` points to, followed until it is not a reference.
    fn resolve<'a>(&'a self, mut node: &'a Value) -> &'a Value {
        for _ in 0..32 {
            let Some(r) = node.get("$ref").and_then(Value::as_str) else {
                return node;
            };
            let target = r
                .strip_prefix("#/$defs/")
                .map(|n| ("$defs", n))
                .or_else(|| r.strip_prefix("#/definitions/").map(|n| ("definitions", n)));
            match target.and_then(|(defs, n)| self.root.get(defs)?.get(n)) {
                Some(t) => node = t,
                None => return &Value::Bool(true),
            }
        }
        node
    }

    /// The alternatives a node allows: itself, or each of its `anyOf`/`oneOf`.
    fn alternatives<'a>(&'a self, node: &'a Value) -> Vec<&'a Value> {
        let node = self.resolve(node);
        for k in ["anyOf", "oneOf"] {
            if let Some(alts) = node.get(k).and_then(Value::as_array) {
                return alts.iter().flat_map(|a| self.alternatives(a)).collect();
            }
        }
        vec![node]
    }

    /// The JSON types a node allows; empty for any.
    fn types(&self, node: &Value) -> BTreeSet<String> {
        let mut out = BTreeSet::new();
        for alt in self.alternatives(node) {
            match alt.get("type") {
                Some(Value::String(t)) => {
                    out.insert(t.clone());
                }
                Some(Value::Array(ts)) => {
                    out.extend(ts.iter().filter_map(Value::as_str).map(str::to_string));
                }
                // an alternative of any type: the node is any type
                _ => return BTreeSet::new(),
            }
        }
        out
    }

    /// Whether the schema names no fields: a plugin that passes records on
    /// as they came says `{}`, and what it promises is what feeds it.
    pub fn names_nothing(&self) -> bool {
        self.alternatives(&self.root).iter().all(|n| {
            n.get("properties")
                .and_then(Value::as_object)
                .is_none_or(|p| p.is_empty())
        })
    }

    /// What the schema says of the field at a dotted path; None when it does
    /// not name it.
    pub fn field(&self, path: &str) -> Option<Field> {
        let mut nodes = vec![&self.root];
        let mut required = true;
        for key in path.split('.') {
            let mut next = Vec::new();
            let mut all_require = true;
            for node in nodes.iter().flat_map(|n| self.alternatives(n)) {
                // null is no record; it does not need the key
                if node.get("type").and_then(Value::as_str) == Some("null") {
                    continue;
                }
                if let Some(p) = node.get("properties").and_then(|p| p.get(key)) {
                    next.push(p);
                    let req = node
                        .get("required")
                        .and_then(Value::as_array)
                        .is_some_and(|r| r.iter().any(|k| k == key));
                    all_require &= req;
                } else {
                    all_require = false;
                }
            }
            if next.is_empty() {
                return None;
            }
            required &= all_require;
            nodes = next;
        }
        let mut types = BTreeSet::new();
        for n in &nodes {
            let t = self.types(n);
            if t.is_empty() {
                types.clear();
                break;
            }
            types.extend(t);
        }
        Some(Field { types, required })
    }

    /// Where `v` breaks the schema, each as `path: what`; empty when it fits.
    pub fn check(&self, v: &Value) -> Vec<String> {
        let mut out = Vec::new();
        self.check_at(&self.root, v, "", &mut out);
        out
    }

    fn check_at(&self, node: &Value, v: &Value, at: &str, out: &mut Vec<String>) {
        let alts = self.alternatives(node);
        if alts.len() > 1 {
            // fits one of them, or say what the first non-null one wanted
            let mut first = None;
            for a in &alts {
                let mut errs = Vec::new();
                self.check_at(a, v, at, &mut errs);
                if errs.is_empty() {
                    return;
                }
                if first.is_none() && a.get("type").and_then(Value::as_str) != Some("null") {
                    first = Some(errs);
                }
            }
            out.extend(first.unwrap_or_else(|| vec![format!("{}: fits no alternative", here(at))]));
            return;
        }
        let node = alts[0];
        if node == &Value::Bool(false) {
            out.push(format!("{}: not allowed", here(at)));
            return;
        }
        let types = self.types(node);
        if !types.is_empty() && !types.iter().any(|t| is(v, t)) {
            out.push(format!(
                "{}: {} where the schema says {}",
                here(at),
                kind(v),
                list(&types)
            ));
            return;
        }
        if let Value::Object(m) = v {
            let props = node.get("properties").and_then(Value::as_object);
            for k in node
                .get("required")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                if let Some(k) = k.as_str() {
                    if !m.contains_key(k) {
                        out.push(format!("{}: required, missing", join(at, k)));
                    }
                }
            }
            for (k, x) in m {
                match props.and_then(|p| p.get(k)) {
                    Some(p) => self.check_at(p, x, &join(at, k), out),
                    None if node.get("additionalProperties") == Some(&Value::Bool(false)) => {
                        out.push(format!("{}: not in the schema", join(at, k)))
                    }
                    None => {}
                }
            }
        }
        if let (Value::Array(xs), Some(items)) = (v, node.get("items")) {
            for (i, x) in xs.iter().enumerate() {
                self.check_at(items, x, &format!("{at}[{i}]"), out);
            }
        }
    }
}

fn here(at: &str) -> &str {
    if at.is_empty() {
        "the record"
    } else {
        at
    }
}

fn join(at: &str, k: &str) -> String {
    if at.is_empty() {
        k.to_string()
    } else {
        format!("{at}.{k}")
    }
}

fn is(v: &Value, t: &str) -> bool {
    match t {
        "null" => v.is_null(),
        "boolean" => v.is_boolean(),
        "object" => v.is_object(),
        "array" => v.is_array(),
        "string" => v.is_string(),
        "number" => v.is_number(),
        "integer" => v.as_i64().is_some() || v.as_u64().is_some(),
        _ => true,
    }
}

fn kind(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(n) if n.is_f64() => "a number",
        Value::Number(_) => "an integer",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

fn list(types: &BTreeSet<String>) -> String {
    types.iter().cloned().collect::<Vec<_>>().join(" or ")
}

/// A field a sink reads, and the JSON types it can use.
#[derive(Debug, PartialEq, Eq)]
pub struct Need {
    pub path: String,
    pub types: &'static [&'static str],
    pub why: String,
}

const TIME: &[&str] = &["string", "integer", "number"];
const SCALAR: &[&str] = &["string", "integer", "number", "boolean"];

/// The fields a sink reads out of each record: a sqlite sink's
/// `timestamp_key` and partition fields, a Parquet encoding's columns.
pub fn needs(sink: &SinkConfig) -> Vec<Need> {
    let mut out = Vec::new();
    if let SinkKind::Sqlite(c) = &sink.kind {
        if let Some(k) = &c.timestamp_key {
            out.push(Need {
                path: k.clone(),
                types: TIME,
                why: "its timestamp_key".into(),
            });
        }
        let mut seen = BTreeSet::new();
        for (name, fields) in &c.partitions {
            for f in fields {
                if seen.insert(f) {
                    out.push(Need {
                        path: f.clone(),
                        types: SCALAR,
                        why: format!("its partition {name}"),
                    });
                }
            }
        }
    }
    if let Encoding::Parquet { schema, .. } = &sink.common.encoding {
        columns(schema, "", &mut out);
    }
    out
}

fn columns(cols: &Columns, at: &str, out: &mut Vec<Need>) {
    for (name, col) in &cols.0 {
        let path = join(at, name);
        let types: &'static [&'static str] = match col {
            Column::Type(ColumnType::String) => &["string"],
            Column::Type(ColumnType::Int) => &["integer"],
            Column::Type(ColumnType::Float) => &["number", "integer"],
            Column::Type(ColumnType::Bool) => &["boolean"],
            Column::Type(ColumnType::Timestamp) => &["string"],
            Column::Struct(inner) => {
                columns(inner, &path, out);
                continue;
            }
        };
        out.push(Need {
            path: path.clone(),
            types,
            why: format!("its Parquet column {path}"),
        });
    }
}

/// Every sink's needs against what the plugins feeding it say they write.
/// A plugin whose schema names nothing passes on what feeds it, so the
/// plugins before it are checked in its place; a source or an inference
/// node says nothing of its records, and the walk back stops there unchecked.
/// A field a plugin does not name, or names with no type the sink can use,
/// is an error; a field it names but may leave out is not, as every sink
/// takes a record without it (a null column, an arrival time).
pub fn check_dag(
    sinks: &BTreeMap<Arc<str>, SinkConfig>,
    dag: &[tangent_shared::dag::Edge],
    shapes: &BTreeMap<Arc<str>, Shape>,
) -> Result<()> {
    let mut into: BTreeMap<String, Vec<&NodeRef>> = BTreeMap::new();
    for e in dag {
        for to in &e.to {
            into.entry(node_key(to)).or_default().push(&e.from);
        }
    }
    let mut errors = Vec::new();
    for (sink, cfg) in sinks {
        let needs = needs(cfg);
        if needs.is_empty() {
            continue;
        }
        let mut plugins = Vec::new();
        let mut seen = HashSet::new();
        let mut todo: Vec<&NodeRef> = into
            .get(&node_key(&NodeRef::Sink {
                name: Arc::clone(sink),
                key_prefix: None,
            }))
            .cloned()
            .unwrap_or_default();
        while let Some(n) = todo.pop() {
            if !seen.insert(node_key(n)) {
                continue;
            }
            let NodeRef::Plugin { name } = n else {
                continue;
            };
            match shapes.get(name) {
                Some(s) if !s.names_nothing() => plugins.push((name, s)),
                _ => todo.extend(into.get(&node_key(n)).into_iter().flatten()),
            }
        }
        for (plugin, shape) in plugins {
            for need in &needs {
                match shape.field(&need.path) {
                    None => errors.push(format!(
                        "sink {sink} reads `{}` ({}), which plugin {plugin} does not say it writes",
                        need.path, need.why
                    )),
                    Some(f)
                        if !f.types.is_empty()
                            && !f
                                .types
                                .iter()
                                .any(|t| t != "null" && need.types.contains(&t.as_str())) =>
                    {
                        errors.push(format!(
                            "sink {sink} reads `{}` ({}) as {}, which plugin {plugin} writes as {}",
                            need.path,
                            need.why,
                            need.types.join(" or "),
                            list(&f.types)
                        ))
                    }
                    Some(_) => {}
                }
            }
        }
    }
    if !errors.is_empty() {
        bail!("{}", errors.join("\n"));
    }
    Ok(())
}

/// A node as an edge names it, its sink's key prefix aside.
fn node_key(n: &NodeRef) -> String {
    match n {
        NodeRef::Source { name } => format!("source:{name}"),
        NodeRef::Plugin { name } => format!("plugin:{name}"),
        NodeRef::Inference { name } => format!("inference:{name}"),
        NodeRef::Sink { name, .. } => format!("sink:{name}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A schema like schemars writes for a finished record.
    fn finished() -> Shape {
        Shape::parse(
            &json!({
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "title": "Finished",
                "type": "object",
                "properties": {
                    "ts": {"type": "string"},
                    "source": {"type": "string"},
                    "_norm": {"$ref": "#/$defs/Norm"},
                    "n": {"type": "integer", "format": "int64"},
                    "tags": {"type": ["array", "null"], "items": {"type": "string"}},
                    "who": {"anyOf": [{"$ref": "#/$defs/Who"}, {"type": "null"}]}
                },
                "required": ["ts", "source", "_norm", "n"],
                "additionalProperties": true,
                "$defs": {
                    "Norm": {
                        "type": "object",
                        "properties": {
                            "org": {"type": ["string", "null"]},
                            "actor": {"type": ["string", "null"]}
                        }
                    },
                    "Who": {
                        "type": "object",
                        "properties": {"id": {"type": "string"}},
                        "required": ["id"]
                    }
                }
            })
            .to_string(),
        )
        .unwrap()
    }

    #[test]
    fn a_field_through_refs_and_options() {
        let s = finished();
        let f = |p: &str| s.field(p);
        assert_eq!(
            f("ts"),
            Some(Field {
                types: ["string".to_string()].into(),
                required: true
            })
        );
        let actor = f("_norm.actor").unwrap();
        assert!(
            !actor.required && actor.types.contains("string"),
            "{actor:?}"
        );
        // under an optional struct, a required field is not always there
        let id = f("who.id").unwrap();
        assert!(!id.required && id.types.contains("string"), "{id:?}");
        assert_eq!(f("_norm.kind"), None);
        assert_eq!(f("eventName"), None);
        assert!(!s.names_nothing());
        assert!(Shape::parse("{}").unwrap().names_nothing());
        assert!(
            Shape::parse(r#"{"type":"object","additionalProperties":true}"#)
                .unwrap()
                .names_nothing()
        );
    }

    #[test]
    fn a_record_against_its_schema() {
        let s = finished();
        let ok = json!({"ts": "2026-10-20T13:00:00Z", "source": "okta", "_norm": {"org": "acme"},
                        "n": 1, "tags": null, "who": {"id": "u1"}, "eventType": "x"});
        assert_eq!(s.check(&ok), Vec::<String>::new());
        let bad = json!({"source": 3, "_norm": {"org": 1}, "n": 1.5, "tags": [1], "who": {}});
        let errs = s.check(&bad);
        for want in [
            "ts: required, missing",
            "source: an integer where the schema says string",
            "_norm.org: an integer where the schema says null or string",
            "n: a number where the schema says integer",
            "tags[0]: an integer where the schema says string",
            "who.id: required, missing",
        ] {
            assert!(errs.iter().any(|e| e == want), "{want} not in {errs:#?}");
        }
        let closed =
            Shape::parse(r#"{"type":"object","properties":{"a":{}},"additionalProperties":false}"#)
                .unwrap();
        assert_eq!(
            closed.check(&json!({"a": 1, "b": 2})),
            ["b: not in the schema"]
        );
    }

    fn cfg(
        yaml: &str,
    ) -> (
        BTreeMap<Arc<str>, SinkConfig>,
        Vec<tangent_shared::dag::Edge>,
    ) {
        #[derive(serde::Deserialize)]
        struct C {
            sinks: BTreeMap<Arc<str>, SinkConfig>,
            dag: Vec<tangent_shared::dag::Edge>,
        }
        let c: C = serde_yaml::from_str(yaml).unwrap();
        (c.sinks, c.dag)
    }

    const PIPELINE: &str = "
sinks:
  logs: { type: sqlite, path: logs.sqlite, table: logs, timestamp_key: ts,
          partitions: { actor: [_norm.org, _norm.actor] } }
  lake: { type: rolling_file, path: lake, wal_path: wal,
          encoding: { type: parquet, schema: { ts: timestamp, n: float, _norm: { org: string } } } }
  out: { type: file, path: out.ndjson }
dag:
  - from: { kind: source, name: input }
    to: [{ kind: plugin, name: pack }]
  - from: { kind: plugin, name: pack }
    to: [{ kind: plugin, name: pass }, { kind: sink, name: out }]
  - from: { kind: plugin, name: pass }
    to: [{ kind: sink, name: logs }, { kind: sink, name: lake }]
";

    #[test]
    fn sinks_are_checked_against_the_plugins_that_feed_them() {
        let (sinks, dag) = cfg(PIPELINE);
        let shapes = |pack: Shape| {
            BTreeMap::from([
                (Arc::<str>::from("pack"), pack),
                (Arc::<str>::from("pass"), Shape::parse("{}").unwrap()),
            ])
        };
        // `pass` names nothing, so `pack` is checked for both sinks
        check_dag(&sinks, &dag, &shapes(finished())).unwrap();

        let short = Shape::parse(
            r#"{"type":"object","properties":{"ts":{"type":"integer"},"n":{"type":"string"},
                "_norm":{"type":"object","properties":{"org":{"type":"string"}}}}}"#,
        )
        .unwrap();
        let e = check_dag(&sinks, &dag, &shapes(short))
            .unwrap_err()
            .to_string();
        let lines: Vec<&str> = e.lines().collect();
        assert_eq!(
            lines,
            [
                "sink lake reads `ts` (its Parquet column ts) as string, which plugin pack writes as integer",
                "sink lake reads `n` (its Parquet column n) as number or integer, which plugin pack writes as string",
                "sink logs reads `_norm.actor` (its partition actor), which plugin pack does not say it writes",
            ],
            "{e}"
        );
    }

    #[test]
    fn a_source_or_a_plugin_naming_nothing_goes_unchecked() {
        let (sinks, dag) = cfg(PIPELINE);
        let nothing = BTreeMap::from([
            (Arc::<str>::from("pack"), Shape::parse("{}").unwrap()),
            (Arc::<str>::from("pass"), Shape::parse("true").unwrap()),
        ]);
        check_dag(&sinks, &dag, &nothing).unwrap();
    }
}
