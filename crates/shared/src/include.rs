//! `include:`: a config spread over several files, merged into one before it
//! is read. Nothing here knows what the files are for; it knows the config's
//! shape only: which keys name a file, and that `dag` adds up.
//!
//! Every file reachable from the root is read once, depth first, each file's
//! includes before the file itself, and merged in that order:
//! - maps merge key by key, at any depth;
//! - a file wins over every file it includes, directly or not, and its
//!   `key: null` removes what they set there;
//! - two files neither of which includes the other must agree on any value
//!   that is not a map, and on whether a key holds a map at all, unless a
//!   file that includes both settles it; else loading fails naming the key
//!   and both files. A null says nothing about files its file does not
//!   include;
//! - lists are values, except the top-level `dag`, whose edges add up;
//! - in a plugin's `config` and an inference node's `extra`, which tangent
//!   hands on as written, a null is a value like any other;
//! - a top-level key that starts `x-` is the file's own (a place for YAML
//!   anchors) and is not merged.
//!
//! The result does not depend on the order of `include:`, only the order of
//! the edges does: a file included twice is the same file to both includers,
//! and what each changes in it holds. Each scalar keeps the text its file
//! wrote, so a value reads as it would in a file without `include:`: `1.10`
//! is "1.10" to a string field, not 1.1.

use anyhow::{anyhow, bail, Context, Result};
use serde::de::{
    self, Deserialize, DeserializeSeed, Deserializer, IgnoredAny, MapAccess, SeqAccess, Visitor,
};
use serde_yaml::value::{Tag, TaggedValue};
use serde_yaml::{Mapping, Value};
use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::fs;
use std::path::{Component, Path, PathBuf};

/// The keys a config file may have at its top, besides the `x-` keys it
/// keeps for itself.
pub const TOP_LEVEL: [&str; 8] = [
    "runtime",
    "sources",
    "sinks",
    "plugins",
    "inference",
    "enrichment_tables",
    "dag",
    "include",
];

/// The keys that name a file, relative to the file that sets them. `*` is any
/// key of a map, `[]` any item of a list. A path here is absolute once its
/// file is merged; `runtime` paths stay relative to the root config and
/// source and sink paths to the working directory, as they always were.
pub const PATHS: &[&[&str]] = &[
    &["plugins", "*", "path"],
    &["plugins", "*", "data"],
    &["plugins", "*", "tests", "[]", "input"],
    &["plugins", "*", "tests", "[]", "expected"],
    &[
        "plugins",
        "*",
        "tests",
        "[]",
        "enrichment_tables",
        "*",
        "path",
    ],
    &["inference", "*", "prompt_file"],
    &["inference", "*", "schema_file"],
    &["enrichment_tables", "*", "path"],
];

/// The maps tangent hands on as written: in them a null is a value, where
/// elsewhere it is a key not set.
pub const HANDED_ON: &[&[&str]] = &[&["plugins", "*", "config"], &["inference", "*", "extra"]];

/// A top-level key a file keeps for itself, for anchors: `x-prompt: &p
/// judge.md`. It is not merged and not read.
pub fn own_key(k: &Value) -> bool {
    k.as_str().is_some_and(|k| k.starts_with("x-"))
}

/// A file read for a config: `${VAR}` expanded, parsed, its top checked.
pub(crate) struct Read {
    pub text: String,
    pub map: Mapping,
}

pub(crate) fn read(path: &Path) -> Result<Read> {
    let raw = fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let text = expand_env(&raw);
    let value: Value =
        serde_yaml::from_str(&text).with_context(|| format!("parsing YAML {}", path.display()))?;
    let map = match value {
        Value::Mapping(m) => m,
        // an empty file, or only comments
        Value::Null => Mapping::new(),
        _ => bail!("{}: a config is a map at its top", path.display()),
    };
    for k in map.keys() {
        if !k.as_str().is_some_and(|k| TOP_LEVEL.contains(&k)) && !own_key(k) {
            bail!(
                "{}: unknown top-level key `{}` (one of {}, or x-<anything>, ignored)",
                path.display(),
                key_text(k),
                TOP_LEVEL.join(", ")
            );
        }
    }
    Ok(Read { text, map })
}

/// `map`, `text` as serde_yaml read it, with every scalar that is not a
/// string (a null, a bool, a number) as the text its file wrote, tagged
/// [`RAW`]. serde_yaml's reading of `1.10` is 1.1, which a string field
/// would get as "1.1"; read from the text, it gets "1.10".
fn scalar_text(text: &str, map: Mapping) -> Mapping {
    let shape = Value::Mapping(map);
    match Shape(&shape).deserialize(serde_yaml::Deserializer::from_str(text)) {
        Ok(Value::Mapping(m)) => m,
        // A file the second reading cannot follow (none is known) keeps
        // serde_yaml's reading of its scalars.
        _ => match shape {
            Value::Mapping(m) => m,
            _ => Mapping::new(),
        },
    }
}

/// The tag on a scalar kept as its file wrote it.
const RAW: &str = "!tangent/as-written";

fn raw(text: String) -> Value {
    Value::Tagged(Box::new(TaggedValue {
        tag: Tag::new(RAW),
        value: Value::String(text),
    }))
}

/// A scalar's text, when it was kept as written.
fn raw_text(v: &Value) -> Option<&str> {
    match v {
        Value::Tagged(t) if t.tag == Tag::new(RAW) => t.value.as_str(),
        _ => None,
    }
}

/// A null, as serde_yaml reads one: `~`, `null` or nothing at all.
fn is_null(v: &Value) -> bool {
    match v {
        Value::Null => true,
        v => raw_text(v).is_some_and(|t| matches!(t, "" | "~" | "null" | "Null" | "NULL")),
    }
}

/// Reads a YAML node again as `.0` says it is shaped, for its scalars' text.
struct Shape<'a>(&'a Value);

impl<'de> DeserializeSeed<'de> for Shape<'_> {
    type Value = Value;

    fn deserialize<D: Deserializer<'de>>(self, d: D) -> Result<Value, D::Error> {
        match self.0 {
            Value::Mapping(m) => d.deserialize_map(MapShape(m)),
            Value::Sequence(items) => d.deserialize_seq(SeqShape(items)),
            // asked for a string, serde_yaml gives a scalar's text as written
            Value::Null | Value::Bool(_) | Value::Number(_) => d.deserialize_str(Text).map(raw),
            v => {
                IgnoredAny::deserialize(d)?;
                Ok(v.clone())
            }
        }
    }
}

struct MapShape<'a>(&'a Mapping);

impl<'de> Visitor<'de> for MapShape<'_> {
    type Value = Value;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("a map")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut a: A) -> Result<Value, A::Error> {
        let mut out = Mapping::with_capacity(self.0.len());
        for (k, v) in self.0 {
            let Some(k) = a.next_key_seed(Shape(k))? else {
                return Err(de::Error::custom("a map read again is shorter"));
            };
            out.insert(k, a.next_value_seed(Shape(v))?);
        }
        if a.next_key::<IgnoredAny>()?.is_some() {
            return Err(de::Error::custom("a map read again is longer"));
        }
        Ok(Value::Mapping(out))
    }
}

struct SeqShape<'a>(&'a [Value]);

impl<'de> Visitor<'de> for SeqShape<'_> {
    type Value = Value;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("a list")
    }

    fn visit_seq<A: SeqAccess<'de>>(self, mut a: A) -> Result<Value, A::Error> {
        let mut out = Vec::with_capacity(self.0.len());
        for v in self.0 {
            let Some(v) = a.next_element_seed(Shape(v))? else {
                return Err(de::Error::custom("a list read again is shorter"));
            };
            out.push(v);
        }
        if a.next_element::<IgnoredAny>()?.is_some() {
            return Err(de::Error::custom("a list read again is longer"));
        }
        Ok(Value::Sequence(out))
    }
}

struct Text;

impl<'de> Visitor<'de> for Text {
    type Value = String;

    fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.write_str("a scalar")
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<String, E> {
        Ok(v.to_string())
    }

    fn visit_unit<E: de::Error>(self) -> Result<String, E> {
        Ok(String::new())
    }
}

/// `${VAR}` is the variable's value, empty when unset; `${VAR:-default}` is
/// `default` when the variable is unset or empty.
pub(crate) fn expand_env(s: &str) -> String {
    let mut out = String::new();
    let mut rest = s;

    while let Some(start) = rest.find("${") {
        out.push_str(&rest[..start]);
        if let Some(end) = rest[start + 2..].find('}') {
            let inner = &rest[start + 2..start + 2 + end];
            let val = match inner.split_once(":-") {
                Some((var, default)) => std::env::var(var)
                    .ok()
                    .filter(|v| !v.is_empty())
                    .unwrap_or_else(|| default.to_string()),
                None => std::env::var(inner).unwrap_or_default(),
            };
            out.push_str(&val);
            rest = &rest[start + 3 + end..];
        } else {
            // no closing }, just append
            out.push_str(&rest[start..]);
            break;
        }
    }
    out.push_str(rest);
    out
}

/// Every relative path under `PATHS` joined to `dir`: canonical when it
/// exists, `.` and `..` worked out otherwise (a data directory a setup script
/// has yet to fetch).
pub fn rebase(map: &mut Mapping, dir: &Path) {
    for pattern in PATHS {
        rebase_at(map, pattern, dir);
    }
}

fn rebase_at(map: &mut Mapping, pattern: &[&str], dir: &Path) {
    let Some((first, rest)) = pattern.split_first() else {
        return;
    };
    let children: Vec<&mut Value> = if *first == "*" {
        map.values_mut().collect()
    } else {
        map.get_mut(*first).into_iter().collect()
    };
    for child in children {
        rebase_value(child, rest, dir);
    }
}

fn rebase_value(v: &mut Value, pattern: &[&str], dir: &Path) {
    match (pattern.first(), v) {
        (None, Value::String(s)) => {
            if !s.is_empty() && Path::new(s.as_str()).is_relative() {
                let joined = dir.join(s.as_str());
                let resolved = joined.canonicalize().unwrap_or_else(|_| lexical(&joined));
                *s = resolved.to_string_lossy().into_owned();
            }
        }
        (Some(&"[]"), Value::Sequence(items)) => {
            for item in items {
                rebase_value(item, &pattern[1..], dir);
            }
        }
        (Some(_), Value::Mapping(m)) => rebase_at(m, pattern, dir),
        _ => {}
    }
}

/// `path` with `.` dropped and each `..` taking the component before it.
fn lexical(path: &Path) -> PathBuf {
    let mut out = PathBuf::new();
    for c in path.components() {
        match c {
            Component::CurDir => {}
            Component::ParentDir => {
                if !out.pop() {
                    out.push("..");
                }
            }
            c => out.push(c.as_os_str()),
        }
    }
    out
}

/// What a config's files add up to.
pub(crate) struct Merged {
    /// The merged config as YAML text, read like any config file so a
    /// value means what it means in a file: `since: 3600` is the string a
    /// `String` field wants, and `model: 1.10` is "1.10".
    pub text: String,
    /// Every file, in the order merged; the root is last.
    pub files: Vec<PathBuf>,
    tree: Node,
    /// The file each `dag` edge came from, in order.
    edges: Vec<usize>,
    shown: Vec<String>,
}

impl Merged {
    /// `err` from reading the merged text, with the files that set
    /// something under the key it is about. Its line and column are in the
    /// merged text, which no one has seen, so they go.
    pub fn explain(&self, err: serde_path_to_error::Error<serde_yaml::Error>) -> anyhow::Error {
        let path = err.path().to_string();
        let mut msg = err.inner().to_string();
        if let Some(at) = msg.rfind(" at line ") {
            if msg[at + 9..]
                .split(" column ")
                .all(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()))
            {
                msg.truncate(at);
            }
        }
        // The YAML reader names where it stood, which inside a `type`-tagged
        // block is the block's map; the path here goes on to the block.
        if let Some((head, rest)) = msg.split_once(": ") {
            if path == head
                || path.starts_with(&format!("{head}."))
                || path.starts_with(&format!("{head}["))
            {
                msg = rest.to_string();
            }
        }
        if path != "." {
            msg = format!("{path}: {msg}");
        }
        let files = self.files_under(err.path());
        if !files.is_empty() {
            let names: Vec<&str> = files.iter().map(|&f| self.shown[f].as_str()).collect();
            let _ = write!(msg, " (set in {})", names.join(", "));
        }
        anyhow!(msg)
    }

    fn files_under(&self, path: &serde_path_to_error::Path) -> Vec<usize> {
        use serde_path_to_error::Segment;
        let segments: Vec<&Segment> = path.iter().collect();
        if let [Segment::Map { key }, Segment::Seq { index }, ..] = segments.as_slice() {
            if key == "dag" {
                return self.edges.get(*index).copied().into_iter().collect();
            }
        }
        let mut node = &self.tree;
        for s in segments {
            let Segment::Map { key } = s else { break };
            let Node::Map { entries, .. } = node else {
                break;
            };
            match entries
                .iter()
                .find(|(k, _)| k.as_str() == Some(key.as_str()))
            {
                Some((_, child)) => node = child,
                None => break,
            }
        }
        node.owners().into_iter().collect()
    }
}

/// A merged value and the files that set it. A map's owners are the files
/// that wrote it as a map; its entries have their own. A leaf holds every
/// value still standing: one, unless files that include neither of each
/// other disagree, which a file that includes them all may settle. A map one
/// file wrote where another wrote a value is a leaf's value too.
enum Node {
    Map {
        owners: BTreeSet<usize>,
        entries: Vec<(Value, Node)>,
    },
    Leaf(Vec<(Value, BTreeSet<usize>)>),
}

impl Node {
    /// `v` as file `f` set it at `path`, without its nulls: a null where
    /// nothing was set is nothing, but in a map handed on as written.
    fn new(v: &Value, f: usize, path: &mut Vec<String>) -> Option<Self> {
        match v {
            v if is_null(v) && !handed_on(path) => None,
            Value::Mapping(m) => Some(Node::Map {
                owners: BTreeSet::from([f]),
                entries: m
                    .iter()
                    .filter_map(|(k, v)| {
                        path.push(key_text(k));
                        let n = Node::new(v, f, path);
                        path.pop();
                        Some((k.clone(), n?))
                    })
                    .collect(),
            }),
            v => Some(Node::Leaf(vec![(v.clone(), BTreeSet::from([f]))])),
        }
    }

    /// Every file that set this or anything under it.
    fn owners(&self) -> BTreeSet<usize> {
        match self {
            Node::Leaf(values) => values.iter().flat_map(|(_, o)| o.iter().copied()).collect(),
            Node::Map { owners, entries } => {
                let mut all = owners.clone();
                for (_, n) in entries {
                    all.extend(n.owners());
                }
                all
            }
        }
    }

    fn value(&self) -> Value {
        match self {
            Node::Leaf(values) => values.first().map_or(Value::Null, |(v, _)| v.clone()),
            Node::Map { entries, .. } => Value::Mapping(
                entries
                    .iter()
                    .map(|(k, n)| (k.clone(), n.value()))
                    .collect(),
            ),
        }
    }
}

struct File {
    shown: String,
    map: Mapping,
    includes: Vec<usize>,
}

struct Loader {
    files: Vec<File>,
    paths: Vec<PathBuf>,
    /// The files being read, outermost first, to name a cycle.
    stack: Vec<PathBuf>,
    /// Files in merge order: each after everything it includes.
    order: Vec<usize>,
    cwd: Option<PathBuf>,
}

/// The config at `root` (already read) and everything it includes, merged.
pub(crate) fn merge(root: &Path, read: Read) -> Result<Merged> {
    let canonical = root
        .canonicalize()
        .with_context(|| format!("reading {}", root.display()))?;
    let mut loader = Loader {
        files: Vec::new(),
        paths: Vec::new(),
        stack: Vec::new(),
        order: Vec::new(),
        cwd: std::env::current_dir()
            .ok()
            .and_then(|d| d.canonicalize().ok()),
    };
    loader.visit(&canonical, Some(read))?;

    // What each file includes, directly or not. Everything a file includes
    // is merged before it, so one pass in that order sees it all.
    let mut reach: Vec<BTreeSet<usize>> = vec![BTreeSet::new(); loader.files.len()];
    for &f in &loader.order {
        let mut r = BTreeSet::new();
        for &i in &loader.files[f].includes {
            r.insert(i);
            r.extend(reach[i].iter().copied());
        }
        reach[f] = r;
    }

    let merger = Merger {
        reach: &reach,
        shown: loader.files.iter().map(|f| f.shown.clone()).collect(),
    };
    let mut tree: Option<Node> = None;
    let mut dag: Vec<Value> = Vec::new();
    let mut edges: Vec<usize> = Vec::new();
    for &f in &loader.order {
        let mut map = std::mem::take(&mut loader.files[f].map);
        map.shift_remove("include");
        map.retain(|k, _| !own_key(k));
        match map.shift_remove("dag") {
            None => {}
            Some(v) if is_null(&v) => {}
            Some(Value::Sequence(es)) => {
                edges.extend(std::iter::repeat_n(f, es.len()));
                dag.extend(es);
            }
            Some(_) => bail!("{}: `dag` is a list of edges", merger.shown[f]),
        }
        tree = merger.merge(tree, &Value::Mapping(map), f, &mut Vec::new())?;
    }
    let tree = tree.unwrap_or(Node::Map {
        owners: BTreeSet::new(),
        entries: Vec::new(),
    });
    let mut conflicts = Vec::new();
    merger.conflicts(&tree, &mut Vec::new(), &mut conflicts);
    if !conflicts.is_empty() {
        bail!(
            "included files disagree, and no file that includes them both settles it:\n  {}",
            conflicts.join("\n  ")
        );
    }

    let mut value = tree.value();
    if let Value::Mapping(m) = &mut value {
        if !dag.is_empty() {
            m.insert(Value::from("dag"), Value::Sequence(dag));
        }
    }
    let text = emit(&value);
    Ok(Merged {
        text,
        files: loader
            .order
            .iter()
            .map(|&f| loader.paths[f].clone())
            .collect(),
        tree,
        edges,
        shown: merger.shown,
    })
}

impl Loader {
    fn show(&self, path: &Path) -> String {
        self.cwd
            .as_deref()
            .and_then(|cwd| path.strip_prefix(cwd).ok())
            .unwrap_or(path)
            .display()
            .to_string()
    }

    /// Reads `path` (canonical) and, first, what it includes; its index.
    fn visit(&mut self, path: &Path, root: Option<Read>) -> Result<usize> {
        if let Some(at) = self.stack.iter().position(|p| p == path) {
            let chain: Vec<String> = self.stack[at..]
                .iter()
                .chain(std::iter::once(&path.to_path_buf()))
                .map(|p| self.show(p))
                .collect();
            bail!("include cycle: {}", chain.join(" -> "));
        }
        if let Some(f) = self.paths.iter().position(|p| p == path) {
            return Ok(f);
        }

        let dir = path.parent().unwrap_or(Path::new("/")).to_path_buf();
        let shown = self.show(path);
        let is_root = root.is_some();
        let read = match root {
            Some(read) => read,
            None => read(path)?,
        };
        let mut map = scalar_text(&read.text, read.map);
        // The root's own paths are left as written: whatever reads them joins
        // them to the root's directory already, as it always has.
        if !is_root {
            rebase(&mut map, &dir);
        }
        let named: Vec<String> = match map.get("include") {
            None => Vec::new(),
            Some(v) if is_null(v) => Vec::new(),
            Some(Value::Sequence(items)) => items
                .iter()
                .enumerate()
                .map(|(i, v)| match v {
                    Value::String(s) if !s.is_empty() => Ok(s.clone()),
                    _ => Err(anyhow!(
                        "{shown}: include[{i}] is not a file name: {}",
                        brief(v)
                    )),
                })
                .collect::<Result<_>>()?,
            Some(_) => bail!("{shown}: `include` is a list of files"),
        };

        let f = self.files.len();
        self.files.push(File {
            shown: shown.clone(),
            map,
            includes: Vec::new(),
        });
        self.paths.push(path.to_path_buf());
        self.stack.push(path.to_path_buf());
        let mut includes = Vec::with_capacity(named.len());
        for name in named {
            let target = dir.join(&name);
            let canonical = target
                .canonicalize()
                .with_context(|| format!("{shown} includes {name}"))?;
            includes.push(self.visit(&canonical, None)?);
        }
        self.stack.pop();
        self.files[f].includes = includes;
        self.order.push(f);
        Ok(f)
    }
}

struct Merger<'a> {
    reach: &'a [BTreeSet<usize>],
    shown: Vec<String>,
}

impl Merger<'_> {
    /// `f` is `o`, or includes it: what `o` set, `f` may change.
    fn over(&self, f: usize, o: usize) -> bool {
        o == f || self.reach[f].contains(&o)
    }

    /// What `f` includes taken out of `node`: what is left of it, if
    /// anything.
    fn without(&self, node: Node, f: usize) -> Option<Node> {
        match node {
            Node::Leaf(values) => {
                let left: Vec<(Value, BTreeSet<usize>)> = values
                    .into_iter()
                    .filter_map(|(v, mut owners)| {
                        owners.retain(|&o| !self.over(f, o));
                        (!owners.is_empty()).then_some((v, owners))
                    })
                    .collect();
                (!left.is_empty()).then_some(Node::Leaf(left))
            }
            Node::Map {
                mut owners,
                entries,
            } => {
                owners.retain(|&o| !self.over(f, o));
                let entries: Vec<(Value, Node)> = entries
                    .into_iter()
                    .filter_map(|(k, n)| Some((k, self.without(n, f)?)))
                    .collect();
                (!owners.is_empty() || !entries.is_empty()).then_some(Node::Map { owners, entries })
            }
        }
    }

    /// File `f`'s `v` laid over `node` (absent: `None`), at `path`.
    fn merge(
        &self,
        node: Option<Node>,
        v: &Value,
        f: usize,
        path: &mut Vec<String>,
    ) -> Result<Option<Node>> {
        let Some(node) = node else {
            return Ok(Node::new(v, f, path));
        };
        match (node, v) {
            // what f includes goes; what it does not is not f's to remove
            (node, v) if is_null(v) && !handed_on(path) => Ok(self.without(node, f)),
            (
                Node::Map {
                    mut owners,
                    mut entries,
                },
                Value::Mapping(m),
            ) => {
                owners.insert(f);
                for (k, v) in m {
                    path.push(key_text(k));
                    let at = entries.iter().position(|(ek, _)| ek == k);
                    let old = at.map(|i| entries.remove(i).1);
                    if let Some(new) = self.merge(old, v, f, path)? {
                        match at {
                            Some(i) => entries.insert(i, (k.clone(), new)),
                            None => entries.push((k.clone(), new)),
                        }
                    }
                    path.pop();
                }
                Ok(Some(Node::Map { owners, entries }))
            }
            (node, v) => {
                let Some(left) = self.without(node, f) else {
                    return Ok(Node::new(v, f, path));
                };
                // What stands, and f's value beside it, for a file that
                // includes them all to settle. A map against a value is two
                // values like any other.
                let mut values = match left {
                    Node::Leaf(values) => values,
                    map => {
                        let owners = map.owners();
                        vec![(map.value(), owners)]
                    }
                };
                let Some(mine) = Node::new(v, f, path) else {
                    return Ok(Some(Node::Leaf(values)));
                };
                let mine = mine.value();
                match values.iter_mut().find(|(have, _)| *have == mine) {
                    Some((_, owners)) => {
                        owners.insert(f);
                    }
                    None => values.push((mine, BTreeSet::from([f]))),
                }
                Ok(Some(Node::Leaf(values)))
            }
        }
    }

    /// The values files that include neither of each other left in
    /// disagreement.
    fn conflicts(&self, node: &Node, path: &mut Vec<String>, out: &mut Vec<String>) {
        match node {
            Node::Leaf(values) if values.len() > 1 => {
                let said: Vec<String> = values
                    .iter()
                    .map(|(v, owners)| {
                        let files: Vec<&str> =
                            owners.iter().map(|&o| self.shown[o].as_str()).collect();
                        format!("{} has {}", files.join(" and "), brief(v))
                    })
                    .collect();
                out.push(format!("{}: {}", dotted(path), said.join(", ")));
            }
            Node::Leaf(_) => {}
            Node::Map { entries, .. } => {
                for (k, n) in entries {
                    path.push(key_text(k));
                    self.conflicts(n, path, out);
                    path.pop();
                }
            }
        }
    }
}

/// Whether `path` is inside one of the [`HANDED_ON`] maps.
fn handed_on(path: &[String]) -> bool {
    HANDED_ON
        .iter()
        .any(|p| path.len() > p.len() && p.iter().zip(path).all(|(p, k)| *p == "*" || p == k))
}

fn dotted(path: &[String]) -> String {
    if path.is_empty() {
        ".".to_string()
    } else {
        path.join(".")
    }
}

fn key_text(k: &Value) -> String {
    match k {
        Value::String(s) => s.clone(),
        k => brief(k),
    }
}

/// A value on one line, for a message.
fn brief(v: &Value) -> String {
    if let Some(text) = raw_text(v) {
        return if text.is_empty() { "null" } else { text }.to_string();
    }
    match v {
        Value::Null => "null".to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Number(n) => n.to_string(),
        Value::String(s) => s.clone(),
        Value::Sequence(items) => {
            let items: Vec<String> = items.iter().map(brief).collect();
            format!("[{}]", items.join(", "))
        }
        Value::Mapping(m) => {
            let items: Vec<String> = m
                .iter()
                .map(|(k, v)| format!("{}: {}", key_text(k), brief(v)))
                .collect();
            format!("{{{}}}", items.join(", "))
        }
        Value::Tagged(t) => format!("{} {}", t.tag, brief(&t.value)),
    }
}

/// The merged config as YAML that reads back as its files did: block style,
/// every string double-quoted, every other scalar as its file wrote it.
fn emit(v: &Value) -> String {
    let mut out = String::new();
    match v {
        Value::Mapping(m) if !m.is_empty() => block(v, 0, &mut out),
        _ => out.push_str("{}\n"),
    }
    out
}

/// A non-empty map's entries or list's items, each line at `indent`.
fn block(v: &Value, indent: usize, out: &mut String) {
    let pad = " ".repeat(indent);
    match v {
        Value::Mapping(m) => {
            for (k, v) in m {
                out.push_str(&pad);
                match k {
                    Value::String(k) => quoted(k, out),
                    k => match raw_text(k) {
                        Some("") => out.push('~'),
                        Some(text) => out.push_str(text),
                        None => quoted(&brief(k), out),
                    },
                }
                out.push(':');
                after(v, indent, out);
            }
        }
        Value::Sequence(items) => {
            for v in items {
                out.push_str(&pad);
                out.push('-');
                after(v, indent, out);
            }
        }
        _ => {}
    }
}

/// `v` after a key's `:` or an item's `-`: on the line, or below it.
fn after(v: &Value, indent: usize, out: &mut String) {
    let v = match v {
        Value::Tagged(t) if raw_text(v).is_none() => {
            let _ = write!(out, " {}", t.tag);
            &t.value
        }
        v => v,
    };
    match v {
        Value::Mapping(m) if !m.is_empty() => {
            out.push('\n');
            block(v, indent + 2, out);
        }
        Value::Sequence(items) if !items.is_empty() => {
            out.push('\n');
            block(v, indent + 2, out);
        }
        v => {
            match v {
                Value::String(s) => {
                    out.push(' ');
                    quoted(s, out);
                }
                Value::Mapping(_) => out.push_str(" {}"),
                Value::Sequence(_) => out.push_str(" []"),
                v => match raw_text(v) {
                    Some("") => {}
                    Some(text) => {
                        out.push(' ');
                        out.push_str(text);
                    }
                    None => {
                        out.push(' ');
                        out.push_str(serde_yaml::to_string(v).unwrap_or_default().trim_end());
                    }
                },
            }
            out.push('\n');
        }
    }
}

/// `s` double-quoted, with every character YAML would not take as it is
/// (a control character, a line break of its own) escaped.
fn quoted(s: &str, out: &mut String) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\t' => out.push_str("\\t"),
            '\r' => out.push_str("\\r"),
            ' '..='~' | '\u{a0}'..='\u{2027}' | '\u{202a}'..='\u{d7ff}' => out.push(c),
            '\u{e000}'..='\u{fefe}' | '\u{ff00}'..='\u{fffd}' | '\u{10000}'.. => out.push(c),
            // what is left is all below U+10000
            c => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
        }
    }
    out.push('"');
}
