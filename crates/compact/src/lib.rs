//! `compact`: records as text a model reads in a fraction of the tokens, and
//! back. Lossless: [`decode`] of [`encode`] is the records, value for value.
//!
//! ```text
//! # compact v2, 3 records
//! ## 3 records
//! awsRegion=us-west-2
//! source=cloudtrail
//! userAgent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko)
//! eventTime(2026-10-20)
//! 0 eventTime=13:00:02 eventName=GetObject requestParameters.key=exports/2026-10.csv
//! 1 eventTime=13:00:14 eventName=GetObject requestParameters.key=exports/2026-11.csv
//! 2 eventTime=13:01:40 eventName=PutBucketPolicy requestParameters.bucketName=exports
//! # end
//! ```
//!
//! Lines, by how they start: `#` the chunk's header (its record count), a
//! group's (`## N records`: one principal's records, every source) or the
//! chunk's last line, `# end`; `@N=` an alias for a long value the chunk
//! repeats; a digit, a record: its index, then its fields as `path=value`
//! separated by spaces; anything else, a field every record of the group has
//! with one value (`path,path=value`), or `path(2026-10-20)`, which says the
//! rows hold that field's times of day on that date. A path is keys joined by
//! `.`; a key that is not a plain name is a JSON string. A row writes its
//! `keep` fields whatever they repeat; only the rest are shared above it.
//!
//! Values: `null`, `true`, `false` and numbers are themselves; `"..."` a JSON
//! string; `{...}` and `[...]` JSON in which a bare `@N` is an alias; `@N` an
//! alias; `&N` a value kept in the sidecar. Anything else is the string as
//! written. The text is printable ASCII. A string is bare only if nothing
//! else could read the same: printable ASCII starting with a letter (or one
//! of `_ / < ( $ ~ % ! ? : ; '`), without `" \ =`, not a word like `true`
//! or `None`, and in a row without a space. Quoted strings escape controls and
//! every non-ASCII character. So no value or key can end a field, a row or a
//! section, and no character can hide or pass for another.

use anyhow::{anyhow, bail, ensure, Result};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::ops::Range;
use std::sync::LazyLock;

/// What the model is told about the format, after the prompt.
pub const LEGEND: &str = "The records are in compact v2. `## N records` starts a group of records. Each \
`path=value` line under it is a field every record in the group has with that value (`a,b=v`: both fields). A \
line `path(2026-09-08)` says that field's values in the rows are times of day on that date. Then each row is one \
record: its index `i`, then its own fields as `path=value`, separated by spaces (`a,b=v`: both fields). \
`@N=value` names a long value the text repeats, and `@N` stands for it; `&N` is an opaque id kept out of the \
text. A string is quoted when it could read as something else, and in a row also when it holds a space; \
non-ASCII characters are \\u escapes.";

/// The `logfmt` encoding: a request's records grouped by principal, a line
/// each, the boring fields a group shares written once above its lines and
/// the fields that matter (`keep`) on every line; long repeated values as
/// aliases, a day's timestamps as times of day. Lossless: the text decodes to
/// the records. A request is split between records at `budget_tokens`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CompactConfig {
    /// Per value of the records' `source` field, the fields that name a
    /// principal, hold times, or hold opaque ids. A source not listed is
    /// written the same way, without groups by principal or times of day.
    #[serde(default)]
    pub sources: BTreeMap<String, SourceSpec>,

    /// Fields every record's line writes whatever they repeat, for every
    /// source: which records are context, who acted, the risk flags.
    #[serde(default)]
    pub keep: Vec<String>,

    /// Tokens per request, estimated; 90k keeps a request under the 100k
    /// past which some models bill at a higher rate.
    #[serde(default = "default_budget_tokens")]
    pub budget_tokens: usize,

    /// The `ids` fields' values out of the text: each written `&N`, the same
    /// N for the same value.
    #[serde(default)]
    pub sidecar: bool,
}

/// Paths are dotted, as in `fields` and `group_by`: a key holding the dot
/// itself, or the fields nested under it.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct SourceSpec {
    /// Tried in order; the first present names the record's principal.
    #[serde(default)]
    pub principal: Vec<String>,
    /// RFC 3339 timestamps. Those of a group on one date are written as the
    /// date once, on a `path(date)` line, and the time of day in each row.
    #[serde(default)]
    pub time: Vec<String>,
    /// Fields this source's lines write whatever they repeat: the action,
    /// the target, the parameters that decide what happened.
    #[serde(default)]
    pub keep: Vec<String>,
    /// High-entropy ids, for `sidecar`.
    #[serde(default)]
    pub ids: Vec<String>,
}

const fn default_budget_tokens() -> usize {
    90_000
}

/// serde_json's own limit: no record tangent holds is deeper.
const MAX_DEPTH: usize = 128;
/// Bytes an alias must save to be worth a lookup, and the shortest value it
/// stands for: a model reading shorter ones misreads more than they save.
const ALIAS_SAVES: usize = 24;
const ALIAS_MIN: usize = 40;
/// The shortest value fields share one line for.
const SHARE_MIN: usize = 16;
/// Bytes per token, low on purpose: an estimate that errs toward smaller requests.
const BYTES_PER_TOKEN: f64 = 2.0;

pub struct Chunk {
    pub records: Range<usize>,
    pub text: String,
    /// `&N=value` lines, with `sidecar`.
    pub side: Option<String>,
}

/// The records in chunks of at most `budget_tokens`, split between records;
/// a record alone over the budget is a chunk of its own.
pub fn encode(records: &[Value], cfg: &CompactConfig) -> Result<Vec<Chunk>> {
    let fits = |text: &str| text.len() as f64 / BYTES_PER_TOKEN <= cfg.budget_tokens as f64;
    let mut chunks = Vec::new();
    let mut start = 0;
    while start < records.len() {
        let (text, side) = encode_chunk(&records[start..], cfg)?;
        if fits(&text) {
            chunks.push(Chunk {
                records: start..records.len(),
                text,
                side,
            });
            break;
        }
        // the longest run from `start` that fits; at least one record
        let (mut lo, mut hi) = (start + 1, records.len() - 1);
        while lo < hi {
            let mid = (lo + hi).div_ceil(2);
            if fits(&encode_chunk(&records[start..mid], cfg)?.0) {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        let (text, side) = encode_chunk(&records[start..lo], cfg)?;
        chunks.push(Chunk {
            records: start..lo,
            text,
            side,
        });
        start = lo;
    }
    Ok(chunks)
}

// ---- values ----------------------------------------------------------------

type Path = Vec<String>;

/// A field's value as the text writes it: the JSON value, or a sidecar ref.
#[derive(Clone)]
enum V {
    J(Value),
    Ref(usize),
}

/// One string per value: the same for equal values, different otherwise.
fn canon(v: &V) -> String {
    match v {
        V::J(j) => canon_json(j),
        V::Ref(n) => format!("&{n}"),
    }
}

/// JSON with each object's keys sorted: the maps keep the order keys arrived
/// in (serde_json's `preserve_order`, which rig turns on).
fn canon_json(v: &Value) -> String {
    match v {
        Value::Array(a) => format!(
            "[{}]",
            a.iter().map(canon_json).collect::<Vec<_>>().join(",")
        ),
        Value::Object(m) => format!(
            "{{{}}}",
            sorted(m)
                .map(|(k, x)| format!("{}:{}", Value::from(k.as_str()), canon_json(x)))
                .collect::<Vec<_>>()
                .join(",")
        ),
        other => other.to_string(),
    }
}

fn sorted(m: &Map<String, Value>) -> impl Iterator<Item = (&String, &Value)> {
    let mut entries: Vec<(&String, &Value)> = m.iter().collect();
    entries.sort_by_key(|(k, _)| *k);
    entries.into_iter()
}

fn depth_ok(v: &Value, depth: usize) -> bool {
    depth <= MAX_DEPTH
        && match v {
            Value::Array(a) => a.iter().all(|x| depth_ok(x, depth + 1)),
            Value::Object(m) => m.values().all(|x| depth_ok(x, depth + 1)),
            _ => true,
        }
}

/// A JSON string in printable ASCII: `|`, controls and every non-ASCII
/// character escaped.
fn q(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ' '..='~' => out.push(c),
            _ => {
                let mut buf = [0u16; 2];
                for unit in c.encode_utf16(&mut buf) {
                    out.push_str(&format!("\\u{unit:04x}"));
                }
            }
        }
    }
    out.push('"');
    out
}

const BARE_FIRST: &str = "_/<($~%!?:;'";
const WORDS: [&str; 9] = [
    "true",
    "false",
    "null",
    "none",
    "nan",
    "inf",
    "infinity",
    "undefined",
    "nil",
];

/// Whether `s` can be written as itself: nothing else may read the same.
fn bare(s: &str) -> bool {
    let Some(first) = s.chars().next() else {
        return false;
    };
    (first.is_ascii_alphabetic() || BARE_FIRST.contains(first))
        && !s.ends_with(' ')
        && !WORDS.iter().any(|w| s.eq_ignore_ascii_case(w))
        && s.chars()
            .all(|c| matches!(c, ' '..='~') && !matches!(c, '"' | '\\' | '='))
}

// ---- paths -----------------------------------------------------------------

static SEGMENT: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[A-Za-z_$@][A-Za-z0-9_$@:\-]*").unwrap());

fn seg(k: &str) -> String {
    let plain = SEGMENT.find(k).is_some_and(|m| m.len() == k.len())
        && !(k.starts_with('@') && k[1..].starts_with(|c: char| c.is_ascii_digit()))
        && k != "i";
    if plain {
        k.to_string()
    } else {
        q(k)
    }
}

fn path_str(p: &[String]) -> String {
    p.iter().map(|k| seg(k)).collect::<Vec<_>>().join(".")
}

fn paths_str(ps: &[Path]) -> String {
    ps.iter().map(|p| path_str(p)).collect::<Vec<_>>().join(",")
}

/// A path as the config names it.
fn dotted(p: &[String]) -> String {
    p.join(".")
}

fn flatten(rec: &Map<String, Value>) -> Vec<(Path, Value)> {
    fn walk(v: &Value, p: &mut Path, out: &mut Vec<(Path, Value)>) {
        match v {
            Value::Object(m) if !m.is_empty() => {
                for (k, x) in m {
                    p.push(k.clone());
                    walk(x, p, out);
                    p.pop();
                }
            }
            _ => out.push((p.clone(), v.clone())),
        }
    }
    let mut out = Vec::new();
    for (k, v) in rec {
        walk(v, &mut vec![k.clone()], &mut out);
    }
    out
}

fn unflatten(leaves: Vec<(Path, Value)>) -> Result<Value> {
    let mut rec = Map::new();
    for (p, v) in leaves {
        let (last, init) = p.split_last().ok_or_else(|| anyhow!("empty path"))?;
        let mut cur = &mut rec;
        for k in init {
            let next = cur
                .entry(k.clone())
                .or_insert_with(|| Value::Object(Map::new()));
            cur = next
                .as_object_mut()
                .ok_or_else(|| anyhow!("path {} crosses a value", path_str(&p)))?;
        }
        ensure!(!cur.contains_key(last), "path {} set twice", path_str(&p));
        cur.insert(last.clone(), v);
    }
    Ok(Value::Object(rec))
}

/// The field at a dotted path: a key holding the whole path first, as the
/// inference node's `fields` writes one, then the nested fields.
fn lookup<'a>(rec: &'a Value, path: &str) -> Option<&'a Value> {
    rec.get(path)
        .or_else(|| path.split('.').try_fold(rec, |cur, k| cur.get(k)))
}

// ---- times -----------------------------------------------------------------

static CLOCK: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^([0-9]{4}-[0-9]{2}-[0-9]{2})T([0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,9})?)(Z|[+-][0-9]{2}:[0-9]{2})$")
        .unwrap()
});
static TIME_OF_DAY: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,9})?$").unwrap());
static CLOCK_TAG: LazyLock<Regex> = LazyLock::new(|| {
    Regex::new(r"^\(([0-9]{4}-[0-9]{2}-[0-9]{2})(?: ([+-][0-9]{2}:[0-9]{2}))?\)$").unwrap()
});

/// A timestamp field whose values in a group share a date and a zone: those
/// go on its `path(date)` line and the time of day in each row. Lossless
/// whatever the strings say: a value is the date, `T`, the row's time and the zone.
#[derive(Clone, PartialEq)]
struct Clock {
    date: String,
    zone: String,
}

fn clock_of(values: &[Option<V>]) -> Option<Clock> {
    let mut clock: Option<Clock> = None;
    for v in values.iter().flatten() {
        let V::J(Value::String(s)) = v else {
            return None;
        };
        let c = CLOCK.captures(s)?;
        let this = Clock {
            date: c[1].into(),
            zone: c[3].into(),
        };
        match &clock {
            Some(k) if *k != this => return None,
            Some(_) => {}
            None => clock = Some(this),
        }
    }
    clock
}

fn clock_cell(v: &Value) -> String {
    v.as_str()
        .and_then(|s| CLOCK.captures(s))
        .map_or(String::new(), |c| c[2].to_string())
}

fn clock_tag(c: &Clock) -> String {
    if c.zone == "Z" {
        format!("({})", c.date)
    } else {
        format!("({} {})", c.date, c.zone)
    }
}

fn read_clock(tag: &str) -> Result<Clock> {
    let c = CLOCK_TAG
        .captures(tag)
        .ok_or_else(|| anyhow!("bad time tag {:?}", head(tag)))?;
    Ok(Clock {
        date: c[1].into(),
        zone: c.get(2).map_or("Z", |m| m.as_str()).into(),
    })
}

fn from_clock(cell: &str, c: &Clock) -> Result<Value> {
    ensure!(
        TIME_OF_DAY.is_match(cell),
        "bad time of day {:?}",
        head(cell)
    );
    Ok(Value::String(format!("{}T{cell}{}", c.date, c.zone)))
}

// ---- encode ----------------------------------------------------------------

/// A group's records: each one's index and its fields.
type Rows = Vec<(usize, BTreeMap<Path, V>)>;

/// One principal's records as lines: the boring fields they all share once,
/// then a row per record holding its kept fields whatever they repeat, and
/// what else varies.
struct Lines {
    rows: Vec<usize>,
    constants: Vec<(Path, V)>,
    fields: Vec<Field>,
}

/// A field of the rows, or several holding the same value in every row.
struct Field {
    paths: Vec<Path>,
    values: Vec<Option<V>>,
    time: Option<Clock>,
    kept: bool,
}

fn under(p: &str, names: &[String]) -> bool {
    names.iter().any(|k| {
        p == k
            || p.strip_prefix(k.as_str())
                .is_some_and(|r| r.starts_with('.'))
    })
}

impl Lines {
    fn new(rows: Rows, times: &BTreeSet<String>, keep: &[String]) -> Lines {
        let n = rows.len();
        let mut paths: Vec<Path> = rows
            .iter()
            .flat_map(|(_, l)| l.keys().cloned())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect();
        paths.sort_by_key(|p| (p[0].starts_with('_'), path_str(p)));
        let (mut constants, mut fields) = (Vec::new(), Vec::<Field>::new());
        let mut by_values: HashMap<Vec<Option<String>>, usize> = HashMap::new();
        for p in paths {
            let values: Vec<Option<V>> = rows.iter().map(|(_, l)| l.get(&p).cloned()).collect();
            let have: Vec<&V> = values.iter().flatten().collect();
            let kept = under(&dotted(&p), keep);
            if !kept
                && have.len() == n
                && have.iter().map(|v| canon(v)).collect::<HashSet<_>>().len() == 1
            {
                constants.push((p, have[0].clone()));
                continue;
            }
            // a field equal to another in every row is written once, named twice
            let key: Vec<Option<String>> = values.iter().map(|v| v.as_ref().map(canon)).collect();
            if let Some(&k) = by_values.get(&key) {
                fields[k].paths.push(p);
                fields[k].kept |= kept;
                continue;
            }
            by_values.insert(key, fields.len());
            fields.push(Field {
                paths: vec![p],
                values,
                time: None,
                kept,
            });
        }
        for f in &mut fields {
            if f.paths.iter().any(|p| times.contains(&dotted(p))) {
                f.time = clock_of(&f.values);
            }
        }
        // times first, then the kept fields in the order they are kept, then the rest
        fields.sort_by_key(|f| {
            let names: Vec<String> = f.paths.iter().map(|p| dotted(p)).collect();
            let t = times.iter().position(|t| names.contains(t));
            let k = keep
                .iter()
                .position(|k| names.iter().any(|x| under(x, std::slice::from_ref(k))));
            (
                t.is_none(),
                t.unwrap_or(0),
                k.is_none(),
                k.unwrap_or(0),
                f.paths[0][0].starts_with('_'),
                path_str(&f.paths[0]),
            )
        });
        Lines {
            rows: rows.into_iter().map(|(i, _)| i).collect(),
            constants,
            fields,
        }
    }

    /// Constants, one line per value: every path that has it, then the value.
    fn constant_lines(&self) -> Vec<(Vec<Path>, V)> {
        let mut out: Vec<(Vec<Path>, V)> = Vec::new();
        let mut by: HashMap<String, usize> = HashMap::new();
        for (p, v) in &self.constants {
            // a short value keeps its own line: merged, `_context,_facts.ip.ours=true` hides a flag
            let k = canon(v);
            if k.len() < SHARE_MIN {
                out.push((vec![p.clone()], v.clone()));
                continue;
            }
            if let Some(&at) = by.get(&k) {
                out[at].0.push(p.clone());
            } else {
                by.insert(k, out.len());
                out.push((vec![p.clone()], v.clone()));
            }
        }
        out
    }

    /// Every string an alias may stand for, in the order the text holds them:
    /// the shared fields and what varies, never a kept field.
    fn strings(&self, out: &mut Vec<String>) {
        fn walk(v: &Value, out: &mut Vec<String>) {
            match v {
                Value::String(s) => out.push(s.clone()),
                Value::Array(a) => a.iter().for_each(|x| walk(x, out)),
                Value::Object(m) => sorted(m).for_each(|(_, x)| walk(x, out)),
                _ => {}
            }
        }
        for (_, v) in self.constant_lines() {
            if let V::J(j) = &v {
                walk(j, out);
            }
        }
        for r in 0..self.rows.len() {
            for f in &self.fields {
                if let (None, false, Some(V::J(j))) = (&f.time, f.kept, &f.values[r]) {
                    walk(j, out);
                }
            }
        }
    }
}

fn choose_aliases(strings: &[String]) -> HashMap<String, usize> {
    let mut counts: HashMap<&str, usize> = HashMap::new();
    for s in strings {
        *counts.entry(s).or_default() += 1;
    }
    let mut aliases: HashMap<String, usize> = HashMap::new();
    for s in strings {
        // numbered by first use
        let count = counts[s.as_str()];
        if aliases.contains_key(s) || count < 2 {
            continue;
        }
        let n = aliases.len() + 1;
        let written = if bare(s) { s.len() } else { q(s).len() };
        let cost = format!("@{n}=\n").len() + count * format!("@{n}").len();
        if written >= ALIAS_MIN && (count - 1) * written >= cost + ALIAS_SAVES {
            aliases.insert(s.clone(), n);
        }
    }
    aliases
}

/// A value on a line of its own.
fn value_text(v: &V, aliases: &HashMap<String, usize>) -> String {
    match v {
        V::Ref(n) => format!("&{n}"),
        V::J(Value::String(s)) => match aliases.get(s) {
            Some(n) => format!("@{n}"),
            None if bare(s) => s.clone(),
            None => q(s),
        },
        V::J(j) => compound(j, aliases),
    }
}

/// A value in a row, where a space ends it.
fn word_text(v: &V, aliases: &HashMap<String, usize>) -> String {
    match v {
        V::J(Value::String(s)) if !aliases.contains_key(s) => {
            if bare(s) && !s.contains(' ') {
                s.clone()
            } else {
                q(s)
            }
        }
        _ => value_text(v, aliases),
    }
}

/// JSON, its strings quoted or aliased.
fn compound(v: &Value, aliases: &HashMap<String, usize>) -> String {
    match v {
        Value::String(s) => aliases.get(s).map_or_else(|| q(s), |n| format!("@{n}")),
        Value::Array(a) => format!(
            "[{}]",
            a.iter()
                .map(|x| compound(x, aliases))
                .collect::<Vec<_>>()
                .join(",")
        ),
        Value::Object(m) => format!(
            "{{{}}}",
            sorted(m)
                .map(|(k, x)| format!("{}:{}", q(k), compound(x, aliases)))
                .collect::<Vec<_>>()
                .join(",")
        ),
        other => other.to_string(),
    }
}

fn plural(n: usize) -> &'static str {
    if n == 1 {
        ""
    } else {
        "s"
    }
}

/// One chunk's text and, with `sidecar`, its `&N=` lines.
pub fn encode_chunk(records: &[Value], cfg: &CompactConfig) -> Result<(String, Option<String>)> {
    let generic = SourceSpec::default();
    // per principal: the time and kept fields of every source in it, and its records
    let mut groups: Vec<(String, BTreeSet<String>, Vec<String>, Rows)> = Vec::new();
    let mut refs: HashMap<String, usize> = HashMap::new();
    let mut side: Vec<Value> = Vec::new();
    for (i, rec) in records.iter().enumerate() {
        let obj = rec
            .as_object()
            .ok_or_else(|| anyhow!("record {i} is not a JSON object"))?;
        ensure!(
            depth_ok(rec, 0),
            "record {i} is nested deeper than {MAX_DEPTH}"
        );
        let spec = obj
            .get("source")
            .and_then(Value::as_str)
            .and_then(|s| cfg.sources.get(s))
            .unwrap_or(&generic);
        let mut leaves = BTreeMap::new();
        // in path order, so refs are numbered the same whatever order the keys arrived in
        let flat: BTreeMap<Path, Value> = flatten(obj).into_iter().collect();
        for (p, v) in flat {
            let v = if cfg.sidecar && under(&dotted(&p), &spec.ids) {
                let n = *refs.entry(canon_json(&v)).or_insert_with(|| {
                    side.push(v.clone());
                    side.len()
                });
                V::Ref(n)
            } else {
                V::J(v)
            };
            leaves.insert(p, v);
        }
        // records group by principal, every source in one group, so a principal's records read in
        // order: split by source, the judge missed attacks it caught in JSON
        let principal = spec
            .principal
            .iter()
            .find_map(|p| lookup(rec, p).map(|v| (p, v)));
        let key = principal.map_or(String::new(), |(p, v)| format!("{p}\0{}", canon_json(v)));
        let at = match groups.iter().position(|(k, ..)| *k == key) {
            Some(at) => at,
            None => {
                groups.push((key, BTreeSet::new(), cfg.keep.clone(), Vec::new()));
                groups.len() - 1
            }
        };
        let g = &mut groups[at];
        g.1.extend(spec.time.iter().cloned());
        for k in &spec.keep {
            if !g.2.contains(k) {
                g.2.push(k.clone());
            }
        }
        g.3.push((i, leaves));
    }
    let groups: Vec<Lines> = groups
        .into_iter()
        .map(|(_, times, keep, rows)| Lines::new(rows, &times, &keep))
        .collect();
    let mut strings = Vec::new();
    for g in &groups {
        g.strings(&mut strings);
    }
    let aliases = choose_aliases(&strings);
    let mut defs: Vec<(&String, &usize)> = aliases.iter().collect();
    defs.sort_by_key(|(_, n)| **n);

    let mut out = format!(
        "# compact v2, {} record{}\n",
        records.len(),
        plural(records.len())
    );
    for (s, n) in defs {
        out.push_str(&format!(
            "@{n}={}\n",
            if bare(s) { s.clone() } else { q(s) }
        ));
    }
    let none = HashMap::new();
    for g in &groups {
        out.push_str(&format!(
            "## {} record{}\n",
            g.rows.len(),
            plural(g.rows.len())
        ));
        for (ps, v) in g.constant_lines() {
            out.push_str(&format!(
                "{}={}\n",
                paths_str(&ps),
                value_text(&v, &aliases)
            ));
        }
        for f in &g.fields {
            if let Some(c) = &f.time {
                out.push_str(&format!("{}{}\n", paths_str(&f.paths), clock_tag(c)));
            }
        }
        for (r, i) in g.rows.iter().enumerate() {
            out.push_str(&i.to_string());
            for f in &g.fields {
                let Some(v) = &f.values[r] else { continue };
                let text = match (v, &f.time) {
                    (V::J(j), Some(_)) => clock_cell(j),
                    _ => word_text(v, if f.kept { &none } else { &aliases }),
                };
                out.push_str(&format!(" {}={text}", paths_str(&f.paths)));
            }
            out.push('\n');
        }
    }
    // a chunk cut short anywhere, even inside its last value, is missing this
    out.push_str("# end\n");
    let side = cfg.sidecar.then(|| {
        side.iter()
            .enumerate()
            .map(|(k, v)| format!("&{}={}\n", k + 1, compound(v, &HashMap::new())))
            .collect()
    });
    Ok((out, side))
}

// ---- decode ----------------------------------------------------------------

/// A value an alias or a ref stands for, and the bytes it adds where it is used.
type Named = HashMap<usize, (Value, usize)>;

struct Reader<'a> {
    s: &'a str,
    at: usize,
    aliases: &'a Named,
    side: Option<&'a Named>,
    /// Bytes references may still add: text that expands without bound is refused.
    budget: &'a Cell<usize>,
}

impl Reader<'_> {
    fn peek(&self) -> Option<u8> {
        self.s.as_bytes().get(self.at).copied()
    }

    fn expect(&mut self, b: u8) -> Result<()> {
        ensure!(self.peek() == Some(b), "expected {:?}", b as char);
        self.at += 1;
        Ok(())
    }

    fn string(&mut self) -> Result<String> {
        let bytes = self.s.as_bytes();
        let start = self.at;
        let mut end = start + 1;
        loop {
            match bytes.get(end) {
                None => bail!("unterminated string"),
                Some(b'\\') => end += 2,
                Some(b'"') => break,
                Some(_) => end += 1,
            }
        }
        self.at = end + 1;
        Ok(serde_json::from_str(
            self.s
                .get(start..=end)
                .ok_or_else(|| anyhow!("bad string"))?,
        )?)
    }

    fn reference(&mut self) -> Result<Value> {
        let sigil = self.s.as_bytes()[self.at];
        let digits = self.s.as_bytes()[self.at + 1..]
            .iter()
            .take_while(|b| b.is_ascii_digit())
            .count();
        ensure!(digits > 0, "bad reference");
        let n: usize = self.s[self.at + 1..self.at + 1 + digits].parse()?;
        self.at += 1 + digits;
        let (v, size) = if sigil == b'@' {
            self.aliases
                .get(&n)
                .ok_or_else(|| anyhow!("alias @{n} not defined"))?
        } else {
            let side = self.side.ok_or_else(|| {
                anyhow!("this text keeps values in a sidecar; decode it with its sidecar")
            })?;
            side.get(&n)
                .ok_or_else(|| anyhow!("&{n} not in the sidecar"))?
        };
        spend(self.budget, *size)?;
        Ok(v.clone())
    }

    /// JSON, where a bare `@N` is an alias and `&N` a sidecar value.
    fn value(&mut self, depth: usize) -> Result<Value> {
        ensure!(depth <= MAX_DEPTH + 1, "nested deeper than {MAX_DEPTH}");
        match self.peek().ok_or_else(|| anyhow!("value expected"))? {
            b'{' => {
                self.at += 1;
                let mut m = Map::new();
                if self.peek() == Some(b'}') {
                    self.at += 1;
                    return Ok(Value::Object(m));
                }
                loop {
                    ensure!(self.peek() == Some(b'"'), "key expected");
                    let k = self.string()?;
                    self.expect(b':')?;
                    let v = self.value(depth + 1)?;
                    ensure!(m.insert(k, v).is_none(), "duplicate key");
                    if self.peek() == Some(b'}') {
                        self.at += 1;
                        return Ok(Value::Object(m));
                    }
                    self.expect(b',')?;
                }
            }
            b'[' => {
                self.at += 1;
                let mut a = Vec::new();
                if self.peek() == Some(b']') {
                    self.at += 1;
                    return Ok(Value::Array(a));
                }
                loop {
                    a.push(self.value(depth + 1)?);
                    if self.peek() == Some(b']') {
                        self.at += 1;
                        return Ok(Value::Array(a));
                    }
                    self.expect(b',')?;
                }
            }
            b'"' => Ok(Value::String(self.string()?)),
            b'@' | b'&' => self.reference(),
            _ => {
                let rest = &self.s[self.at..];
                for (lit, v) in [
                    ("null", Value::Null),
                    ("true", Value::Bool(true)),
                    ("false", Value::Bool(false)),
                ] {
                    if rest.starts_with(lit) {
                        self.at += lit.len();
                        return Ok(v);
                    }
                }
                let m = NUMBER
                    .find(rest)
                    .ok_or_else(|| anyhow!("bad value at {:?}", head(rest)))?;
                self.at += m.len();
                Ok(serde_json::from_str(m.as_str())?)
            }
        }
    }
}

/// About what `v` takes as JSON text: what a copy of it costs the budget.
fn size(v: &Value) -> usize {
    match v {
        Value::String(s) => s.len() + 2,
        Value::Array(a) => 2 + a.iter().map(size).sum::<usize>(),
        Value::Object(m) => 2 + m.iter().map(|(k, x)| k.len() + 4 + size(x)).sum::<usize>(),
        _ => 8,
    }
}

/// What a path costs each record it is copied into: a map per key.
fn path_size(p: &Path) -> usize {
    p.iter().map(|k| k.len() + 64).sum()
}

fn spend(budget: &Cell<usize>, bytes: usize) -> Result<()> {
    budget.set(
        budget
            .get()
            .checked_sub(bytes)
            .ok_or_else(|| anyhow!("text expands past any record it could be"))?,
    );
    Ok(())
}

static NUMBER: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?").unwrap());

fn head(s: &str) -> &str {
    &s[..s.char_indices().nth(20).map_or(s.len(), |(i, _)| i)]
}

/// A field's text as a value: everything but times of day, which need their `path(date)` line.
fn read_value(
    cell: &str,
    aliases: &Named,
    side: Option<&Named>,
    budget: &Cell<usize>,
) -> Result<Value> {
    match cell.as_bytes().first() {
        None => bail!("empty cell"),
        Some(b'"' | b'{' | b'[' | b'@' | b'&') => {
            let mut r = Reader {
                s: cell,
                at: 0,
                aliases,
                side,
                budget,
            };
            let v = r.value(0)?;
            ensure!(r.at == cell.len(), "trailing text in {:?}", head(cell));
            Ok(v)
        }
        Some(b'-' | b'0'..=b'9') => {
            ensure!(
                NUMBER.find(cell).is_some_and(|m| m.len() == cell.len()),
                "bad number {:?}",
                head(cell)
            );
            Ok(serde_json::from_str(cell)?)
        }
        Some(b'+') => bail!("bad cell {:?}", head(cell)),
        _ => Ok(match cell {
            "null" => Value::Null,
            "true" => Value::Bool(true),
            "false" => Value::Bool(false),
            _ => Value::String(cell.to_string()),
        }),
    }
}

/// A path at `s[at..]`: its keys and where it ends.
fn read_path(s: &str, mut at: usize) -> Result<(Path, usize)> {
    let mut p = Vec::new();
    loop {
        ensure!(p.len() <= MAX_DEPTH, "path deeper than {MAX_DEPTH}");
        if s[at..].starts_with('"') {
            let mut r = Reader {
                s,
                at,
                aliases: &HashMap::new(),
                side: None,
                budget: &Cell::new(0),
            };
            p.push(r.string()?);
            at = r.at;
        } else {
            let m = SEGMENT
                .find(&s[at..])
                .ok_or_else(|| anyhow!("bad path at {:?}", head(&s[at..])))?;
            p.push(m.as_str().to_string());
            at += m.len();
        }
        if s[at..].starts_with('.') {
            at += 1;
            continue;
        }
        return Ok((p, at));
    }
}

/// Paths joined by `,`.
fn read_paths(s: &str, mut at: usize) -> Result<(Vec<Path>, usize)> {
    let mut ps = Vec::new();
    loop {
        let (p, end) = read_path(s, at)?;
        ps.push(p);
        at = end;
        if s[at..].starts_with(',') {
            at += 1;
            continue;
        }
        return Ok((ps, at));
    }
}

static HEAD: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^# compact v2, ([0-9]+) records?$").unwrap());
static GROUP: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^## ([0-9]+) records?$").unwrap());
static ALIAS: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"^@([0-9]+)=").unwrap());

fn read_side(text: &str, budget: &Cell<usize>) -> Result<Named> {
    ensure!(
        text.is_empty() || text.ends_with('\n'),
        "the sidecar was cut short"
    );
    let mut side = HashMap::new();
    for line in text.lines() {
        let digits = line
            .strip_prefix('&')
            .map_or(0, |l| l.bytes().take_while(u8::is_ascii_digit).count());
        ensure!(
            digits > 0 && line[1 + digits..].starts_with('='),
            "bad sidecar line {:?}",
            head(line)
        );
        let n: usize = line[1..1 + digits].parse()?;
        let mut r = Reader {
            s: line,
            at: digits + 2,
            aliases: &HashMap::new(),
            side: None,
            budget,
        };
        let v = r.value(0)?;
        ensure!(r.at == line.len(), "bad sidecar line {:?}", head(line));
        side.insert(n, (v, line.len()));
    }
    Ok(side)
}

/// `i path=value ...`: a row's index and its fields.
fn read_row(
    line: &str,
    aliases: &Named,
    side: Option<&Named>,
    clocks: &HashMap<Vec<Path>, Clock>,
    budget: &Cell<usize>,
) -> Result<(usize, Vec<(Path, Value)>)> {
    let digits = line.bytes().take_while(u8::is_ascii_digit).count();
    let i: usize = line[..digits].parse()?;
    let mut at = digits;
    let mut leaves = Vec::new();
    while at < line.len() {
        ensure!(
            line.as_bytes()[at] == b' ',
            "bad row at {:?}",
            head(&line[at..])
        );
        let (paths, end) = read_paths(line, at + 1)?;
        ensure!(
            line[end..].starts_with('='),
            "bad row at {:?}",
            head(&line[end..])
        );
        at = end + 1;
        let word = |at: usize| line[at..].find(' ').map_or(line.len(), |k| at + k);
        let v = match clocks.get(&paths) {
            Some(c) => {
                let stop = word(at);
                let v = from_clock(&line[at..stop], c)?;
                at = stop;
                v
            }
            None if line[at..].starts_with(['"', '{', '[', '@', '&']) => {
                let mut r = Reader {
                    s: line,
                    at,
                    aliases,
                    side,
                    budget,
                };
                let v = r.value(0)?;
                at = r.at;
                v
            }
            None => {
                let stop = word(at);
                let v = read_value(&line[at..stop], aliases, side, budget)?;
                at = stop;
                v
            }
        };
        spend(budget, (paths.len() - 1) * size(&v))?;
        leaves.extend(paths.into_iter().map(|p| (p, v.clone())));
    }
    Ok((i, leaves))
}

/// One chunk's records, in order.
pub fn decode(text: &str, side: Option<&str>) -> Result<Vec<Value>> {
    let body = text
        .strip_suffix("\n# end\n")
        .ok_or_else(|| anyhow!("no `# end` line: the text was cut short"))?;
    // what references and shared fields may add: generous for a real chunk, finite for a crafted one
    let budget = Cell::new((64 * (text.len() + side.map_or(0, str::len))).max(1 << 20));
    let mut lines = body.split('\n');
    let first = HEAD
        .captures(lines.next().unwrap_or(""))
        .ok_or_else(|| anyhow!("not a compact v2 chunk"))?;
    let count: usize = first[1].parse()?;
    let side = side.map(|s| read_side(s, &budget)).transpose()?;
    let mut aliases: Named = HashMap::new();
    let mut out: BTreeMap<usize, Value> = BTreeMap::new();
    let (mut consts, mut consts_size): (Vec<(Path, Value)>, usize) = (Vec::new(), 0);
    let mut clocks: HashMap<Vec<Path>, Clock> = HashMap::new();
    let mut groups: Vec<(usize, usize)> = Vec::new();
    for line in lines {
        if let Some(g) = GROUP.captures(line) {
            groups.push((g[1].parse()?, 0));
            consts.clear();
            consts_size = 0;
            clocks.clear();
        } else if let Some(a) = ALIAS.captures(line) {
            let n: usize = a[1].parse()?;
            let v = read_value(&line[a[0].len()..], &HashMap::new(), None, &budget)?;
            aliases.insert(n, (v, line.len()));
        } else if line.starts_with(|c: char| c.is_ascii_digit()) {
            let group = groups
                .last_mut()
                .ok_or_else(|| anyhow!("a row outside a group"))?;
            let (i, row) = read_row(line, &aliases, side.as_ref(), &clocks, &budget)?;
            spend(&budget, consts_size)?;
            let mut leaves = consts.clone();
            leaves.extend(row);
            ensure!(
                out.insert(i, unflatten(leaves)?).is_none(),
                "record {i} twice"
            );
            group.1 += 1;
        } else if line.starts_with('#') {
            bail!("unexpected header {:?}", head(line));
        } else {
            let (ps, at) = read_paths(line, 0)?;
            if line[at..].starts_with('(') {
                clocks.insert(ps, read_clock(&line[at..])?);
                continue;
            }
            ensure!(line[at..].starts_with('='), "bad line {:?}", head(line));
            let v = read_value(&line[at + 1..], &aliases, side.as_ref(), &budget)?;
            let cost = ps.iter().map(|p| path_size(p) + size(&v)).sum();
            spend(&budget, cost)?;
            consts_size += cost;
            consts.extend(ps.into_iter().map(|p| (p, v.clone())));
        }
    }
    ensure!(
        groups.iter().all(|(want, got)| want == got) && out.keys().copied().eq(0..count),
        "records missing: the text was cut short or edited"
    );
    Ok(out.into_values().collect())
}

/// The chunks of one encoding, in order: the records.
pub fn decode_all<'a>(
    chunks: impl IntoIterator<Item = (&'a str, Option<&'a str>)>,
) -> Result<Vec<Value>> {
    let mut out = Vec::new();
    for (text, side) in chunks {
        out.extend(decode(text, side)?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};
    use serde_json::json;

    fn cfg(sidecar: bool, budget_tokens: usize) -> CompactConfig {
        let mut c: CompactConfig = serde_yaml::from_str(
            "{keep: [_context, _norm.actor, _facts.watch], sources: {
               cloudtrail: {principal: [_norm.actor, userIdentity.arn], time: [eventTime, ts],
                            keep: [eventName, requestParameters, errorCode], ids: [eventID, requestID, userIdentity.accessKeyId]},
               okta: {principal: [actor.alternateId], time: [published, ts], keep: [eventType, outcome], ids: [uuid]}}}",
        )
        .unwrap();
        c.sidecar = sidecar;
        c.budget_tokens = budget_tokens;
        c
    }

    fn reverse_keys(v: &Value) -> Value {
        match v {
            Value::Object(m) => Value::Object(
                m.iter()
                    .rev()
                    .map(|(k, x)| (k.clone(), reverse_keys(x)))
                    .collect(),
            ),
            Value::Array(a) => Value::Array(a.iter().map(reverse_keys).collect()),
            other => other.clone(),
        }
    }

    /// Round trip; printable ASCII; deterministic whatever the key order;
    /// damage refused.
    fn strict(records: &[Value], c: &CompactConfig) {
        let chunks = encode(records, c).unwrap();
        let texts = |cs: &[Chunk]| {
            cs.iter()
                .map(|k| (k.text.clone(), k.side.clone()))
                .collect::<Vec<_>>()
        };
        assert_eq!(
            decode_all(chunks.iter().map(|k| (k.text.as_str(), k.side.as_deref()))).unwrap(),
            records
        );
        let reversed: Vec<Value> = records.iter().map(reverse_keys).collect();
        assert_eq!(
            texts(&chunks),
            texts(&encode(&reversed, c).unwrap()),
            "depends on key order"
        );
        for k in &chunks {
            for line in k.text.lines() {
                assert!(
                    line.chars().all(|ch| matches!(ch, ' '..='~')),
                    "not printable ASCII: {line:?}"
                );
            }
            let half = &k.text[..k.text.len() / 2];
            assert!(
                decode(&format!("{half}\n"), k.side.as_deref()).is_err(),
                "decoded a cut chunk"
            );
            assert!(
                decode(&k.text[..k.text.len() - 1], k.side.as_deref()).is_err(),
                "decoded a cut chunk"
            );
            if let Some(side) = k.side.as_deref().filter(|s| !s.is_empty()) {
                assert!(
                    decode(&k.text, Some(&side[..side.len() - 1])).is_err(),
                    "decoded a cut sidecar"
                );
            }
        }
    }

    fn deep(n: usize) -> Value {
        (0..n).fold(json!(1), |v, _| json!([v]))
    }

    #[test]
    fn edge_cases_round_trip() {
        let ct = |t: &str| json!({"source": "cloudtrail", "eventTime": t});
        let cases: Vec<(&str, Vec<Value>)> = vec![
            (
                "absent, null, empty",
                vec![
                    json!({"a": null}),
                    json!({"a": ""}),
                    json!({"a": []}),
                    json!({"a": {}}),
                    json!({}),
                    json!({"b": 1}),
                ],
            ),
            (
                "number or string",
                vec![
                    json!({"a": 123}),
                    json!({"a": "123"}),
                    json!({"a": 1.0}),
                    json!({"a": 1e5}),
                    json!({"a": -0.0}),
                    json!({"a": 8.451888330290004e217}),
                    json!({"a": 422036275.4892504}),
                ],
            ),
            (
                "bool or string",
                vec![
                    json!({"a": true}),
                    json!({"a": "true"}),
                    json!({"a": "False"}),
                    json!({"a": null}),
                    json!({"a": "None"}),
                    json!({"a": ".5"}),
                ],
            ),
            (
                "delimiters in values",
                vec![
                    json!({"a": "x|y", "b": "@1"}),
                    json!({"a": "a\nb", "b": "+12"}),
                    json!({"a": "## 2 records\n0|x", "b": "&1"}),
                    json!({"a": "x,eventName=Root", "b": "# end"}),
                ],
            ),
            (
                "delimiters in keys",
                vec![
                    json!({"a.b": 1, "a": {"b": 2}}),
                    json!({"|": 1, "=": 2, "*": 3, "@1": 4, "": 5, "i": 6, "a,b": 7, "x(2026-01-01)": 8, "a b": 9}),
                ],
            ),
            (
                "non-ASCII",
                vec![
                    json!({"a": "\u{202e}evil"}),
                    json!({"a": "\u{2028}"}),
                    json!({"a": "😀"}),
                    json!({"a": "日本語"}),
                    json!({"a": "\u{430}dmin"}),
                    json!({"a": "\u{338}Root"}),
                    json!({"a": "\u{ff5c}"}),
                ],
            ),
            (
                "sparse",
                vec![
                    json!({"a": 1}),
                    json!({"b": 2}),
                    json!({"c": 3}),
                    json!({"a": 1, "d": {"e": [1, {"f": null}]}}),
                ],
            ),
            (
                "schema drift",
                vec![
                    json!({"a": "x"}),
                    json!({"a": {"b": 1}}),
                    json!({"a": [1]}),
                    json!({"a": 2}),
                    json!({"a": {}}),
                ],
            ),
            (
                "times",
                vec![
                    ct("2026-09-08T00:00:02Z"),
                    ct("2026-09-08T00:00:14Z"),
                    ct("2026-09-08T01:00:02.5Z"),
                ],
            ),
            (
                "times on two days, look-alike digits",
                vec![
                    ct("2026-09-08T00:00:02Z"),
                    ct("2026-09-09T00:00:02Z"),
                    ct("２０26-09-08T00:00:02Z"),
                ],
            ),
            (
                "zones",
                vec![
                    json!({"source": "okta", "published": "2026-09-08T05:30:02.123+05:30"}),
                    json!({"source": "okta", "published": "2026-09-08T05:31:00.000+05:30"}),
                ],
            ),
            (
                "ids",
                vec![
                    json!({"source": "cloudtrail", "eventID": "67954df1-8e0e-329d-8631-16460322f565", "n": 1}),
                    json!({"source": "cloudtrail", "eventID": "67954df1-8e0e-329d-8631-16460322f565", "n": 2}),
                    json!({"source": "cloudtrail", "eventID": 7}),
                ],
            ),
            (
                "ids under dotted keys",
                vec![
                    json!({"source": "cloudtrail", "userIdentity.accessKeyId": "ASIAEXAMPLE1", "eventTime": "2026-09-08T00:00:02Z"}),
                    json!({"source": "cloudtrail", "userIdentity.accessKeyId": "ASIAEXAMPLE2", "eventTime": "2026-09-08T00:00:03Z"}),
                ],
            ),
            (
                "nested 128 deep",
                vec![json!({"a": deep(127)}), json!({"a": deep(3)})],
            ),
        ];
        for (name, records) in cases {
            for c in [cfg(false, 90_000), cfg(true, 90_000), cfg(false, 1)] {
                let r = std::panic::catch_unwind(|| strict(&records, &c));
                assert!(
                    r.is_ok(),
                    "{name} (sidecar {}, budget {})",
                    c.sidecar,
                    c.budget_tokens
                );
            }
        }
        assert!(encode(&[json!({"a": deep(129)})], &cfg(false, 90_000)).is_err());
        assert!(
            encode(&[json!([1])], &cfg(false, 90_000)).is_err(),
            "a record that is not an object"
        );
        let ids = &encode(&cases_ids(), &cfg(true, 90_000)).unwrap()[0];
        assert!(
            !ids.text.contains("ASIA"),
            "an id in the text: {}",
            ids.text
        );
    }

    fn cases_ids() -> Vec<Value> {
        vec![
            json!({"source": "cloudtrail", "userIdentity.accessKeyId": "ASIAEXAMPLE1"}),
            json!({"source": "cloudtrail", "userIdentity": {"accessKeyId": "ASIAEXAMPLE2"}}),
        ]
    }

    #[test]
    fn crafted_text_is_refused() {
        let long = vec!["a"; 20_000].join(".");
        assert!(decode(
            &format!("# compact v2, 1 record\n## 1 record\n{long}=1\n0\n# end\n"),
            None
        )
        .is_err());
        let bomb = format!(
            "# compact v2, 1 record\n@1=\"{}\"\n## 1 record\n0 a=[{}]\n# end\n",
            "x".repeat(10_000),
            vec!["@1"; 20_000].join(",")
        );
        assert!(decode(&bomb, None).is_err());
        // copies of a shared field, of a value under many paths, of a deep path: each costs what it expands to
        let rows = |n: usize| (0..n).map(|i| format!("{i}\n")).collect::<String>();
        let big = format!("\"{}\"", "x".repeat(100_000));
        for text in [
            format!(
                "# compact v2, 5000 records\n@1={big}\n## 5000 records\na=@1\n{}# end\n",
                rows(5000)
            ),
            format!(
                "# compact v2, 1 record\n## 1 record\n{}={big}\n0\n# end\n",
                (0..5000)
                    .map(|k| format!("a{k}"))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            format!(
                "# compact v2, 1 record\n@1={big}\n## 1 record\n0 {}=@1\n# end\n",
                (0..5000)
                    .map(|k| format!("a{k}"))
                    .collect::<Vec<_>>()
                    .join(",")
            ),
            format!(
                "# compact v2, 50000 records\n## 50000 records\n{}=1\n{}# end\n",
                vec!["a"; 128].join("."),
                rows(50_000)
            ),
        ] {
            assert!(decode(&text, None).is_err(), "{}", head(&text));
        }
        assert!(decode(
            "# compact v2, 1 record\n## 1 record\n0 a=\u{ff11}\n# end\n",
            None
        )
        .is_ok_and(|r| r[0]["a"] == "\u{ff11}"));
    }

    #[test]
    fn kept_fields_on_every_row() {
        let rec = |t: &str| {
            json!({"source": "cloudtrail", "eventTime": t, "eventName": "AssumeRole", "awsRegion": "us-west-2",
                                   "requestParameters": {"roleArn": "arn:aws:iam::111122223333:role/r"}, "_norm": {"actor": "bot"}})
        };
        let text = &encode(
            &[rec("2026-09-08T00:00:02Z"), rec("2026-09-08T00:00:19Z")],
            &cfg(false, 90_000),
        )
        .unwrap()[0]
            .text;
        let rows: Vec<&str> = text
            .lines()
            .filter(|l| l.starts_with(|c: char| c.is_ascii_digit()))
            .collect();
        assert_eq!(rows.len(), 2);
        for row in rows {
            assert!(
                row.contains(" eventName=AssumeRole") && row.contains(" _norm.actor=bot"),
                "{row}"
            );
            assert!(
                row.contains(" requestParameters.roleArn=arn:aws:iam::111122223333:role/r"),
                "{row}"
            );
            assert!(!row.contains("awsRegion"), "{row}");
        }
        assert!(
            text.contains("\nawsRegion=us-west-2\n") && text.contains("\neventTime(2026-09-08)\n"),
            "{text}"
        );
    }

    const NASTY: &[&str] = &[
        "",
        " ",
        "  x",
        "x ",
        "|",
        "a|b",
        "=",
        "x,a=b",
        "@1",
        "&1",
        "+5",
        "-",
        "-5",
        "#",
        "## 3 records",
        "# end",
        "i",
        "i|x",
        "*",
        "null",
        "True",
        "None",
        "NaN",
        "123",
        "-0",
        "1.0",
        ".5",
        "\u{2212}5",
        "\u{ff11}",
        "\u{ff5c}",
        "\"",
        "\\",
        "\\u007c",
        "a\nb",
        "a\r\nb",
        "\t",
        "\0",
        "\x1b[31m",
        "\x7f",
        "\u{2028}",
        "\u{85}",
        "\u{200b}",
        "\u{202e}",
        "\u{feff}",
        "e\u{301}",
        "\u{338}R",
        "\u{3164}",
        "😀",
        "日本語",
        "{",
        "[]",
        "{}",
        "<x>",
        "(x)",
        "2026-09-08T00:00:02Z",
        "2026-09-08T00:00:02.000Z",
        "2024-02-30T00:00:00Z",
        "9999-12-31T23:59:59.999999999+14:00",
        "arn:aws:iam::111122223333:role/r",
    ];
    const KEYS: &[&str] = &[
        "a",
        "b",
        "source",
        "eventTime",
        "ts",
        "_norm",
        "actor",
        "",
        ".",
        "a.b",
        "a,b",
        "\"",
        "|",
        "=",
        "*",
        "@1",
        "@timestamp",
        "0",
        "i",
        "#",
        " ",
        "日本",
        "\u{202e}",
        "eventID",
        "userIdentity",
        "userIdentity.accessKeyId",
    ];

    fn scalar(rng: &mut StdRng) -> Value {
        match rng.random_range(0..10) {
            0..=4 => json!(NASTY[rng.random_range(0..NASTY.len())]),
            5 => json!([0i64, -1, i64::MIN, i64::MAX][rng.random_range(0..4)]),
            6 => json!(Some(f64::from_bits(rng.random::<u64>()))
                .filter(|f| f.is_finite())
                .unwrap_or(0.5)),
            7 => [Value::Null, json!(true), json!(false), json!(u64::MAX)][rng.random_range(0..4)]
                .clone(),
            8 => json!((0..rng.random_range(1..4))
                .map(|_| NASTY[rng.random_range(0..NASTY.len())])
                .collect::<String>()),
            _ => json!((0..rng.random_range(1..8))
                .map(|_| char::from_u32(rng.random_range(0..0x11_0000)).unwrap_or('?'))
                .collect::<String>()),
        }
    }

    fn value(rng: &mut StdRng, depth: usize) -> Value {
        match rng.random_range(0..10) {
            _ if depth > 3 => scalar(rng),
            0..=5 => scalar(rng),
            6..=7 => Value::Object(
                (0..rng.random_range(0..4))
                    .map(|_| {
                        (
                            KEYS[rng.random_range(0..KEYS.len())].to_string(),
                            value(rng, depth + 1),
                        )
                    })
                    .collect(),
            ),
            _ => Value::Array(
                (0..rng.random_range(0..3))
                    .map(|_| value(rng, depth + 1))
                    .collect(),
            ),
        }
    }

    /// Hostile records, shaped so groups, constants, columns, aliases, times
    /// and ids all occur. `COMPACT_FUZZ=N` runs N batches (default 300).
    #[test]
    fn fuzz_round_trips() {
        let n: usize = std::env::var("COMPACT_FUZZ")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(300);
        let mut rng = StdRng::seed_from_u64(7);
        for batch in 0..n {
            let base: Map<String, Value> = (0..rng.random_range(0..5))
                .map(|_| {
                    (
                        KEYS[rng.random_range(0..KEYS.len())].to_string(),
                        value(&mut rng, 0),
                    )
                })
                .collect();
            let records: Vec<Value> = (0..rng.random_range(1..30))
                .map(|_| {
                    let mut r = if rng.random_bool(0.7) {
                        base.clone()
                    } else {
                        Map::new()
                    };
                    for _ in 0..rng.random_range(0..6) {
                        r.insert(
                            KEYS[rng.random_range(0..KEYS.len())].to_string(),
                            value(&mut rng, 0),
                        );
                    }
                    if rng.random_bool(0.5) {
                        r.insert(
                            "source".into(),
                            json!(["cloudtrail", "okta", "other"][rng.random_range(0..3)]),
                        );
                    }
                    if rng.random_bool(0.5) {
                        r.insert(
                            "eventTime".into(),
                            json!(format!("2026-09-08T00:00:{:02}Z", rng.random_range(0..60))),
                        );
                    }
                    if rng.random_bool(0.2) {
                        r.insert(
                            "eventID".into(),
                            json!(format!("{:032x}", rng.random::<u128>())),
                        );
                    }
                    Value::Object(r)
                })
                .collect();
            let c = cfg(
                rng.random_bool(0.4),
                [90_000, 1, 300, 2000][rng.random_range(0..4)],
            );
            let r = std::panic::catch_unwind(|| strict(&records, &c));
            assert!(
                r.is_ok(),
                "batch {batch} (sidecar {}, budget {}): {}",
                c.sidecar,
                c.budget_tokens,
                Value::Array(records)
            );
        }
    }

    /// The pack's judge-input fixture (synthetic), encoded as checked in.
    /// `UPDATE_GOLDEN=1` writes the expected text instead.
    #[test]
    fn golden() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("testdata");
        let records: Vec<Value> =
            serde_json::from_str(&std::fs::read_to_string(dir.join("records.json")).unwrap())
                .unwrap();
        for (name, c) in [
            ("compact.txt", cfg(false, 90_000)),
            ("compact-sidecar.txt", cfg(true, 3_000)),
        ] {
            let got: String = encode(&records, &c)
                .unwrap()
                .iter()
                .map(|k| {
                    format!(
                        "{}{}",
                        k.text,
                        k.side
                            .as_ref()
                            .map_or(String::new(), |s| format!("--- sidecar\n{s}"))
                    )
                })
                .collect();
            if std::env::var("UPDATE_GOLDEN").is_ok() {
                std::fs::write(dir.join(name), &got).unwrap();
            }
            assert_eq!(
                got,
                std::fs::read_to_string(dir.join(name)).unwrap(),
                "{name}"
            );
            strict(&records, &c);
        }
    }
}
