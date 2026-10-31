//! `include:` and the config loader: every merge rule, the paths, and what a
//! config without `include:` still reads as.

use std::fs;
use std::path::{Path, PathBuf};

use tangent_shared::dag::NodeRef;
use tangent_shared::enrichment::{TableConfig, TableKind};
use tangent_shared::inference::InferenceConfig;
use tangent_shared::plugins::{PluginConfig, PluginTests};
use tangent_shared::sources::common::SourceConfig;
use tangent_shared::{Config, Loaded};
use tempfile::TempDir;

/// A directory holding `files`, each `(path, text)`.
fn tree(files: &[(&str, &str)]) -> TempDir {
    let dir = tempfile::tempdir().unwrap();
    for (name, text) in files {
        let path = dir.path().join(name);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, text).unwrap();
    }
    dir
}

fn load(dir: &TempDir, name: &str) -> anyhow::Result<Loaded> {
    Config::load(&dir.path().join(name))
}

fn error(dir: &TempDir, name: &str) -> String {
    match load(dir, name) {
        Ok(_) => panic!("{name} loaded"),
        Err(e) => format!("{e:#}"),
    }
}

fn canonical(dir: &TempDir, rel: &str) -> PathBuf {
    dir.path().canonicalize().unwrap().join(rel)
}

fn yaml(cfg: &Config) -> String {
    serde_yaml::to_string(cfg).unwrap()
}

fn plugin(name: &str) -> NodeRef {
    NodeRef::Plugin { name: name.into() }
}

fn sink(name: &str) -> NodeRef {
    NodeRef::Sink {
        name: name.into(),
        key_prefix: None,
    }
}

const PIPELINE: &str = "
runtime:
  workers: 2
sources:
  gh:
    type: github_audit
    org: 12345
    since: 3600
inference:
  judge:
    url: ${TANGENT_TEST_UNSET_URL}
    model:
    prompt_file: judge.md
sinks:
  out: { type: file, path: out.ndjson }
dag:
  - from: { kind: source, name: gh }
    to: [{ kind: inference, name: judge }]
  - from: { kind: inference, name: judge }
    to: [{ kind: sink, name: out }]
";

#[test]
fn a_root_without_include_loads_as_before() {
    let dir = tree(&[("tangent.yaml", PIPELINE)]);
    let loaded = load(&dir, "tangent.yaml").unwrap();
    // as the loader before `include:` read it: the variable expanded, then
    // the text read straight into the config
    let before: Config =
        serde_yaml::from_str(&PIPELINE.replace("${TANGENT_TEST_UNSET_URL}", "")).unwrap();
    assert_eq!(yaml(&loaded.config), yaml(&before));

    let cfg = &loaded.config;
    let SourceConfig::GithubAudit(gh) = &cfg.sources["gh"] else {
        panic!("github_audit");
    };
    assert_eq!(
        gh.since, "3600",
        "a bare number is the string a String wants"
    );
    assert_eq!(gh.org.as_deref(), Some("12345"), "an all-digit org");
    let judge = &cfg.inference["judge"];
    assert_eq!(judge.model, "", "an empty `model:` is the empty string");
    assert_eq!(judge.url, "", "an unset ${{VAR}} is empty, as before");
    assert_eq!(
        judge.prompt_file,
        PathBuf::from("judge.md"),
        "the root's own paths stay as written"
    );
    assert_eq!(loaded.files, vec![dir.path().join("tangent.yaml")]);
}

#[test]
fn merged_text_keeps_the_scalars_lenient() {
    let dir = tree(&[
        (
            "source.yaml",
            "sources:\n  gh: { type: github_audit, org: 12345, since: 3600 }\n",
        ),
        (
            "judge.yaml",
            "inference:\n  judge: { model: 4, url: 8080, prompt_file: 7.md, temperature: 1 }\n",
        ),
        (
            "root.yaml",
            "include: [source.yaml, judge.yaml]\nsources:\n  gh: { interval_secs: 30 }\n",
        ),
    ]);
    let cfg = load(&dir, "root.yaml").unwrap().config;
    let SourceConfig::GithubAudit(gh) = &cfg.sources["gh"] else {
        panic!("github_audit");
    };
    assert_eq!(gh.since, "3600");
    assert_eq!(gh.org.as_deref(), Some("12345"));
    assert_eq!(gh.interval_secs, 30);
    let judge = &cfg.inference["judge"];
    assert_eq!(
        judge.model, "4",
        "a number where a string goes, as in a file"
    );
    assert_eq!(judge.url, "8080");
    assert_eq!(judge.temperature, Some(1.0));
}

/// A bare scalar a string field takes is the text its file wrote, with
/// `include:` as without: serde_yaml alone would read `1.10` as 1.1, `0x1F`
/// as 31, `True` as true.
#[test]
fn a_scalar_reads_as_its_file_wrote_it() {
    for written in ["1.10", "0x1F", "True", "+12", "1e3", "0o17", ".inf", "007"] {
        let judge = format!(
            "inference:\n  judge: {{ model: {written}, url: {written}, prompt_file: judge.md, temperature: 1.50 }}\n"
        );
        let dir = tree(&[
            ("plain.yaml", judge.as_str()),
            ("empty.yaml", ""),
            ("inc.yaml", &format!("include: [empty.yaml]\n{judge}")),
            ("judge.yaml", judge.as_str()),
            ("root.yaml", "include: [judge.yaml]\n"),
        ]);
        for name in ["plain.yaml", "inc.yaml", "root.yaml"] {
            let cfg = load(&dir, name).unwrap().config;
            let judge = &cfg.inference["judge"];
            assert_eq!(judge.model, written, "{name}");
            assert_eq!(judge.url, written, "{name}");
            assert_eq!(
                judge.temperature,
                Some(1.5),
                "{name}: a number is still one"
            );
        }
    }
}

/// Two files agree on a value only as written: to a string field `1.1` and
/// `1.10` differ.
#[test]
fn siblings_agree_on_a_scalar_as_written() {
    let dir = tree(&[
        (
            "a.yaml",
            "inference:\n  judge: { model: 1.10, prompt_file: j.md }\n",
        ),
        (
            "b.yaml",
            "inference:\n  judge: { model: 1.1, prompt_file: j.md }\n",
        ),
        ("c.yaml", "inference:\n  judge: { model: 1.10 }\n"),
        ("disagree.yaml", "include: [a.yaml, b.yaml]\n"),
        ("agree.yaml", "include: [a.yaml, c.yaml]\n"),
    ]);
    let err = error(&dir, "disagree.yaml");
    assert!(err.contains("inference.judge.model: "), "{err}");
    assert!(err.contains("a.yaml has 1.10, "), "{err}");
    assert!(
        err.contains(
            "b.yaml has 1.1
"
        ) || err.ends_with("b.yaml has 1.1"),
        "{err}"
    );
    let cfg = load(&dir, "agree.yaml").unwrap().config;
    assert_eq!(cfg.inference["judge"].model, "1.10");
}

/// A plugin's `config` and a node's `extra` are handed on as written, so a
/// null in them is a value: what `response_format: ~` says to the server
/// (no JSON mode) holds with `include:` too.
#[test]
fn a_null_is_a_value_in_a_map_handed_on_as_written() {
    let pipeline = "
plugins:
  p: { module_type: rust, path: p, config: { mode: ~, keep: [a], deep: { off: null } } }
inference:
  judge:
    model: m
    prompt_file: judge.md
    extra: { response_format: ~, chat_template_kwargs: { enable_thinking: false, x: } }
";
    let dir = tree(&[
        ("plain.yaml", pipeline),
        ("empty.yaml", ""),
        ("inc.yaml", &format!("include: [empty.yaml]\n{pipeline}")),
        ("pack.yaml", pipeline),
        (
            "over.yaml",
            "
include: [pack.yaml]
plugins:
  p: { config: { keep: ~ } }
inference:
  judge: { extra: { chat_template_kwargs: { enable_thinking: null } } }
",
        ),
    ]);
    let plain = load(&dir, "plain.yaml").unwrap().config;
    let inc = load(&dir, "inc.yaml").unwrap().config;
    // a plugin's config is a hash map: compared as one, not as text
    assert_eq!(plain.plugins["p"].config, inc.plugins["p"].config);
    assert_eq!(plain.inference["judge"].extra, inc.inference["judge"].extra);
    let extra = &inc.inference["judge"].extra;
    assert_eq!(extra["response_format"], serde_json::Value::Null);
    assert_eq!(extra["chat_template_kwargs"]["x"], serde_json::Value::Null);
    let config = &inc.plugins["p"].config;
    assert_eq!(config["mode"], serde_json::Value::Null);
    assert_eq!(config["deep"]["off"], serde_json::Value::Null);

    // over a value a file it includes set: null, not the value gone
    let over = load(&dir, "over.yaml").unwrap().config;
    assert_eq!(over.plugins["p"].config["keep"], serde_json::Value::Null);
    assert_eq!(over.plugins["p"].config["mode"], serde_json::Value::Null);
    let kwargs = &over.inference["judge"].extra["chat_template_kwargs"];
    assert_eq!(kwargs["enable_thinking"], serde_json::Value::Null);
    assert_eq!(kwargs["x"], serde_json::Value::Null);
}

/// `x-` keys at the top are a file's own, for anchors: read without
/// `include:` as before, and not merged with another file's.
#[test]
fn an_x_key_holds_anchors_and_is_not_merged() {
    let file = "
x-prompt: &prompt judge.md
x-sink: &sink { type: file, path: out.ndjson }
inference:
  judge: { model: m, prompt_file: *prompt }
sinks:
  out: *sink
";
    let dir = tree(&[
        ("alone.yaml", file),
        ("other.yaml", "x-prompt: other.md\n"),
        (
            "root.yaml",
            "include: [alone.yaml, other.yaml]\nx-prompt: mine.md\n",
        ),
        ("typo.yaml", "x-prompt: p.md\nincludes: [alone.yaml]\n"),
    ]);
    let alone = load(&dir, "alone.yaml").unwrap().config;
    assert_eq!(
        alone.inference["judge"].prompt_file,
        PathBuf::from("judge.md")
    );
    assert!(alone.sinks.contains_key("out"));
    let root = load(&dir, "root.yaml").unwrap().config;
    assert_eq!(
        root.inference["judge"].prompt_file,
        canonical(&dir, "judge.md"),
        "alone.yaml's anchor, rebased as any path"
    );
    assert!(!yaml(&root).contains("x-prompt"));
    let err = error(&dir, "typo.yaml");
    assert!(err.contains("unknown top-level key `includes`"), "{err}");
}

#[test]
fn runtime_is_optional() {
    let dir = tree(&[(
        "pack.yaml",
        "plugins:\n  aws: { module_type: rust, path: . }\n",
    )]);
    let cfg = load(&dir, "pack.yaml").unwrap().config;
    assert_eq!(cfg.runtime.plugins_path, PathBuf::from("plugins/"));
    assert_eq!(cfg.runtime.batch_size, 256);
    assert!(cfg.runtime.workers >= 1);
}

#[test]
fn included_maps_merge_key_by_key() {
    let dir = tree(&[
        (
            "core.yaml",
            "
plugins:
  facts: { module_type: rust, path: facts }
inference:
  judge:
    model: m
    prompt_file: judge.md
    encoding: { type: logfmt, keep: [_norm.actor] }
",
        ),
        (
            "aws.yaml",
            "
plugins:
  aws: { module_type: rust, path: aws }
inference:
  judge:
    encoding:
      sources:
        cloudtrail: { principal: [_norm.actor], time: [eventTime] }
",
        ),
        ("root.yaml", "include: [core.yaml, aws.yaml]\n"),
    ]);
    let cfg = load(&dir, "root.yaml").unwrap().config;
    assert!(cfg.plugins.contains_key("facts"));
    assert!(cfg.plugins.contains_key("aws"));
    let text = yaml(&cfg);
    assert!(text.contains("cloudtrail"), "{text}");
    assert!(text.contains("_norm.actor"), "{text}");
}

/// Two packs that define the same plugin, each relative to itself, are one
/// node: one entry, so one instance per worker and its data read once.
#[test]
fn packs_that_define_the_same_plugin_make_one_node() {
    let pack = |name: &str| {
        format!(
            "
plugins:
  {name}: {{ module_type: rust, path: . }}
  enrich:
    module_type: rust
    path: ../../shared/enrich
    data: ../../shared/enrich/data
    config: {{ fields: [_norm.ip] }}
dag:
  - from: {{ kind: plugin, name: {name} }}
    to: [{{ kind: plugin, name: enrich }}]
  - from: {{ kind: plugin, name: enrich }}
    to: [{{ kind: sink, name: logs }}]
"
        )
    };
    let dir = tree(&[
        ("shared/enrich/Cargo.toml", ""),
        ("packs/aws/ingest.yaml", &pack("aws")),
        ("packs/okta/ingest.yaml", &pack("okta")),
        (
            "deploy/ingest.yaml",
            "
include: [../packs/aws/ingest.yaml, ../packs/okta/ingest.yaml]
sinks:
  logs: { type: file, path: logs.ndjson }
",
        ),
    ]);
    let cfg = load(&dir, "deploy/ingest.yaml").unwrap().config;
    let plugins: Vec<&str> = cfg.plugins.keys().map(|k| k.as_ref()).collect();
    assert_eq!(plugins, ["aws", "enrich", "okta"]);
    let enrich = &cfg.plugins["enrich"];
    assert_eq!(enrich.path, canonical(&dir, "shared/enrich"));
    assert_eq!(
        enrich.data.as_deref(),
        Some(canonical(&dir, "shared/enrich/data").as_path()),
        "a path that does not exist yet is joined all the same"
    );
    let from_enrich: Vec<&NodeRef> = cfg
        .dag
        .iter()
        .filter(|e| e.from == plugin("enrich"))
        .flat_map(|e| &e.to)
        .collect();
    assert_eq!(
        from_enrich,
        [&sink("logs")],
        "the edge both packs state, once"
    );
}

#[test]
fn siblings_that_disagree_are_an_error_naming_both() {
    let dir = tree(&[
        (
            "a.yaml",
            "plugins:\n  enrich: { module_type: rust, path: e, config: { fields: [_norm.ip] } }\n",
        ),
        (
            "b.yaml",
            "plugins:\n  enrich: { module_type: rust, path: e, config: { fields: [ip] } }\n",
        ),
        ("root.yaml", "include: [a.yaml, b.yaml]\n"),
    ]);
    let err = error(&dir, "root.yaml");
    assert!(err.contains("plugins.enrich.config.fields"), "{err}");
    assert!(err.contains("a.yaml has [_norm.ip]"), "{err}");
    assert!(err.contains("b.yaml has [ip]"), "{err}");
}

#[test]
fn the_including_file_wins() {
    let dir = tree(&[
        (
            "core.yaml",
            "
runtime: { workers: 2 }
plugins:
  enrich: { module_type: rust, path: e, config: { fields: [_norm.ip], ours: [10.0.0.0/8] } }
",
        ),
        (
            "root.yaml",
            "
include: [core.yaml]
runtime: { workers: 1 }
plugins:
  enrich: { config: { ours: [203.0.113.0/24] } }
",
        ),
    ]);
    let cfg = load(&dir, "root.yaml").unwrap().config;
    assert_eq!(cfg.runtime.workers, 1);
    let config = &cfg.plugins["enrich"].config;
    assert_eq!(
        config["ours"],
        serde_json::json!(["203.0.113.0/24"]),
        "a list is a value"
    );
    assert_eq!(config["fields"], serde_json::json!(["_norm.ip"]));
}

#[test]
fn null_removes_what_the_including_file_includes() {
    let dir = tree(&[
        (
            "core.yaml",
            "
plugins:
  facts: { module_type: rust, path: facts, data: profile, state_namespace: facts }
sinks:
  debug: { type: file, path: debug.ndjson }
  logs: { type: file, path: logs.ndjson }
",
        ),
        (
            "root.yaml",
            "
include: [core.yaml]
plugins:
  facts: { data: null, state_namespace: }
sinks:
  debug: null
",
        ),
    ]);
    let cfg = load(&dir, "root.yaml").unwrap().config;
    assert_eq!(cfg.plugins["facts"].data, None);
    assert_eq!(cfg.plugins["facts"].state_namespace, None);
    assert!(!cfg.sinks.contains_key("debug"));
    assert!(cfg.sinks.contains_key("logs"));
}

#[test]
fn a_null_in_an_included_file_is_not_set() {
    let dir = tree(&[
        (
            "core.yaml",
            "
inference:
  judge:
    url: ${TANGENT_TEST_UNSET_JUDGE_URL}
    model: m
    prompt_file: judge.md
    temperature:
",
        ),
        ("other.yaml", "inference:\n  judge: { temperature: 0.5 }\n"),
        ("root.yaml", "include: [core.yaml, other.yaml]\n"),
    ]);
    let cfg = load(&dir, "root.yaml").unwrap().config;
    let judge = &cfg.inference["judge"];
    assert_eq!(judge.url, "http://localhost:8080/v1", "the field's default");
    assert_eq!(
        judge.temperature,
        Some(0.5),
        "a sibling's value over a null"
    );
}

#[test]
fn dag_edges_add_up_and_count_once() {
    let dir = tree(&[
        (
            "a.yaml",
            "
dag:
  - from: { kind: plugin, name: facts }
    to: [{ kind: sink, name: logs }]
",
        ),
        (
            "b.yaml",
            "
dag:
  - from: { name: facts, kind: plugin }
    to: [{ kind: sink, name: logs }, { kind: sink, name: debug }]
  - from: { kind: plugin, name: facts }
    to: [{ kind: sink, name: logs }]
",
        ),
        (
            "root.yaml",
            "
include: [a.yaml, b.yaml]
plugins:
  facts: { module_type: rust, path: facts }
sinks:
  logs: { type: file, path: logs.ndjson }
  debug: { type: file, path: debug.ndjson }
dag:
  - from: { kind: plugin, name: facts }
    to: [{ kind: sink, name: alerts }]
",
        ),
    ]);
    let cfg = load(&dir, "root.yaml").unwrap().config;
    let edges: Vec<(NodeRef, Vec<NodeRef>)> = cfg
        .dag
        .iter()
        .map(|e| (e.from.clone(), e.to.clone()))
        .collect();
    assert_eq!(
        edges,
        vec![
            (plugin("facts"), vec![sink("logs")]),
            (plugin("facts"), vec![sink("debug")]),
            (plugin("facts"), vec![sink("alerts")]),
        ],
        "in include order, the root's last; each target once"
    );
}

/// `core` is included by both packs. It is read once, and what each pack
/// changes in it holds whichever pack comes first.
#[test]
fn a_diamond_is_read_once_and_merges_the_same_either_way() {
    let dir = tree(&[
        (
            "core.yaml",
            "
runtime: { workers: 2, batch_age: 5 }
plugins:
  facts: { module_type: rust, path: facts, data: profile }
dag:
  - from: { kind: plugin, name: facts }
    to: [{ kind: sink, name: logs }]
",
        ),
        ("a.yaml", "include: [core.yaml]\nruntime: { workers: 4 }\n"),
        (
            "b.yaml",
            "include: [core.yaml]\nplugins:\n  facts: { data: null }\nruntime: { batch_size: 64 }\n",
        ),
    ]);
    let mut merged = Vec::new();
    for order in ["[a.yaml, b.yaml]", "[b.yaml, a.yaml]"] {
        fs::write(dir.path().join("root.yaml"), format!("include: {order}\n")).unwrap();
        let loaded = load(&dir, "root.yaml").unwrap();
        let cfg = &loaded.config;
        assert_eq!(cfg.runtime.workers, 4, "a's change to core");
        assert_eq!(cfg.runtime.batch_size, 64);
        assert_eq!(cfg.runtime.batch_age, 5);
        assert_eq!(cfg.plugins["facts"].data, None, "b's removal from core");
        assert_eq!(cfg.dag.len(), 1, "core's edge once");
        let core = canonical(&dir, "core.yaml");
        assert_eq!(loaded.files.iter().filter(|f| **f == core).count(), 1);
        assert_eq!(loaded.files[0], core, "core first: both include it");
        assert_eq!(*loaded.files.last().unwrap(), canonical(&dir, "root.yaml"));
        merged.push(yaml(cfg));
    }
    assert_eq!(merged[0], merged[1]);
}

#[test]
fn a_diamond_whose_sides_disagree_is_an_error() {
    let dir = tree(&[
        ("core.yaml", "runtime: { workers: 2 }\n"),
        ("a.yaml", "include: [core.yaml]\nruntime: { workers: 4 }\n"),
        ("b.yaml", "include: [core.yaml]\nruntime: { workers: 8 }\n"),
        ("root.yaml", "include: [a.yaml, b.yaml]\n"),
    ]);
    let err = error(&dir, "root.yaml");
    assert!(err.contains("runtime.workers"), "{err}");
    assert!(err.contains("a.yaml has 4"), "{err}");
    assert!(err.contains("b.yaml has 8"), "{err}");

    // and the root settles it
    fs::write(
        dir.path().join("root.yaml"),
        "include: [a.yaml, b.yaml]\nruntime: { workers: 1 }\n",
    )
    .unwrap();
    assert_eq!(load(&dir, "root.yaml").unwrap().config.runtime.workers, 1);
}

/// b removes core's `workers`; a, which b does not include, sets it too,
/// and that b's null does not touch.
#[test]
fn a_null_removes_only_what_its_file_includes() {
    let dir = tree(&[
        ("core.yaml", "runtime: { workers: 2, batch_age: 7 }\n"),
        ("a.yaml", "runtime: { workers: 3 }\n"),
        (
            "b.yaml",
            "include: [core.yaml]\nruntime: { workers: null, batch_age: null }\n",
        ),
        ("root.yaml", "include: [core.yaml, a.yaml, b.yaml]\n"),
    ]);
    let cfg = load(&dir, "root.yaml").unwrap().config;
    assert_eq!(cfg.runtime.workers, 3);
    assert_eq!(cfg.runtime.batch_age, 5, "removed: the default");
}

#[test]
fn a_map_against_a_value_is_an_error() {
    let dir = tree(&[
        (
            "a.yaml",
            "plugins:\n  enrich: { module_type: rust, path: e, config: { fields: [ip] } }\n",
        ),
        ("b.yaml", "plugins:\n  enrich: { config: none }\n"),
        ("root.yaml", "include: [a.yaml, b.yaml]\n"),
    ]);
    let err = error(&dir, "root.yaml");
    assert!(
        err.contains("no file that includes them both settles it"),
        "{err}"
    );
    assert!(err.contains("plugins.enrich.config: "), "{err}");
    assert!(err.contains("a.yaml has {fields: [ip]}, "), "{err}");
    assert!(err.contains("b.yaml has none"), "{err}");
}

#[test]
fn a_map_against_a_value_is_settled_by_a_file_that_includes_both() {
    let judge = "inference:\n  judge: { model: m, prompt_file: judge.md }\n";
    for (b, c, settles, want) in [
        ("{type: logfmt}", "ndjson", "{type: ndjson}", "type: ndjson"),
        ("ndjson", "{type: ndjson}", "{type: logfmt}", "type: logfmt"),
    ] {
        let dir = tree(&[
            ("judge.yaml", judge),
            ("b.yaml", &format!("inference:\n  judge:\n    encoding: {b}\n")),
            ("c.yaml", &format!("inference:\n  judge:\n    encoding: {c}\n")),
            (
                "a.yaml",
                &format!(
                    "include: [judge.yaml, b.yaml, c.yaml]\ninference:\n  judge:\n    encoding: {settles}\n"
                ),
            ),
            (
                "unsettled.yaml",
                "include: [judge.yaml, b.yaml, c.yaml]\n",
            ),
        ]);
        let cfg = load(&dir, "a.yaml").unwrap().config;
        let text = yaml(&cfg);
        assert!(text.contains(want), "{text}");
        let err = error(&dir, "unsettled.yaml");
        assert!(err.contains("inference.judge.encoding: "), "{err}");
        assert!(err.contains(&format!("b.yaml has {b}, ")), "{err}");
        assert!(err.contains(&format!("c.yaml has {c}")), "{err}");
    }
}

#[test]
fn a_cycle_is_an_error_naming_it() {
    let dir = tree(&[
        ("a.yaml", "include: [b.yaml]\n"),
        ("b.yaml", "include: [a.yaml]\n"),
    ]);
    let err = error(&dir, "a.yaml");
    assert!(err.contains("include cycle"), "{err}");
    assert!(err.contains("a.yaml -> "), "{err}");
    assert!(err.contains("b.yaml -> "), "{err}");
}

#[test]
fn a_missing_include_names_the_file_that_includes_it() {
    let dir = tree(&[("root.yaml", "include: [packs/oktaa/ingest.yaml]\n")]);
    let err = error(&dir, "root.yaml");
    assert!(
        err.contains("root.yaml includes packs/oktaa/ingest.yaml"),
        "{err}"
    );
}

#[test]
fn env_works_in_include_paths_with_defaults() {
    std::env::set_var("TANGENT_TEST_PACKS", "packs");
    std::env::set_var("TANGENT_TEST_EMPTY", "");
    let dir = tree(&[
        (
            "packs/aws.yaml",
            "plugins:\n  aws: { module_type: rust, path: . }\n",
        ),
        (
            "packs/okta.yaml",
            "plugins:\n  okta: { module_type: rust, path: . }\n",
        ),
        (
            "root.yaml",
            "
include:
  - ${TANGENT_TEST_PACKS}/aws.yaml
  - ${TANGENT_TEST_UNSET_DIR:-packs}/okta.yaml
inference:
  judge:
    model: ${TANGENT_TEST_EMPTY:-glm-4.7-flash}
    url: ${TANGENT_TEST_PACKS:-unused}
    prompt_file: judge.md
",
        ),
    ]);
    let cfg = load(&dir, "root.yaml").unwrap().config;
    assert!(cfg.plugins.contains_key("aws"));
    assert!(cfg.plugins.contains_key("okta"));
    assert_eq!(
        cfg.inference["judge"].model, "glm-4.7-flash",
        "empty is unset"
    );
    assert_eq!(cfg.inference["judge"].url, "packs", "a set variable wins");
}

#[test]
fn an_unset_model_with_a_default_loads_without_include_too() {
    let dir = tree(&[(
        "judge.yaml",
        "inference:\n  judge: { model: \"${TANGENT_TEST_UNSET_MODEL:-glm-4.7-flash}\", prompt_file: judge.md }\n",
    )]);
    let cfg = load(&dir, "judge.yaml").unwrap().config;
    assert_eq!(cfg.inference["judge"].model, "glm-4.7-flash");
}

/// Component paths are relative to the file that names them; `runtime`
/// paths to the root config, and source and sink paths to the working
/// directory, as before.
#[test]
fn paths_are_relative_to_the_file_that_names_them() {
    let dir = tree(&[
        ("packs/core/facts/Cargo.toml", ""),
        ("packs/core/judge.md", "judge"),
        ("packs/core/tests/input.json", "[]"),
        (
            "packs/core/ingest.yaml",
            "
runtime:
  plugins_path: plugins/
  state: { path: state.sqlite }
plugins:
  facts:
    module_type: rust
    path: facts
    data: ./profile/../profile
    tests:
      - { input: tests/input.json, expected: tests/expected.json }
inference:
  judge: { model: m, prompt_file: judge.md, schema_file: ../core/judge.schema.json }
sinks:
  logs: { type: file, path: logs.ndjson }
sources:
  in: { type: file, path: in.ndjson, decoding: { format: { type: ndjson } } }
",
        ),
        (
            "deploy/ingest.yaml",
            "
include: [../packs/core/ingest.yaml]
plugins:
  facts: { data: profile }
",
        ),
    ]);
    let cfg = load(&dir, "deploy/ingest.yaml").unwrap().config;
    let facts = &cfg.plugins["facts"];
    assert_eq!(facts.path, canonical(&dir, "packs/core/facts"));
    assert_eq!(
        facts.data,
        Some(PathBuf::from("profile")),
        "the root's own override, as written: read against the root's directory"
    );
    assert_eq!(
        facts.tests[0].input,
        canonical(&dir, "packs/core/tests/input.json")
    );
    assert_eq!(
        facts.tests[0].expected,
        canonical(&dir, "packs/core/tests/expected.json")
    );
    let judge = &cfg.inference["judge"];
    assert_eq!(judge.prompt_file, canonical(&dir, "packs/core/judge.md"));
    assert_eq!(
        judge.schema_file.as_deref(),
        Some(canonical(&dir, "packs/core/judge.schema.json").as_path())
    );
    assert_eq!(cfg.runtime.plugins_path, PathBuf::from("plugins/"));
    assert_eq!(cfg.runtime.state.path, PathBuf::from("state.sqlite"));
    let text = yaml(&cfg);
    assert!(text.contains("path: logs.ndjson"), "{text}");
    assert!(text.contains("path: in.ndjson"), "{text}");

    // An included file's override of an included path is its own.
    fs::write(
        dir.path().join("packs/core/data.yaml"),
        "include: [ingest.yaml]\nplugins:\n  facts: { data: profile }\n",
    )
    .unwrap();
    fs::write(
        dir.path().join("deploy/ingest.yaml"),
        "include: [../packs/core/data.yaml]\n",
    )
    .unwrap();
    let cfg = load(&dir, "deploy/ingest.yaml").unwrap().config;
    assert_eq!(
        cfg.plugins["facts"].data,
        Some(canonical(&dir, "packs/core/profile"))
    );
}

#[test]
fn an_unknown_top_level_key_is_an_error_in_any_file() {
    let dir = tree(&[
        ("typo.yaml", "includes: [core.yaml]\n"),
        (
            "pack.yaml",
            "plugin:\n  aws: { module_type: rust, path: . }\n",
        ),
        ("root.yaml", "include: [pack.yaml]\n"),
    ]);
    let err = error(&dir, "typo.yaml");
    assert!(err.contains("unknown top-level key `includes`"), "{err}");
    let err = error(&dir, "root.yaml");
    assert!(
        err.contains("pack.yaml: unknown top-level key `plugin`"),
        "{err}"
    );
}

#[test]
fn an_unknown_key_in_a_block_is_an_error() {
    for (text, key) in [
        (
            "plugins:\n  facts: { module_type: rust, path: facts, dta: profile }\n",
            "dta",
        ),
        (
            "inference:\n  judge: { model: m, prompt_file: p, max_token: 10 }\n",
            "max_token",
        ),
        ("runtime: { worker: 2 }\n", "worker"),
        ("runtime: { state: { pth: s.sqlite } }\n", "pth"),
        (
            "sources:\n  events: { type: tcp, bind_adress: 0.0.0.0:9400 }\n",
            "bind_adress",
        ),
        (
            "sinks:\n  logs: { type: sqlite, path: l.sqlite, table: logs, partition: { org: [o] } }\n",
            "partition",
        ),
        (
            "sinks:\n  out: { type: file, path: o.ndjson, compresion: { type: none } }\n",
            "compresion",
        ),
        (
            "enrichment_tables:\n  geo: { type: mmdb, path: g.mmdb, refresh: 1d }\n",
            "refresh",
        ),
    ] {
        let dir = tree(&[("tangent.yaml", text)]);
        let err = error(&dir, "tangent.yaml");
        assert!(err.contains(&format!("unknown field `{key}`")), "{err}");
    }
}

/// Enrichment tables merge like any map: two packs that name the same file,
/// each relative to itself, are one table; the root's own path stays as
/// written, for the runtime to read against the root's directory.
#[test]
fn enrichment_tables_merge_and_their_paths_are_the_naming_file_s() {
    let core = "
enrichment_tables:
  geo: { type: mmdb, path: ../../data/GeoLite2-Country.mmdb }
  networks: { type: cidr, path: ../../data/networks.csv }
";
    let dir = tree(&[
        ("data/networks.csv", "203.0.113.0/24,aws\n"),
        ("packs/core/ingest.yaml", core),
        ("packs/aws/ingest.yaml", "include: [../core/ingest.yaml]\n"),
        (
            "packs/okta/ingest.yaml",
            "enrichment_tables:\n  geo: { type: mmdb, path: ../../data/./GeoLite2-Country.mmdb }\n",
        ),
        (
            "deploy/ingest.yaml",
            "
include: [../packs/aws/ingest.yaml, ../packs/okta/ingest.yaml]
enrichment_tables:
  profile: { type: document, path: profile/profile.yaml, required: true }
",
        ),
    ]);
    let cfg = load(&dir, "deploy/ingest.yaml").unwrap().config;
    let names: Vec<&str> = cfg.enrichment_tables.keys().map(|k| k.as_ref()).collect();
    assert_eq!(names, ["geo", "networks", "profile"]);
    assert_eq!(
        cfg.enrichment_tables["geo"],
        TableConfig {
            kind: TableKind::Mmdb,
            path: canonical(&dir, "data/GeoLite2-Country.mmdb"),
            required: false,
        },
        "a file setup has yet to fetch, joined all the same"
    );
    assert_eq!(
        cfg.enrichment_tables["networks"].path,
        canonical(&dir, "data/networks.csv")
    );
    assert_eq!(
        cfg.enrichment_tables["profile"],
        TableConfig {
            kind: TableKind::Document,
            path: PathBuf::from("profile/profile.yaml"),
            required: true,
        }
    );
    let text = yaml(&cfg);
    assert!(text.contains("enrichment_tables:"), "{text}");

    // Two packs that name different files under one table's name.
    fs::write(
        dir.path().join("packs/okta/ingest.yaml"),
        "enrichment_tables:\n  geo: { type: mmdb, path: ../../data/GeoLite2-City.mmdb }\n",
    )
    .unwrap();
    let err = error(&dir, "deploy/ingest.yaml");
    assert!(err.contains("enrichment_tables.geo.path"), "{err}");
    assert!(err.contains("packs/core/ingest.yaml"), "{err}");
    assert!(err.contains("packs/okta/ingest.yaml"), "{err}");

    // The root settles it, and a config without tables writes none.
    fs::write(
        dir.path().join("deploy/ingest.yaml"),
        "
include: [../packs/aws/ingest.yaml, ../packs/okta/ingest.yaml]
enrichment_tables:
  geo: { path: city.mmdb }
",
    )
    .unwrap();
    let cfg = load(&dir, "deploy/ingest.yaml").unwrap().config;
    assert_eq!(
        cfg.enrichment_tables["geo"].path,
        PathBuf::from("city.mmdb")
    );
    let bare = tree(&[("tangent.yaml", "dag: []\n")]);
    let text = yaml(&load(&bare, "tangent.yaml").unwrap().config);
    assert!(!text.contains("enrichment_tables"), "{text}");
}

/// A pack's fixture names the tables it was written against, each relative to
/// the pack's file; the config's tables of other names stay.
#[test]
fn a_test_s_own_tables_are_relative_to_the_file_that_names_them() {
    let pack = "
plugins:
  aws:
    module_type: rust
    path: .
    tests:
      - input: tests/input.json
        expected: tests/expected.json
        enrichment_tables:
          profile: { type: document, path: tests/profile.yaml }
";
    let dir = tree(&[
        ("packs/aws/ingest.yaml", pack),
        (
            "deploy/ingest.yaml",
            "
include: [../packs/aws/ingest.yaml]
enrichment_tables:
  profile: { type: document, path: profile/profile.yaml }
",
        ),
    ]);
    let cfg = load(&dir, "deploy/ingest.yaml").unwrap().config;
    let test = &cfg.plugins["aws"].tests[0];
    assert_eq!(
        test.enrichment_tables["profile"],
        TableConfig {
            kind: TableKind::Document,
            path: canonical(&dir, "packs/aws/tests/profile.yaml"),
            required: false,
        }
    );
    assert_eq!(
        cfg.enrichment_tables["profile"].path,
        PathBuf::from("profile/profile.yaml"),
        "the config's own, as written"
    );
    let text = yaml(&cfg);
    assert_eq!(text.matches("enrichment_tables:").count(), 2, "{text}");
}

#[test]
fn an_enrichment_table_of_an_unknown_type_is_an_error() {
    let dir = tree(&[(
        "tangent.yaml",
        "enrichment_tables:\n  geo: { type: geoip, path: g.mmdb }\n",
    )]);
    let err = error(&dir, "tangent.yaml");
    assert!(err.contains("unknown variant `geoip`"), "{err}");
    assert!(
        err.contains("expected one of `mmdb`, `cidr`, `document`"),
        "{err}"
    );
}

#[test]
fn a_required_table_is_checked_against_the_config_s_directory() {
    let dir = tree(&[
        ("deploy/profile/profile.yaml", "org: acme\n"),
        (
            "deploy/ingest.yaml",
            "
enrichment_tables:
  profile: { type: document, path: profile/profile.yaml, required: true }
  typo: { type: document, path: profle/profile.yaml, required: true }
  geo: { type: mmdb, path: data/GeoLite2-Country.mmdb }
",
        ),
    ]);
    let cfg = load(&dir, "deploy/ingest.yaml").unwrap().config;
    let at = dir.path().join("deploy");
    let t = &cfg.enrichment_tables;
    assert!(t["profile"].required && !t["geo"].required);
    t["profile"].check("profile", &at).unwrap();
    t["geo"].check("geo", &at).unwrap();
    let err = format!("{:#}", t["typo"].check("typo", &at).unwrap_err());
    assert!(err.contains("enrichment table typo: "), "{err}");
    assert!(
        err.contains("deploy/profle/profile.yaml not found"),
        "{err}"
    );
}

/// `deny_unknown_fields` on the blocks inside a `type`-tagged enum: the tag
/// is the enum's, and every kind still loads with it.
#[test]
fn every_source_and_sink_type_loads_with_its_tag() {
    let dir = tree(&[(
        "tangent.yaml",
        "
sources:
  a: { type: msk, bootstrap_servers: b, topic: t, auth: { mode: scram, username: u, password: p }, decoding: { format: { type: ndjson } } }
  b: { type: file, path: in.ndjson, decoding: { format: { type: json-array }, compression: { type: none } } }
  c: { type: socket, socket_path: /tmp/s.sock }
  d: { type: tcp, bind_address: 0.0.0.0:9400, read_buffer_size: 1024 }
  e: { type: sqs, queue_url: q, decoding: { format: { type: ndjson } } }
  f: { type: github_webhook, path: /hook, token: t }
  g: { type: npm_registry, packages: [left-pad] }
  h: { type: cloudtrail, region: us-east-1, queue_url: q }
  i: { type: github_audit, org: acme, token: t }
  j: { type: okta, org_url: https://example.okta.com, token: t }
  k: { type: google_workspace, credentials_file: c.json, subject: admin@example.com }
  l: { type: sqlite, path: logs.sqlite, table: logs, partition: actor, batch_size: 40, timeout_secs: 3600 }
  m: { type: github_audit, org: , since: 7d }
sinks:
  s3: { type: s3, bucket_name: b, region: us-east-1, compression: { type: gzip, level: 6 } }
  file: { type: file, path: out.ndjson, compression: { type: none }, encoding: { type: ndjson }, in_flight_limit: 1, default: true }
  hole: { type: blackhole }
  logs: { type: sqlite, path: logs.sqlite, table: logs, timestamp_key: ts, partitions: { org: [_norm.org] } }
  slack: { type: slack, webhook_url: https://hooks.example.com/x }
",
    )]);
    let cfg = match load(&dir, "tangent.yaml") {
        Ok(l) => l.config,
        Err(e) => panic!("{e:#}"),
    };
    assert_eq!(cfg.sources.len(), 13);
    assert_eq!(cfg.sinks.len(), 5);
    let SourceConfig::GithubAudit(idle) = &cfg.sources["m"] else {
        panic!("github_audit");
    };
    assert_eq!(
        idle.org, None,
        "an empty org: the source stays idle, as before"
    );
    assert_eq!(idle.since, "7d");
}

/// A config tangent wrote reads back: `plugin test` writes one and runs it.
#[test]
fn a_config_tangent_wrote_reads_back() {
    let dir = tree(&[(
        "tangent.yaml",
        "
sources:
  input: { type: file, path: in.json, decoding: { format: { type: json-array }, compression: { type: none } } }
  events: { type: tcp, bind_address: 0.0.0.0:9400 }
plugins:
  probe: { module_type: rust, path: probe, data: profile, config: { fields: [ip] }, tests: [{ input: i.json, expected: e.json }] }
sinks:
  out: { type: file, path: out.ndjson, compression: { type: none }, in_flight_limit: 1, default: true }
  logs: { type: sqlite, path: logs.sqlite, table: logs, partitions: { org: [_norm.org] } }
dag:
  - from: { kind: source, name: input }
    to: [{ kind: plugin, name: probe }]
  - from: { kind: plugin, name: probe }
    to: [{ kind: sink, name: out }, { kind: sink, name: logs, key_prefix: p }]
",
    )]);
    let cfg = load(&dir, "tangent.yaml").unwrap().config;
    let text = yaml(&cfg);
    fs::write(dir.path().join("again.yaml"), &text).unwrap();
    let again = match load(&dir, "again.yaml") {
        Ok(l) => l.config,
        Err(e) => panic!("{e:#}\n{text}"),
    };
    assert_eq!(yaml(&again), text);
}

#[test]
fn a_type_error_names_the_files_that_set_the_block() {
    let dir = tree(&[
        ("okta.yaml", "plugins:\n  okta: { path: . }\n"),
        ("root.yaml", "include: [okta.yaml]\n"),
    ]);
    let err = error(&dir, "root.yaml");
    assert!(err.contains("plugins.okta"), "{err}");
    assert!(err.contains("missing field `module_type`"), "{err}");
    assert!(err.contains("(set in "), "{err}");
    assert!(err.contains("okta.yaml)"), "{err}");
    assert!(
        !err.contains(" at line "),
        "no line of a text no one wrote: {err}"
    );
}

#[test]
fn a_bad_edge_names_its_file() {
    let dir = tree(&[
        (
            "pack.yaml",
            "dag:\n  - from: { kind: plugn, name: aws }\n    to: []\n",
        ),
        ("root.yaml", "include: [pack.yaml]\n"),
    ]);
    let err = error(&dir, "root.yaml");
    assert!(err.contains("dag"), "{err}");
    assert!(err.contains("pack.yaml)"), "{err}");
}

/// Every field of the blocks that may name a file, by name. A new field
/// fails to compile here: if it names a file relative to the config, add it
/// to `include::PATHS` too.
#[test]
fn every_path_field_is_rebased() {
    let PluginConfig {
        module_type: _,
        path: _,  // PATHS
        tests: _, // PATHS: input, expected
        config: _,
        state_namespace: _,
        data: _, // PATHS
    } = PluginConfig::default();
    let PluginTests {
        input: _,             // PATHS
        expected: _,          // PATHS
        enrichment_tables: _, // PATHS: each table's path, as TableConfig below
    } = PluginTests {
        input: PathBuf::new(),
        expected: PathBuf::new(),
        enrichment_tables: Default::default(),
    };
    let judge: InferenceConfig = serde_yaml::from_str("{ model: m, prompt_file: p }").unwrap();
    let InferenceConfig {
        backend: _,
        url: _,
        api_key: _,
        model: _,
        aws: _,
        prompt_file: _, // PATHS
        schema_file: _, // PATHS
        fields: _,
        batch_bytes: _,
        encoding: _,
        group_by: _,
        concurrency: _,
        timeout_ms: _,
        retries: _,
        max_tokens: _,
        temperature: _,
        extra: _,
    } = judge;
    let TableConfig {
        kind: _,
        path: _, // PATHS
        required: _,
    } = TableConfig {
        kind: TableKind::Cidr,
        path: PathBuf::new(),
        required: false,
    };
    let paths: Vec<String> = tangent_shared::include::PATHS
        .iter()
        .map(|p| p.join("."))
        .collect();
    assert_eq!(
        paths,
        [
            "plugins.*.path",
            "plugins.*.data",
            "plugins.*.tests.[].input",
            "plugins.*.tests.[].expected",
            "plugins.*.tests.[].enrichment_tables.*.path",
            "inference.*.prompt_file",
            "inference.*.schema_file",
            "enrichment_tables.*.path",
        ]
    );
}

/// Run from the directory above the config (`--config sub/ingest.yaml`):
/// the included file's paths are its own, not joined twice. `cargo test`
/// runs this from crates/shared.
#[test]
fn paths_resolve_from_another_working_directory() {
    let root = Path::new("tests/include/rebase/deploy/ingest.yaml");
    assert!(
        root.is_relative() && root.exists(),
        "run from crates/shared"
    );
    let cfg = Config::load(root).unwrap().config;
    let here = Path::new("tests/include/rebase").canonicalize().unwrap();
    let probe = &cfg.plugins["probe"];
    assert_eq!(probe.path, here.join("pack/probe"));
    assert_eq!(probe.tests[0].input, here.join("pack/tests/input.json"));
    assert_eq!(
        probe.data,
        Some(PathBuf::from("profile")),
        "the root's, as written"
    );
}
