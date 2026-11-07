//! Every config in the repo loads: alone, or through a config that includes
//! it (a pack's ingest.yaml, say, is a plugin whose tables enrich gives). And
//! the pipelines pass the checks `tangent run` makes before it starts: with
//! the judge, as the examples run, and without it.

use std::collections::BTreeSet;
use std::fs;
use std::path::{Path, PathBuf};

use serde_yaml::Value;
use tangent_shared::include::TOP_LEVEL;
use tangent_shared::Config;

/// The repo's YAML files whose top is a config's: a map of config keys only,
/// or one with a key only a config has (so one with a typo is still found).
/// Cases and profiles are YAML too, but not configs: a case has `sources`.
fn configs(dir: &Path, out: &mut Vec<PathBuf>) {
    let Ok(entries) = fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let path = entry.path();
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.starts_with('.') || ["target", "node_modules", "plugins"].contains(&name.as_str()) {
            continue;
        }
        if path.is_dir() {
            configs(&path, out);
            continue;
        }
        if !(name.ends_with(".yaml") || name.ends_with(".yml")) {
            continue;
        }
        let Ok(text) = fs::read_to_string(&path) else {
            continue;
        };
        let Ok(Value::Mapping(map)) = serde_yaml::from_str::<Value>(&text) else {
            continue;
        };
        let keys: Vec<&str> = map.keys().filter_map(Value::as_str).collect();
        let only_config = !keys.is_empty() && keys.iter().all(|k| TOP_LEVEL.contains(k));
        let config_only = keys
            .iter()
            .any(|k| ["runtime", "sinks", "plugins", "inference", "dag", "include"].contains(k));
        if only_config || config_only {
            out.push(path);
        }
    }
}

#[test]
fn every_config_in_the_repo_loads() {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let repo = repo.canonicalize().unwrap();
    let mut found = Vec::new();
    for top in ["examples", "packs", "crates", "docker", "scripts"] {
        configs(&repo.join(top), &mut found);
    }
    found.sort();
    assert!(
        found
            .iter()
            .any(|p| p.ends_with("examples/counter/tangent.yaml")),
        "found the examples: {found:?}"
    );

    let mut included: BTreeSet<PathBuf> = BTreeSet::new();
    let mut failed: Vec<(PathBuf, String)> = Vec::new();
    for path in &found {
        match Config::load(path) {
            Ok(loaded) => {
                for f in loaded.files {
                    included.insert(f.canonicalize().unwrap_or(f));
                }
            }
            Err(e) => failed.push((path.clone(), format!("{e:#}"))),
        }
    }
    let failed: Vec<String> = failed
        .into_iter()
        .filter(|(p, _)| !included.contains(&p.canonicalize().unwrap()))
        .map(|(p, e)| format!("{}: {e}", p.strip_prefix(&repo).unwrap_or(&p).display()))
        .collect();
    assert!(
        failed.is_empty(),
        "configs that load neither alone nor through another:\n{}",
        failed.join("\n")
    );
    eprintln!("{} configs load", found.len());
}

/// Every edge's nodes exist, which `tangent run` checks before it starts and
/// loading does not: in examples/cloud-infra, and in a pipeline of every pack
/// in packs/ without the judge, so no pack names a node of the judge's.
#[test]
fn pipelines_validate_with_and_without_the_judge() {
    let repo = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap();
    let example = "examples/cloud-infra/tangent.yaml";
    let cfg = Config::load(&repo.join(example)).unwrap().config;
    cfg.validate()
        .unwrap_or_else(|e| panic!("{example}: {e:#}"));

    // Every pack, as packs/Makefile finds them: a directory with an
    // ingest.yaml and its plugin's Cargo.toml, so not enrich, behavior-baseline
    // or judge. Its plugins are the ones its file defines.
    let mut files: Vec<PathBuf> = fs::read_dir(repo.join("packs"))
        .unwrap()
        .flatten()
        .map(|e| e.path().join("ingest.yaml"))
        .filter(|f| f.is_file() && f.with_file_name("Cargo.toml").is_file())
        .collect();
    files.sort();
    let mut plugins: Vec<String> = Vec::new();
    for f in &files {
        let pack: Value = serde_yaml::from_str(&fs::read_to_string(f).unwrap()).unwrap();
        let Some(Value::Mapping(defined)) = pack.get("plugins") else {
            panic!("{} defines no plugin", f.display());
        };
        plugins.extend(defined.keys().filter_map(Value::as_str).map(String::from));
    }
    assert!(
        ["aws", "github", "google_workspace", "okta"]
            .iter()
            .all(|p| plugins.iter().any(|q| q == p)),
        "found the packs: {plugins:?}"
    );

    let mut yaml = format!(
        "include:\n  - {}\n",
        repo.join("packs/enrich/ingest.yaml").display()
    );
    for f in &files {
        yaml += &format!("  - {}\n", f.display());
    }
    yaml += "sources:\n  events: { type: tcp, bind_address: 127.0.0.1:0 }\n";
    yaml += "sinks:\n  lake: { type: file, path: lake.ndjson }\ndag:\n";
    for p in &plugins {
        yaml += &format!("  - from: events\n    to: [{p}]\n");
        yaml += &format!("  - from: {p}\n    to: [lake]\n");
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tangent.yaml");
    fs::write(&path, yaml).unwrap();
    let cfg = Config::load(&path).unwrap().config;
    cfg.validate()
        .unwrap_or_else(|e| panic!("the packs without the judge: {e:#}"));
}
