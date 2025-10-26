use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::str::FromStr;

use anyhow::{bail, Context, Result};
use tangent_shared::dag::{Edge, NodeRef};
use tangent_shared::plugins::PluginConfig;
use tangent_shared::runtime::RuntimeConfig;
use tangent_shared::sinks::common::{CommonSinkOptions, Compression, Encoding};
use tangent_shared::Config;
use tracing::{info, warn};

use serde_json::{Map, Value};
use tangent_runtime::RuntimeOptions;
use tangent_shared::sinks::{
    common::{SinkConfig, SinkKind},
    file as fileSink,
};
use tangent_shared::sources::common::{Decoding, SourceConfig};
use tangent_shared::sources::file;

#[derive(Debug)]
pub struct TestOptions {
    pub input: PathBuf,
    pub expected: PathBuf,
    pub plugin: PathBuf,
    pub config_path: PathBuf,
}

pub async fn run(opts: TestOptions) -> Result<()> {
    let input = opts.input.canonicalize().unwrap_or(opts.input);
    let expected = opts.expected;
    let cfg = Config::from_file(&opts.config_path)?;

    let mut rt = RuntimeOptions::default();
    rt.once = true;

    let input_source = SourceConfig::File(file::FileConfig {
        path: input,
        decoding: Decoding::default(),
    });

    let out_file = PathBuf::from_str("test_out.json")?;
    if out_file.exists() {
        fs::remove_file(out_file.clone())?;
    }

    let file_sink = SinkConfig {
        kind: SinkKind::File(fileSink::FileConfig {
            path: out_file.clone(),
        }),
        common: CommonSinkOptions {
            compression: Compression::None,
            encoding: Encoding::NDJSON,
            object_max_bytes: tangent_shared::sinks::common::object_max_bytes(),
            in_flight_limit: tangent_shared::sinks::common::in_flight_limit(),
            default: true,
        },
    };

    let plugin_config = PluginConfig {
        module_type: "".to_string(), // not used
        path: opts.plugin,
    };

    let runtime = RuntimeConfig {
        plugins_path: cfg.runtime.plugins_path,
        batch_size: 1,
        batch_age: 1,
        workers: 1,
    };

    let entry = Edge {
        from: NodeRef::Source {
            name: "input".into(),
        },
        to: vec![NodeRef::Plugin {
            name: "test_plugin".into(),
        }],
    };

    let exit = Edge {
        from: NodeRef::Plugin {
            name: "test_plugin".into(),
        },
        to: vec![NodeRef::Plugin { name: "out".into() }],
    };

    let mut sinks = BTreeMap::new();
    sinks.insert(String::from("out"), file_sink);

    let mut sources = BTreeMap::new();
    sources.insert(String::from("input"), input_source);

    let mut plugins = BTreeMap::new();
    plugins.insert(String::from("test_plugin"), plugin_config);

    let test_config = tangent_shared::Config {
        runtime,
        sources,
        sinks,
        plugins,
        dag: vec![entry, exit],
    };

    let yaml = serde_yaml::to_string(&test_config)?;
    fs::write(".test.yaml", yaml)?;

    let cfg_path: PathBuf = ".test.yaml".into();

    tangent_runtime::run(&cfg_path, rt).await?;

    let produced = read_json(&out_file).context("reading produced JSON")?;
    let expected = read_json(&expected)?;
    let diffs = diff_lines(&expected, &produced);

    if diffs.is_empty() {
        info!("✅ test passed: output matches expected");
        Ok(())
    } else {
        warn!("❌ test failed: output differs from expected\n{}", diffs);
        bail!("output differs from expected");
    }
}

fn read_json(path: &Path) -> Result<Value> {
    let f = fs::File::open(path).with_context(|| format!("open {}", path.display()))?;
    let reader = BufReader::new(f);

    let v: Value = serde_json::from_reader(reader)?;
    Ok(stabilize(v))
}

fn stabilize(v: Value) -> Value {
    match v {
        Value::Object(m) => {
            let mut items: Vec<(String, Value)> =
                m.into_iter().map(|(k, v)| (k, stabilize(v))).collect();
            items.sort_by(|a, b| a.0.cmp(&b.0));
            let mut new = Map::with_capacity(items.len());
            for (k, v) in items {
                new.insert(k, v);
            }
            Value::Object(new)
        }
        Value::Array(a) => Value::Array(a.into_iter().map(stabilize).collect()),
        x => x,
    }
}

pub fn diff_lines(expected: &Value, produced: &Value) -> String {
    if expected == produced {
        return String::new();
    }

    let mut exp_norm = expected.clone();
    let mut pro_norm = produced.clone();
    normalize_embedded_json(&mut exp_norm);
    normalize_embedded_json(&mut pro_norm);

    let exp = serde_json::to_string_pretty(&exp_norm).unwrap_or_else(|_| exp_norm.to_string());
    let pro = serde_json::to_string_pretty(&pro_norm).unwrap_or_else(|_| pro_norm.to_string());

    let e: Vec<&str> = exp.lines().collect();
    let p: Vec<&str> = pro.lines().collect();

    let mut i = 0usize;
    let max = e.len().max(p.len());
    while i < max {
        let le = *e.get(i).unwrap_or(&"");
        let lp = *p.get(i).unwrap_or(&"");
        if le != lp {
            break;
        }
        i += 1;
    }

    if i == max {
        return String::new();
    }

    let mut s = String::new();
    let _ = writeln!(s, "first difference at line {}", i + 1);

    let win = 3usize;
    let start = i.saturating_sub(win);
    let end_e = (i + win).min(e.len());
    let end_p = (i + win).min(p.len());

    // Expected block
    let _ = writeln!(s, "\n--- expected ({} lines) ---", e.len());
    for (ln, line) in e[start..end_e].iter().enumerate() {
        let line_no = start + ln + 1;
        let marker = if line_no == i + 1 { ">" } else { " " };
        let _ = writeln!(s, "{:>6}  {} {}", line_no, marker, line);
    }

    let _ = writeln!(s, "\n+++ produced ({} lines) +++", p.len());
    for (ln, line) in p[start..end_p].iter().enumerate() {
        let line_no = start + ln + 1;
        let marker = if line_no == i + 1 { ">" } else { " " };
        let _ = writeln!(s, "{:>6}  {} {}", line_no, marker, line);
    }

    s
}

fn normalize_embedded_json(v: &mut Value) {
    match v {
        Value::Object(map) => {
            for child in map.values_mut() {
                normalize_embedded_json(child);
            }
        }
        Value::Array(arr) => {
            for item in arr {
                normalize_embedded_json(item);
            }
        }
        Value::String(s) => {
            let trimmed = s.trim_start();
            if trimmed.starts_with('{') || trimmed.starts_with('[') {
                if let Ok(mut parsed) = serde_json::from_str::<Value>(trimmed) {
                    normalize_embedded_json(&mut parsed);
                    *v = parsed;
                }
            }
        }
        _ => {}
    }
}
