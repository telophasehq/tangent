use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::fs::{self, File};
use std::io::{BufRead, BufReader};
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
use tangent_shared::sources::common::{DecodeCompression, DecodeFormat, Decoding, SourceConfig};
use tangent_shared::sources::file;

#[derive(Debug)]
pub struct TestOptions {
    pub plugin: Option<String>,
    pub config_path: PathBuf,
}

pub async fn run(opts: TestOptions) -> Result<()> {
    let cfg = Config::from_file(&opts.config_path)?;
    let config_root = &opts
        .config_path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .canonicalize()?;

    let mut rt = RuntimeOptions::default();
    rt.once = true;

    let mut plugins_to_test = Vec::<(String, PluginConfig)>::new();

    if opts.plugin.is_some() {
        let mut found = false;
        for (name, plugin_cfg) in cfg.plugins {
            if Some(&name) == opts.plugin.as_ref() {
                found = true;
                plugins_to_test.push((name, plugin_cfg));
                break;
            }
        }

        if !found {
            bail!(
                "plugin {} not found in tangent config",
                opts.plugin.unwrap()
            );
        }
    } else {
        for (name, plugin_cfg) in cfg.plugins {
            plugins_to_test.push((name, plugin_cfg));
        }
    }

    for (name, plugin_cfg) in plugins_to_test {
        for test in plugin_cfg.tests {
            let input = config_root
                .join(test.input)
                .canonicalize()
                .context("test input file")?;
            let expected = config_root
                .join(test.expected)
                .canonicalize()
                .context("test expected file")?;

            let plugins_path = config_root
                .join(plugin_cfg.path.clone())
                .canonicalize()
                .context("plugins path")?;

            let input_source = SourceConfig::File(file::FileConfig {
                path: input,
                decoding: Decoding {
                    compression: DecodeCompression::None,
                    format: DecodeFormat::JsonArray,
                },
            });

            let out_file = PathBuf::from_str("test_out.ndjson")?;
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

            let runtime = RuntimeConfig {
                plugins_path: cfg.runtime.plugins_path.clone(),
                batch_size: 1,
                batch_age: 1,
                workers: 1,
            };

            let entry = Edge {
                from: NodeRef::Source {
                    name: "input".into(),
                },
                to: vec![NodeRef::Plugin { name: name.clone() }],
            };

            let exit = Edge {
                from: NodeRef::Plugin { name: name.clone() },
                to: vec![NodeRef::Sink {
                    name: "out".into(),
                    key_prefix: None,
                }],
            };

            let mut sinks = BTreeMap::new();
            sinks.insert(String::from("out"), file_sink);

            let mut sources = BTreeMap::new();
            sources.insert(String::from("input"), input_source);

            let plugin_config = PluginConfig {
                module_type: "".to_string(), // not used
                path: plugins_path,
                tests: vec![],
            };

            let mut plugins = BTreeMap::new();
            plugins.insert(name.clone(), plugin_config);

            let test_config = tangent_shared::Config {
                runtime,
                sources,
                sinks,
                plugins,
                dag: vec![entry, exit],
            };

            let yaml = serde_yaml::to_string(&test_config)?;

            let test_file = PathBuf::from(".test.yaml");
            let test_config_file = config_root.join(&test_file);
            fs::write(&test_config_file, yaml)?;

            tangent_runtime::run(&test_config_file, rt.clone()).await?;

            let produced = read_ndjson(&out_file).context("reading produced NDJSON")?;
            let expected = read_json(&expected)?;

            if produced.is_array() != expected.is_array() {
                warn!("❌ test failed: output differs from expected\n");
                bail!(
                    "output is array: {}, expected is array: {}",
                    produced.is_array(),
                    expected.is_array()
                );
            }
            let diffs = diff_lines(&expected, &produced);

            if diffs.is_empty() {
                info!("✅ test passed: output matches expected");
            } else {
                warn!("❌ test failed: output differs from expected\n{}", diffs);
                bail!("output differs from expected");
            }
        }
    }
    Ok(())
}

fn read_json(path: &Path) -> Result<Value> {
    let data = fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
    let v: Value = serde_json::from_str(&data)
        .with_context(|| format!("parse JSON array from {}", path.display()))?;
    Ok(stabilize(v))
}

fn read_ndjson(path: &Path) -> Result<Value> {
    let file = File::open(path).with_context(|| format!("read {}", path.display()))?;

    let mut out: Vec<Value> = vec![];
    for line in BufReader::new(file).lines() {
        let line = line?;
        let v: Value =
            serde_json::from_str(&line).with_context(|| format!("parse JSON line: {}", line))?;
        out.push(v);
    }

    let combined_out = Value::Array(out);

    Ok(stabilize(combined_out))
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
