use anyhow::{bail, Context, Result};
use std::os::unix::fs::PermissionsExt;
use std::{fs, path::Path, process::Command};

use crate::wit_assets;

const GO_AGENTS_MD: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../assets/go_Agents.md"
));
const PY_AGENTS_MD: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../assets/py_Agents.md"
));
const RUST_AGENTS_MD: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../assets/rust_Agents.md"
));

const GO_SETUP: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../assets/go_setup.sh"
));
const PY_SETUP: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../assets/py_setup.sh"
));
const RUST_SETUP: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../assets/rust_setup.sh"
));
const DOCKERFILE: &str = include_str!(concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../assets/Dockerfile"
));

pub fn scaffold(name: &str, lang: &str) -> Result<()> {
    let renamed = name.replace("-", "");
    let name = renamed.as_str();

    let proj_dir = Path::new(name);
    if proj_dir.exists() {
        bail!("destination already exists: {}", proj_dir.display());
    }

    println!("🔧 Creating new plugin at {}/", proj_dir.display());
    fs::create_dir_all(&proj_dir)?;

    let wit_dst = proj_dir.join(".tangent/wit");
    write_embedded_wit(&wit_dst)?;

    fs::write(proj_dir.join(".gitignore"), GITIGNORE)?;
    fs::write(proj_dir.join("Makefile"), MAKEFILE)?;
    fs::write(proj_dir.join("Dockerfile"), DOCKERFILE)?;
    fs::write(proj_dir.join("README.md"), readme_for(lang, name))?;
    fs::create_dir(proj_dir.join("tests"))?;
    fs::create_dir(proj_dir.join("plugins"))?;
    fs::write(proj_dir.join("tests/input.json"), TEST_INPUT)?;
    fs::write(proj_dir.join("tests/expected.json"), TEST_EXPECTED)?;
    fs::write(proj_dir.join("tests/bench.json"), TEST_BENCH)?;

    match lang {
        "go" => scaffold_go(name, &proj_dir)?,
        "python" => scaffold_py(name, &proj_dir)?,
        "rust" => scaffold_rust(name, &proj_dir)?,
        other => bail!("unsupported --lang {other} (options: go, python, rust)"),
    }

    println!(
        "✅ Scaffolded {} ({}) at {}/",
        name,
        lang,
        proj_dir.display()
    );
    Ok(())
}

pub fn write_embedded_wit(dest: &Path) -> Result<()> {
    fs::create_dir_all(dest)?;
    for entry in wit_assets::WIT_DIR.find("**/*").unwrap() {
        match entry {
            include_dir::DirEntry::Dir(d) => {
                fs::create_dir_all(dest.join(d.path()))?;
            }
            include_dir::DirEntry::File(f) => {
                if f.path().extension().is_some_and(|e| e == ".md") {
                    continue;
                }
                let out = dest.join(f.path());
                if let Some(parent) = out.parent() {
                    fs::create_dir_all(parent)?;
                }
                fs::write(&out, f.contents())
                    .with_context(|| format!("writing {}", out.display()))?;
            }
        }
    }
    Ok(())
}

fn scaffold_go(name: &str, dir: &Path) -> Result<()> {
    fs::write(dir.join("go.mod"), go_mod_for(name))?;
    fs::write(dir.join("main.go"), go_main_for(name))?;
    fs::create_dir(dir.join("tangenthelpers"))?;
    fs::write(dir.join("tangent.yaml"), tangent_config_for("go", name))?;
    fs::write(dir.join("Agents.md"), GO_AGENTS_MD)?;

    let setup_path = dir.join("setup.sh");
    fs::write(&setup_path, GO_SETUP)?;
    let mut permissions = fs::metadata(&setup_path)?.permissions();
    permissions.set_mode(permissions.mode() | 0o111);
    fs::set_permissions(&setup_path, permissions)?;

    run_setup(dir)?;
    run_go_download(dir)?;

    run_wit_bindgen_go(dir, "processor", ".tangent/wit/")?;
    Ok(())
}

fn scaffold_py(name: &str, dir: &Path) -> Result<()> {
    fs::write(dir.join("pyproject.toml"), py_project_for(name))?;
    fs::write(dir.join("mapper.py"), py_mapper_for(name))?;
    fs::write(dir.join("tangent.yaml"), tangent_config_for("python", name))?;
    fs::write(dir.join("Agents.md"), PY_AGENTS_MD)?;
    fs::write(dir.join("requirements.txt"), PYTHON_REQUIREMENTS)?;

    let setup_path = dir.join("setup.sh");
    fs::write(&setup_path, PY_SETUP)?;
    let mut permissions = fs::metadata(&setup_path)?.permissions();
    permissions.set_mode(permissions.mode() | 0o111);
    fs::set_permissions(&setup_path, permissions)?;

    run_setup(dir)?;
    run_wit_bindgen_py(dir, "processor", ".tangent/wit/")?;
    Ok(())
}

fn scaffold_rust(name: &str, dir: &Path) -> Result<()> {
    fs::create_dir(dir.join("src"))?;
    fs::write(dir.join("Cargo.toml"), rust_cargo_toml_for(name))?;
    fs::write(dir.join("src/lib.rs"), rust_lib_for(name))?;
    fs::write(dir.join("tangent.yaml"), tangent_config_for("rust", name))?;
    fs::write(dir.join("Agents.md"), RUST_AGENTS_MD)?;

    let setup_path = dir.join("setup.sh");
    fs::write(&setup_path, RUST_SETUP)?;
    let mut permissions = fs::metadata(&setup_path)?.permissions();
    permissions.set_mode(permissions.mode() | 0o111);
    fs::set_permissions(&setup_path, permissions)?;

    run_setup(dir)?;

    Ok(())
}

fn run_setup(cwd: &Path) -> Result<()> {
    let out = Command::new("./setup.sh")
        .current_dir(cwd)
        .output()
        .with_context(|| format!("failed to spawn setup {}", cwd.display()))?;

    if !out.status.success() {
        let mut msg = String::from_utf8_lossy(&out.stderr).to_string();
        if msg.trim().is_empty() {
            msg = String::from_utf8_lossy(&out.stdout).to_string();
        }
        bail!("setup failed:\n{}", msg);
    }
    Ok(())
}

fn run_wit_bindgen_py(cwd: &Path, world: &str, wit_entry: &str) -> Result<()> {
    let out = Command::new("componentize-py")
        .args(["-d", &wit_entry, "-w", world, "bindings", "."])
        .current_dir(cwd)
        .output()
        .with_context(|| format!("failed to spawn componentize-py in {}", cwd.display()))?;

    if !out.status.success() {
        let mut msg = String::from_utf8_lossy(&out.stderr).to_string();
        if msg.trim().is_empty() {
            msg = String::from_utf8_lossy(&out.stdout).to_string();
        }
        bail!("componentize-py failed:\n{}", msg);
    }
    Ok(())
}

fn run_wit_bindgen_go(cwd: &Path, world: &str, wit_entry: &str) -> Result<()> {
    let out = Command::new("go")
        .args([
            "tool",
            "wit-bindgen-go",
            "generate",
            "--world",
            world,
            "--out",
            "internal",
            &wit_entry,
        ])
        .current_dir(cwd)
        .output()
        .with_context(|| format!("failed to spawn wit-bindgen-go in {}", cwd.display()))?;

    if !out.status.success() {
        let mut msg = String::from_utf8_lossy(&out.stderr).to_string();
        if msg.trim().is_empty() {
            msg = String::from_utf8_lossy(&out.stdout).to_string();
        }
        bail!("wit-bindgen-go failed:\n{}", msg);
    }
    Ok(())
}

fn run_go_download(cwd: &Path) -> Result<()> {
    let out = Command::new("go")
        .args(["mod", "tidy"])
        .current_dir(cwd)
        .output()
        .with_context(|| format!("failed to spawn go mod tidy in {}", cwd.display()))?;

    if !out.status.success() {
        let mut msg = String::from_utf8_lossy(&out.stderr).to_string();
        if msg.trim().is_empty() {
            msg = String::from_utf8_lossy(&out.stdout).to_string();
        }
        bail!("go mod tidy failed:\n{}", msg);
    }

    let tool_out = Command::new("go")
        .args(["get", "go.bytecodealliance.org/cmd/wit-bindgen-go"])
        .current_dir(cwd)
        .output()
        .with_context(|| format!("failed to spawn go get in {}", cwd.display()))?;

    if !tool_out.status.success() {
        let mut msg = String::from_utf8_lossy(&out.stderr).to_string();
        if msg.trim().is_empty() {
            msg = String::from_utf8_lossy(&out.stdout).to_string();
        }
        bail!("go get failed:\n{}", msg);
    }
    Ok(())
}

const GITIGNORE: &str = r#"
target/
*.wasm
.DS_Store
__pycache__/
*.pyc
**/test_out.ndjson
**/.test.yaml
**/plugins/
"#;

const PYTHON_REQUIREMENTS: &str = r#""#;

fn readme_for(lang: &str, name: &str) -> String {
    match lang {
        "go" | "golang" => format!(
            r#"# {name}

Go component for Tangent.

## Setup
```bash
./setup.sh
```

## Compile
```bash
tangent plugin compile --config tangent.yaml
```

## Test
```bash
tangent plugin test --config tangent.yaml
```

## Run server
```bash
tangent run --config tangent.yaml
```

## Benchmark performance
```bash
tangent run --config tangent.yaml
tangent bench --config tangent.yaml --seconds 30 --payload tests/input.json
```


## Using Makefile
```bash
# build and test
make test

# build and run
make run
```

"#
        ),
        "py" | "python" => format!(
            r#"# {name}

Python component for Tangent.

## Setup
```bash
./setup.sh
```

## Compile
```bash
tangent plugin compile --config tangent.yaml
```

## Test
```bash
tangent plugin test --config tangent.yaml
```

## Run server
```bash
tangent run --config tangent.yaml
```

## Benchmark performance
```bash
tangent run --config tangent.yaml
tangent bench --config tangent.yaml --seconds 30 --payload tests/input.json
```


## Using Makefile
```bash
# build and test
make test

# build and run
make run
```


"#
        ),
        "rust" => format!(
            r#"# {name}

Rust component for Tangent.

## Setup
```bash
./setup.sh
```

## Compile
```bash
tangent plugin compile --config tangent.yaml
```

## Test
```bash
tangent plugin test --config tangent.yaml
```

## Run server
```bash
tangent run --config tangent.yaml
```

## Benchmark performance
```bash
tangent run --config tangent.yaml
tangent bench --config tangent.yaml --seconds 30 --payload tests/input.json
```


## Using Makefile
```bash
# build and test
make test

# build and run
make run
```


"#
        ),
        _ => format!("# {name}\n"),
    }
}

fn go_mod_for(name: &str) -> String {
    format!(
        r#"module {name}
    
go 1.24.0

toolchain go1.24.7

require (
	github.com/telophasehq/tangent-sdk-go v0.0.0-20251120150230-0b8b366f72c4
	go.bytecodealliance.org/cm v0.3.0 // indirect
)

require github.com/mailru/easyjson v0.9.1

require (
	github.com/coreos/go-semver v0.3.1 // indirect
	github.com/davecgh/go-spew v1.1.2-0.20180830191138-d8f796af33cc // indirect
	github.com/docker/libtrust v0.0.0-20160708172513-aabc10ec26b7 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/regclient/regclient v0.8.3 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/tetratelabs/wazero v1.9.0 // indirect
	github.com/ulikunitz/xz v0.5.12 // indirect
	github.com/urfave/cli/v3 v3.3.3 // indirect
	go.bytecodealliance.org v0.7.0 // indirect
	golang.org/x/mod v0.29.0 // indirect
	golang.org/x/sys v0.37.0 // indirect
)

tool go.bytecodealliance.org/cmd/wit-bindgen-go
"#
    )
}

fn tangent_config_for(language: &str, name: &str) -> String {
    let path = if language == "python" {
        "mapper.py"
    } else {
        "."
    };

    format!(
        r#"runtime:
  plugins_path: "plugins/"
plugins:
  {name}:
    module_type: {language}
    path: {path}
    tests:
      - input: tests/input.json
        expected: tests/expected.json
sources:
  network_input:
    type: tcp
    bind_address: 0.0.0.0:9000
sinks:
  blackhole:
    type: blackhole
dag:
  - from:
      kind: source
      name: network_input
    to:
      - kind: plugin
        name: {name}

  - from:
      kind: plugin
      name: {name}
    to:
      - kind: sink
        name: blackhole"#
    )
}

fn go_main_for(module: &str) -> String {
    let tpl = r#"package main

import (
	tangent_sdk "github.com/telophasehq/tangent-sdk-go"
	"github.com/telophasehq/tangent-sdk-go/helpers"
)

//easyjson:json
type ExampleOutput struct {
	Msg		string		`json:"message"`
	Level		string		`json:"level"`
	Seen		int64		`json:"seen"`
	Duration	float64		`json:"duration"`
	Service		string		`json:"service"`
	Tags		[]string	`json:"tags"`
}

var Metadata = tangent_sdk.Metadata{
	Name:		"{module}",
	Version:	"0.2.0",
}

var selectors = []tangent_sdk.Selector{
	{
		All: []tangent_sdk.Predicate{
			tangent_sdk.EqString("source.name", "myservice"),
		},
	},
}

func ExampleMapper(lv tangent_sdk.Log) (ExampleOutput, error) {
	var out ExampleOutput
	// Get String
	msg := helpers.GetString(lv, "msg")
	if msg != nil {
		out.Msg = *msg
	}

	// get dot path
	lvl := helpers.GetString(lv, "msg.level")
	if lvl != nil {
		out.Level = *lvl
	}

	// get int
	seen := helpers.GetInt64(lv, "seen")
	if seen != nil {
		out.Seen = *seen
	}

	// get float
	duration := helpers.GetFloat64(lv, "duration")
	if duration != nil {
		out.Duration = *duration
	}

	// get value from nested json
	service := helpers.GetString(lv, "source.name")
	if service != nil {
		out.Service = *service
	}

	// get string list
	tags, ok := helpers.GetStringList(lv, "tags")
	if ok {
		out.Tags = tags
	}
	return out, nil
}

func init() {
	tangent_sdk.Wire[ExampleOutput](
		Metadata,
		selectors,
		ExampleMapper,
        nil,
	)
}

func main()	{}
"#;

    tpl.replace("{module}", module)
}

fn rust_cargo_toml_for(module: &str) -> String {
    let tpl = r#"[package]
name = "{module}"
version = "0.1.0"
edition = "2021"

[lib]
crate-type = ["cdylib"]

[workspace]

[dependencies]
serde = { version = "1", features = ["derive"] }
serde_json = "1"
wit-bindgen = "0.48.0"

[package.metadata.component]
package = "tangent:logs"

[package.metadata.component.target]
path = ".tangent/wit"
world = "processor"

[package.metadata.component.target.dependencies]
"wasi:cli" = { path = ".tangent/wit/deps/wasi-cli-0.2.0" }
"wasi:filesystem" = { path = ".tangent/wit/deps/wasi-filesystem-0.2.0" }
"wasi:io" = { path = ".tangent/wit/deps/wasi-io-0.2.0" }
"wasi:clocks" = { path = ".tangent/wit/deps/wasi-clocks-0.2.0" }
"wasi:sockets" = { path = ".tangent/wit/deps/wasi-sockets-0.2.0" }
"wasi:random" = { path = ".tangent/wit/deps/wasi-random-0.2.0" }

"#;

    tpl.replace("{module}", module)
}

fn rust_lib_for(module: &str) -> String {
    let tpl = r#"use serde::Serialize;

wit_bindgen::generate!({
    path: ".tangent/wit",
    world: "processor",
    generate_all,
});

use exports::tangent::logs::mapper::{Guest, Meta, Pred, Selector};
use tangent::logs::log::{Logview, Scalar};

struct Component;

export!(Component);

#[derive(Default, Serialize)]
struct ExampleOutput {
    message: String,
    level: String,
    seen: i64,
    duration: f64,
    service: String,
    tags: Option<Vec<String>>,
}

fn string_from_scalar(s: Scalar) -> Option<String> {
    match s {
        Scalar::Str(v) => Some(v),
        _ => None,
    }
}

fn int_from_scalar(s: Scalar) -> Option<i64> {
    match s {
        Scalar::Int(v) => Some(v),
        _ => None,
    }
}

fn float_from_scalar(s: Scalar) -> Option<f64> {
    match s {
        Scalar::Float(v) => Some(v),
        _ => None,
    }
}

impl Guest for Component {
    fn metadata() -> Meta {
        Meta {
            name: "{module}".to_string(),
            version: "0.1.0".to_string(),
        }
    }

    fn probe() -> Vec<Selector> {
        vec![Selector {
            any: Vec::new(),
            all: vec![Pred::Eq((
                "source.name".to_string(),
                Scalar::Str("myservice".to_string()),
            ))],
            none: Vec::new(),
        }]
    }

    fn process_logs(input: Vec<Logview>) -> Result<Vec<u8>, String> {
        let mut buf = Vec::new();

        for lv in input {
            let mut out = ExampleOutput::default();

            if let Some(val) = lv.get("msg").and_then(string_from_scalar) {
                out.message = val;
            }

            if let Some(val) = lv.get("msg.level").and_then(string_from_scalar) {
                out.level = val;
            }

            if let Some(val) = lv.get("seen").and_then(int_from_scalar) {
                out.seen = val;
            }

            if let Some(val) = lv.get("duration").and_then(float_from_scalar) {
                out.duration = val;
            }

            if let Some(val) = lv.get("source.name").and_then(string_from_scalar) {
                out.service = val;
            }

            if let Some(items) = lv.get_list("tags") {
                let mut tags = Vec::with_capacity(items.len());
                for item in items {
                    if let Scalar::Str(val) = item {
                        tags.push(val);
                    }
                }
                if !tags.is_empty() {
                    out.tags = Some(tags);
                }
            }

            let json_line = serde_json::to_vec(&out).map_err(|e| e.to_string())?;
            buf.extend(json_line);
            buf.push(b'\n');
        }

        Ok(buf)
    }
}

"#;

    tpl.replace("{module}", module)
}

const TEST_INPUT: &str = r#"[
  {
    "msg": "my log",
    "msg.level": "info",
    "seen": 5,
    "duration": 6.3,
    "tags": [
      "tag1"
    ],
    "source": {
      "name": "myservice"
    }
  },
  {
    "msg": "my log",
    "msg.level": "info",
    "seen": 5,
    "duration": 6.3,
    "tags": [
      "tag1"
    ],
    "source": {
      "name": "myservice"
    }
  }
]"#;

const TEST_EXPECTED: &str = r#"[
  {
    "message": "my log",
    "level": "info",
    "seen": 5,
    "duration": 6.3,
    "tags": [
      "tag1"
    ],
    "service": "myservice"
  },
  {
    "message": "my log",
    "level": "info",
    "seen": 5,
    "duration": 6.3,
    "tags": [
      "tag1"
    ],
    "service": "myservice"
  }
]"#;

const TEST_BENCH: &str = r#"{
  "source": {
    "name": {
      "$const": "myservice"
    }
  },
  "seen": {
    "$gte1": {
      "$int": {
        "min": 0,
        "max": 500
      }
    }
  },
  "msg": {
    "$string": {
      "len": 8
    }
  },
  "level": {
    "$oneOf": [
      "info",
      "warn",
      "error"
    ]
  },
  "duration": {
    "$normal": {
      "mean": 6,
      "stdev": 0.8,
      "min": 0
    }
  },
  "tags": {
    "$array": {
      "of": {
        "$oneOf": [
          "alpha",
          "beta",
          "prod"
        ]
      },
      "len": 2
    }
  }
}"#;

const MAKEFILE: &str = "build:\n\t\
./setup.sh\n\t\
tangent plugin compile --config tangent.yaml\n\
\n\
test: build\n\t\
tangent plugin test --config tangent.yaml\n\
\n\
run: build\n\t\
tangent run --config tangent.yaml\n\
\n\
.PHONY: build test\n";

fn py_project_for(module: &str) -> String {
    let tpl = r#"
[project]
name = "{module}"
version = "0.1.0"
requires-python = ">=3.10"
dependencies = []
"#;

    tpl.replace("{module}", module)
}

fn py_mapper_for(module: &str) -> String {
    let tpl = r#"
from typing import List

import json
import wit_world
from wit_world.exports import mapper
from wit_world.imports import log


class Mapper(wit_world.WitWorld):
    def metadata(self) -> mapper.Meta:
        return mapper.Meta(name="{module}", version="0.1.0")

    def probe(self) -> List[mapper.Selector]:
        # Match logs where source.name == "myservice"
        return [
            mapper.Selector(
                any=[],
                all=[
                    mapper.Pred_Eq(
                        ("source.name", log.Scalar_Str("myservice"))
                    )
                ],
                none=[],
            )
        ]

    def process_logs(
        self,
        logs: List[log.Logview]
    ) -> bytes:
        buf = bytearray()

        for lv in logs:
            with lv:
                out = {
                    "message": "",
                    "level": "",
                    "seen": 0,
                    "duration": 0.0,
                    "service": "",
                    "tags": None,
                }

                # get string
                s = lv.get("msg")
                if s is not None and hasattr(s, "value"):
                    out["message"] = s.value

                # get dot path
                s = lv.get("msg.level")
                if s is not None and hasattr(s, "value"):
                    out["level"] = s.value

                # get int
                s = lv.get("seen")
                if s is not None and hasattr(s, "value"):
                    out["seen"] = s.value

                # get float
                s = lv.get("duration")
                if s is not None and hasattr(s, "value"):
                    out["duration"] = s.value

                # get value from nested json
                s = lv.get("source.name")
                if s is not None and hasattr(s, "value"):
                    out["service"] = s.value

                # get string list
                lst = lv.get_list("tags")
                if lst is not None:
                    tags: List[str] = []
                    for item in lst:
                        tags.append(item.value)
                    out["tags"] = tags

                buf.extend(json.dumps(out).encode('utf-8') + b"\n")

        return bytes(buf)
"#;

    tpl.replace("{module}", module)
}
