use anyhow::{bail, Context, Result};
use std::{fs, path::Path, process::Command};

use crate::wit_assets;

pub fn scaffold(name: &str, lang: &str) -> Result<()> {
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
    fs::write(proj_dir.join("README.md"), readme_for(lang, name))?;
    fs::create_dir(proj_dir.join("tests"))?;
    fs::create_dir(proj_dir.join("plugins"))?;
    fs::write(proj_dir.join("tests/input.json"), TEST_INPUT)?;
    fs::write(proj_dir.join("tests/expected.json"), TEST_EXPECTED)?;

    match lang {
        "go" | "golang" => scaffold_go(name, &proj_dir)?,
        "py" | "python" => scaffold_py(name, &proj_dir)?,
        other => bail!("unsupported --lang {other} (try: go, py)"),
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
    fs::write(dir.join("tangenthelpers/helpers.go"), go_helpers_for(name))?;
    fs::write(dir.join("tangent.yaml"), tangent_config_for("go", name))?;

    run_go_download(dir)?;

    run_wit_bindgen_go(dir, "processor", ".tangent/wit/")?;
    Ok(())
}

fn scaffold_py(name: &str, dir: &Path) -> Result<()> {
    fs::write(dir.join("pyproject.toml"), PY_PROJECT)?;
    fs::write(dir.join("requirements.txt"), PY_REQUIREMENTS)?;
    fs::write(dir.join("mapper.py"), PY_APP)?;
    fs::write(dir.join("tangent.yaml"), tangent_config_for("py", name))?;

    run_wit_bindgen_py(dir, "processor", ".tangent/wit/")?;
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
**/test_out.json
**/.test.yaml
**/plugins/
"#;

fn readme_for(lang: &str, name: &str) -> String {
    match lang {
        "go" | "golang" => format!(
            r#"# {name}

Go component for Tangent.

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
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
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
    
go 1.24

toolchain go1.24.7

require (
	github.com/buger/jsonparser v1.1.1
	github.com/segmentio/encoding v0.5.3
	github.com/telophasehq/go-ocsf v0.2.1
	go.bytecodealliance.org/cm v0.3.0
)

require (
	github.com/apache/arrow-go/v18 v18.2.1-0.20250425153947-5ae8b27ab357 // indirect
	github.com/coreos/go-semver v0.3.1 // indirect
	github.com/docker/libtrust v0.0.0-20160708172513-aabc10ec26b7 // indirect
	github.com/goccy/go-json v0.10.5 // indirect
	github.com/google/flatbuffers v25.2.10+incompatible // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/regclient/regclient v0.8.3 // indirect
	github.com/segmentio/asm v1.1.3 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/tetratelabs/wazero v1.9.0 // indirect
	github.com/ulikunitz/xz v0.5.12 // indirect
	github.com/urfave/cli/v3 v3.3.3 // indirect
	go.bytecodealliance.org v0.7.0 // indirect
	golang.org/x/exp v0.0.0-20250408133849-7e4ce0ab07d0 // indirect
	golang.org/x/mod v0.26.0 // indirect
	golang.org/x/sync v0.16.0 // indirect
	golang.org/x/sys v0.34.0 // indirect
	golang.org/x/tools v0.35.0 // indirect
	golang.org/x/xerrors v0.0.0-20240903120638-7835f813f4da // indirect
)

tool go.bytecodealliance.org/cmd/wit-bindgen-go
"#
    )
}

fn tangent_config_for(language: &str, name: &str) -> String {
    let path = if language == "py" { "mapper.py" } else { "." };

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
  socket_main:
    type: socket
    path: "/tmp/sidecar.sock"
sinks:
  blackhole:
    type: blackhole
dag:
  - from:
      kind: source
      name: socket_main
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

fn go_helpers_for(module: &str) -> String {
    let tpl = r#"package tangenthelpers

import "{module}/internal/tangent/logs/log"

func GetBool(v log.Logview, path string) *bool {
	opt := v.Get(path)
	if opt.None() {
		return nil
	}
	s := opt.Value()
	return s.Boolean()
}

func GetInt64(v log.Logview, path string) *int64 {
	opt := v.Get(path)
	if opt.None() {
		return nil
	}
	s := opt.Value()
	return s.Int()
}

func GetFloat64(v log.Logview, path string) *float64 {
	opt := v.Get(path)
	if opt.None() {
		return nil
	}
	s := opt.Value()
	return s.Float()
}

func GetString(v log.Logview, path string) *string {
	opt := v.Get(path)
	if opt.None() {
		return nil
	}
	s := opt.Value()
	return s.Str()
}

func GetStringList(v log.Logview, path string) ([]string, bool) {
	opt := v.GetList(path)
	if opt.None() {
		return nil, false
	}
	lst := opt.Value()
	out := make([]string, 0, lst.Len())
	data := lst.Slice()
	for i := 0; i < int(lst.Len()); i++ {
		if p := data[i].Str(); p != nil {
			out = append(out, *p)
		}
	}
	return out, true
}

func GetFloat64List(v log.Logview, path string) ([]float64, bool) {
	opt := v.GetList(path)
	if opt.None() {
		return nil, false
	}
	lst := opt.Value()
	out := make([]float64, 0, lst.Len())
	data := lst.Slice()
	for i := 0; i < int(lst.Len()); i++ {
		if p := data[i].Float(); p != nil {
			out = append(out, *p)
		}
	}
	return out, true
}

func GetInt64List(v log.Logview, path string) ([]int64, bool) {
	opt := v.GetList(path)
	if opt.None() {
		return nil, false
	}
	lst := opt.Value()
	out := make([]int64, 0, lst.Len())
	data := lst.Slice()
	for i := 0; i < int(lst.Len()); i++ {
		if p := data[i].Int(); p != nil {
			out = append(out, *p)
		}
	}
	return out, true
}

"#;
    tpl.replace("{module}", module)
}

fn go_main_for(module: &str) -> String {
    let tpl = r#"package main

import (
	"bytes"
	"sync"
	"{module}/internal/tangent/logs/log"
	"{module}/internal/tangent/logs/mapper"
    "{module}/tangenthelpers"

	"github.com/segmentio/encoding/json"

	"go.bytecodealliance.org/cm"
)

var (
	bufPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}
)

type ExampleOutput struct {
	Msg      string   `json:"message"`
	Level    string   `json:"level"`
	Seen     int64    `json:"seen"`
	Duration float64  `json:"duration"`
	Service  string   `json:"service"`
	Tags     []string `json:"tags"`
}

func Wire() {
    // Metadata is for naming and versioning your plugin.
	mapper.Exports.Metadata = func() mapper.Meta {
		return mapper.Meta{
			Name:    "go-example",
			Version: "0.1.0",
		}
	}

    // Probe allows the mapper to subscribe to logs with specific fields.
	mapper.Exports.Probe = func() cm.List[mapper.Selector] {
		return cm.ToList([]mapper.Selector{
			{
				Any: cm.ToList([]mapper.Pred{}),
				All: cm.ToList([]mapper.Pred{
					mapper.PredEq(
						cm.Tuple[string, mapper.Scalar]{
							F0: "source.name",
							F1: log.ScalarStr("myservice"),
						},
					)}),
				None: cm.ToList([]mapper.Pred{}),
			},
		})
	}

    // ProcessLogs takes a batch of logs, transforms, and outputs bytes.
	mapper.Exports.ProcessLogs = func(input cm.List[log.Logview]) (res cm.Result[cm.List[uint8], cm.List[uint8], string]) {
		buf := bufPool.Get().(*bytes.Buffer)
		buf.Reset()

        // Copy out the slice so we own the backing array.
        // The cm.List view may be backed by a transient buffer that
        // can be reused or mutated after this call, so we take an owned copy.
		var items []log.Logview
		items = append(items, input.Slice()...)
		for idx := range items {
			var out ExampleOutput

			lv := log.Logview(items[idx])

			// Get String
			msg := tangenthelpers.GetString(lv, "msg")
			if msg != nil {
				out.Msg = *msg
			}

			// get dot path
			lvl := tangenthelpers.GetString(lv, "msg.level")
			if lvl != nil {
				out.Level = *lvl
			}

			// get int
			seen := tangenthelpers.GetInt64(lv, "seen")
			if seen != nil {
				out.Seen = *seen
			}

			// get float
			duration := tangenthelpers.GetFloat64(lv, "duration")
			if duration != nil {
				out.Duration = *duration
			}

			// get value from nested json
			service := tangenthelpers.GetString(lv, "source.name")
			if service != nil {
				out.Service = *service
			}

			// get string list
			tags, ok := tangenthelpers.GetStringList(lv, "tags")
			if ok {
				out.Tags = tags
			}

			// Serialize with Segment's encoding/json
			err := json.NewEncoder(buf).Encode(out)
			if err != nil {
				res.SetErr(err.Error()) // error out the entire batch
				return
			}
		}

		res.SetOK(cm.ToList(buf.Bytes()))
		bufPool.Put(buf)
		return
	}
}

func init() {
	Wire()
}

func main() {}
"#;

    tpl.replace("{module}", module)
}

const TEST_INPUT: &str = r#"{
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
}"#;

const TEST_EXPECTED: &str = r#"{
  "message": "my log",
  "level": "info",
  "seen": 5,
  "duration": 6.3,
  "tags": [
    "tag1"
  ],
  "service":  "myservice"
}"#;

const MAKEFILE: &str = "build:\n\t\
tangent plugin compile --config tangent.yaml\n\
\n\
test: build\n\t\
tangent plugin test --config tangent.yaml\n\
\n\
run: build\n\t\
tangent run --config tangent.yaml\n\
\n\
.PHONY: build test\n";

const PY_PROJECT: &str = r#"
[project]
name = "tangent-app"
version = "0.1.0"
requires-python = ">=3.10"
dependencies = [
"componentize-py>=0.13"
]
"#;

const PY_REQUIREMENTS: &str = r#"
componentize-py>=0.13
"#;

const PY_APP: &str = r#"
from typing import List

import json
import wit_world
from wit_world.exports import mapper
from wit_world.imports import log


class Mapper(wit_world.WitWorld):
    def metadata(self) -> mapper.Meta:
        return mapper.Meta(name="python-example", version="0.1.0")

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
