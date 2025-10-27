use anyhow::{bail, Context, Result};
use std::{fs, path::Path, process::Command};

use crate::wit_assets;

pub fn scaffold(name: &str, lang: &str) -> Result<()> {
    let proj_dir = Path::new(name);
    if proj_dir.exists() {
        bail!("destination already exists: {}", proj_dir.display());
    }
    fs::create_dir_all(&proj_dir)?;

    let wit_dst = proj_dir.join(".tangent/wit");
    write_embedded_wit(&wit_dst)?;

    fs::write(proj_dir.join(".gitignore"), GITIGNORE)?;
    fs::write(proj_dir.join("README.md"), readme_for(lang, name))?;

    match lang {
        "go" | "golang" => scaffold_go(name, &proj_dir)?,
        "py" | "python" => scaffold_py(name, &proj_dir)?,
        other => bail!("unsupported --lang {other} (try: go, py)"),
    }

    println!(
        "✅ Scaffolded {} ({}) at {}",
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
    fs::write(dir.join("Makefile"), GO_MAKEFILE)?;
    fs::write(dir.join("tangent.yaml"), tangent_config_for("go", name))?;

    run_go_download(dir)?;

    run_wit_bindgen_go(dir, "processor", "./.tangent/wit/")?;
    Ok(())
}

fn scaffold_py(name: &str, dir: &Path) -> Result<()> {
    fs::write(dir.join("pyproject.toml"), PY_PROJECT)?;
    fs::write(dir.join("requirements.txt"), PY_REQUIREMENTS)?;
    fs::write(dir.join("mapper.py"), PY_APP)?;
    fs::write(dir.join("tangent.yaml"), tangent_config_for("py", name))?;

    run_wit_bindgen_py(dir, "processor", "./.tangent/wit/")?;
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
"#;

fn readme_for(lang: &str, name: &str) -> String {
    match lang {
        "go" | "golang" => format!(
            r#"# {name}

Go/TinyGo WASI component for Tangent.

## Build
```bash
make build

Run compile-wasm (from Tangent CLI)
tangent compile-wasm --config ./tangent.yaml --wit ./.tangent/wit
```

"#
        ),
        "py" | "python" => format!(
            r#"# {name}

Python component for Tangent (componentize-py).

Setup
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

Build
tangent compile-wasm --config ./tangent.yaml --wit ./.tangent/wit


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
    let path = if language == "py" { "app.py" } else { "." };

    format!(
        r#"module_type: {language}
runtime:
    plugins_path: "plugins/"
plugins:
    {name}:
	    type: {language}
		path: {path}
sources:
    socket_main:
        type: socket
        path: "/tmp/sidecar.sock"
sinks:
    s3_bucket:
        type: s3
        bucket_name: my-bucket
        max_inflight: 4
        max_file_age_seconds: 15"#
    )
}

fn go_main_for(module: &str) -> String {
    let tpl = r#"package main

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"
	"{module}/internal/tangent/logs/log"
	"{module}/internal/tangent/logs/mapper"

	"github.com/segmentio/encoding/json"

	"go.bytecodealliance.org/cm"
)

var (
	bufPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}
)

func Wire() {
	mapper.Exports.Metadata = func() mapper.Meta {
		return mapper.Meta{
			Name:    "my new plugin",
			Version: "0.1.3",
		}
	}

	mapper.Exports.Probe = func() cm.List[mapper.Selector] {
		return cm.ToList([]mapper.Selector{
			{
				Any: cm.ToList([]mapper.Pred{}),
				All: cm.ToList([]mapper.Pred{}),
				None: cm.ToList([]mapper.Pred{}),
			},
		})
	}

	mapper.Exports.ProcessLogs = func(input cm.List[log.Logview]) (res cm.Result[cm.List[uint8], cm.List[uint8], string]) {
		buf := bufPool.Get().(*bytes.Buffer)
		buf.Reset()

		var items []log.Logview
		items = append(items, input.Slice()...)
		for idx := range items {
			lv := log.Logview(items[idx])			
		}

		res.SetOK(cm.ToList(buf.Bytes()))
		return
	}
}

func getBool(v log.Logview, path string) *bool {
	opt := v.Get(path)
	if opt.None() {
		return nil
	}
	s := opt.Value()
	return s.Boolean()
}

func getInt64(v log.Logview, path string) *int64 {
	opt := v.Get(path)
	if opt.None() {
		return nil
	}
	s := opt.Value()
	return s.Int()
}

func getFloat(v log.Logview, path string) *float64 {
	opt := v.Get(path)
	if opt.None() {
		return nil
	}
	s := opt.Value()
	return s.Float()
}

func getString(v log.Logview, path string) *string {
	opt := v.Get(path)
	if opt.None() {
		return nil
	}
	s := opt.Value()
	return s.Str()
}

func getStringList(v log.Logview, path string) ([]string, bool) {
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

func init() {
	Wire()
}

func main() {}


"#;

    tpl.replace("{module}", module)
}

const GO_MAKEFILE: &str = "build:\n\t\
tangent compile-wasm --config tangent.yaml --wit ./.tangent/wit\n\
\n\
.PHONY: build\n";

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
        return mapper.Meta(name="example-mapper", version="0.1.3")

    def probe(self) -> List[mapper.Selector]:
        return [mapper.Selector(any=[], all=[], none=[])]

    def process_logs(
        self,
        logs: List[log.Logview]
    ) -> wit_world.Result[bytes, str]:
        buf = bytearray()

        for lv in logs:
            with lv:
                out = {}
                # Check presence
                if lv.has("message"):
                    s = lv.get("message")  # Optional[log.Scalar]
                    message = s.value if s is not None else None
                    out.message = message

                # Get string field
                s = lv.get("host.name")
                out.host = s.value if s is not None else None

                # Get list field
                lst = lv.get_list("tags")  # Optional[List[log.Scalar]]
                out.tags = [x.value for x in lst] if lst is not None else []

                # Get map/object field
                # Optional[List[Tuple[str, log.Scalar]]]
                m = lv.get_map("labels")
                out.labels = {k: v.value for k,
                              v in m} if m is not None else {}

                # Inspect available keys at a path
                out.top_keys = lv.keys("")  # top-level keys
                out.detail_keys = lv.keys("detail")

                buf.extend(json.dumps(out).encode("utf-8") + b"\n")

        return wit_world.Ok(bytes(buf))


"#;
