use anyhow::{bail, Context, Result};
use handlebars::Handlebars;
use serde_json::json;
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
    fs::write(dir.join("wrapper.go"), render_go_wrapper(name)?)?;
    fs::write(dir.join("main.go"), GO_MAIN)?;
    fs::write(dir.join("Makefile"), GO_MAKEFILE)?;
    fs::write(dir.join("tangent.yaml"), tangent_config_for("go"))?;

    run_go_download(dir)?;

    run_wit_bindgen_go(dir, "processor", "./.tangent/wit/")?;
    Ok(())
}

fn scaffold_py(_name: &str, dir: &Path) -> Result<()> {
    fs::write(dir.join("pyproject.toml"), PY_PROJECT)?;
    fs::write(dir.join("requirements.txt"), PY_REQUIREMENTS)?;
    fs::write(dir.join("wrapper.py"), PY_WRAPPER)?;
    fs::write(dir.join("app.py"), PY_APP)?;
    fs::write(dir.join("tangent.yaml"), tangent_config_for("py"))?;
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

fn render_go_wrapper(module: &str) -> anyhow::Result<String> {
    let mut hb = Handlebars::new();
    hb.register_template_string("go_wrapper", GO_WRAPPER_TMPL)?;
    Ok(hb.render("go_wrapper", &json!({ "module": module }))?)
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
	github.com/segmentio/encoding v0.5.3
	go.bytecodealliance.org/cm v0.3.0
)

require (
	github.com/coreos/go-semver v0.3.1 // indirect
	github.com/docker/libtrust v0.0.0-20160708172513-aabc10ec26b7 // indirect
	github.com/klauspost/compress v1.18.0 // indirect
	github.com/opencontainers/go-digest v1.0.0 // indirect
	github.com/regclient/regclient v0.8.3 // indirect
	github.com/segmentio/asm v1.1.3 // indirect
	github.com/sirupsen/logrus v1.9.3 // indirect
	github.com/tetratelabs/wazero v1.9.0 // indirect
	github.com/ulikunitz/xz v0.5.12 // indirect
	github.com/urfave/cli/v3 v3.3.3 // indirect
	go.bytecodealliance.org v0.7.0 // indirect
	golang.org/x/mod v0.26.0 // indirect
	golang.org/x/sys v0.34.0 // indirect
	golang.org/x/tools v0.35.0 // indirect
)

tool go.bytecodealliance.org/cmd/wit-bindgen-go
"#
    )
}

fn tangent_config_for(language: &str) -> String {
    format!(
        r#"module_type: {language}
entry_point: .
batch_size: 1024
sources:
    socket_main:
        type: socket
        path: "/tmp/sidecar.sock"
sinks:
    s3_bucket:
        type: s3
        bucket_name: my-bcuket
        max_inflight: 4
        max_file_age_seconds: 15"#
    )
}

const GO_WRAPPER_TMPL: &str = r#"
package main

import (
	"bytes"
	"errors"
	"io"
	"{{module}}/internal/tangent/logs/processor"
	"sync"

	"github.com/segmentio/encoding/json"

	"go.bytecodealliance.org/cm"
)

var (
	rdrPool = sync.Pool{New: func() any { return new(bytes.Reader) }}
	mapPool = sync.Pool{
		New: func() any { return make(map[string]any, 64) },
	}
	bufPool = sync.Pool{New: func() any { return new(bytes.Buffer) }}
)

type LogOutput struct {
	Sinks []processor.Sink
	Data  any
}

func S3(name string, prefix *string) processor.Sink {
	if prefix != nil {
		return processor.SinkS3(processor.S3Sink{Name: name, KeyPrefix: cm.Some(*prefix)})
	}
	return processor.SinkS3(processor.S3Sink{Name: name, KeyPrefix: cm.None[string]()})
}

type Handler interface {
	// Input: slice of objects decoded.
	// Output: slice of objects to emit.
	ProcessLog(log map[string]any) (*LogOutput, error)
}

func Wire(h Handler) {
	processor.Exports.ProcessLogs = func(input cm.List[uint8]) (r cm.Result[cm.List[processor.Output], cm.List[processor.Output], string]) {
		in := input.Slice()

		rdr := rdrPool.Get().(*bytes.Reader)
		rdr.Reset(in)
		defer rdrPool.Put(rdr)

		dec := json.NewDecoder(rdr)
		dec.UseNumber()

		type sinkKey struct {
			name   string
			prefix cm.Option[string]
		}
		type sinkState struct {
			key sinkKey
			buf *bytes.Buffer
			enc *json.Encoder
		}

		states := make(map[sinkKey]*sinkState, 4)

		for {
			m := mapPool.Get().(map[string]any)
			for k := range m {
				delete(m, k)
			}

			if err := dec.Decode(&m); err != nil {
				if errors.Is(err, io.EOF) {
					mapPool.Put(m)
					break
				}
				r.SetErr(err.Error())
				mapPool.Put(m)
				return
			}

			out, err := h.ProcessLog(m)
			clear(m)
			mapPool.Put(m)

			if err != nil {
				r.SetErr(err.Error())
				return
			}

			if out == nil || len(out.Sinks) == 0 {
				continue
			}

			for _, s := range out.Sinks {
				var k sinkKey
				if s3 := s.S3(); s3 != nil {
					k = sinkKey{name: s3.Name, prefix: s3.KeyPrefix}
				} else {
					r.SetErr("unknown sink type")
					return
				}

				st, ok := states[k]
				if !ok {
					buf := bufPool.Get().(*bytes.Buffer)
					buf.Reset()
					enc := json.NewEncoder(buf)
					enc.SetEscapeHTML(false)
					st = &sinkState{key: k, buf: buf, enc: enc}
					states[k] = st
				}

				if err := st.enc.Encode(out.Data); err != nil {
					r.SetErr(err.Error())
					return
				}
			}

		}

		outputs := make([]processor.Output, 0, len(states))
		for _, st := range states {
			data := st.buf.Bytes()
			sinks := []processor.Sink{processor.SinkS3(processor.S3Sink{
				Name:      st.key.name,
				KeyPrefix: st.key.prefix,
			})}
			outputs = append(outputs, processor.Output{
				Data:  cm.ToList(data),
				Sinks: cm.ToList(sinks),
			})
		}

		for _, st := range states {
			st.buf.Reset()
			bufPool.Put(st.buf)
		}

		r.SetOK(cm.ToList(outputs))
		return
	}
}

func init() {
	Wire(Processor{})
}

func main() {}

"#;

const GO_MAIN: &str = r#"
package main

type Processor struct{}

func (p Processor) ProcessLog(log map[string]any) (*LogOutput, error) {
	return nil, nil
}

"#;

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

const PY_WRAPPER: &str = r#"

Implement the functions required by the processor world here
Example:
def process_logs(input: bytes) -> bytes:
return input

"#;

const PY_APP: &str = r#"

Example module that will be componentized
Import/define functions referenced by wrapper.py

def process_logs(input: bytes) -> bytes:
return input
"#;
