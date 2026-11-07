use anyhow::{anyhow, bail, Context, Result};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};
use tangent_shared::Config;
use toml::Value;
use wasmtime::component::Component;
use which::which;

const WORLD: &str = "processor";

/// Where a plugin keeps the WIT it compiles against, beside its source.
pub const PLUGIN_WIT: &str = ".tangent/wit";

/// The WIT plugins compile against.
pub enum Wit<'a> {
    /// One directory, as it is, for every plugin.
    Dir(&'a Path),
    /// Written into each plugin's own `.tangent/wit` just before it compiles,
    /// replacing what was there: the interface of the tangent that will run
    /// it, so nothing keeps a copy that can fall behind.
    Write(&'a dyn Fn(&Path) -> Result<()>),
}

/// Every plugin of the config at `cfg_path` (or `only`, when given) compiled
/// from its source into `runtime.plugins_path`. A plugin with `prebuilt` and
/// no `path` is left to `tangent plugin fetch`.
pub fn compile_from_config(cfg_path: &PathBuf, wit: Wit, only: Option<&str>) -> Result<()> {
    let cfg = Config::from_file(cfg_path)?;

    let config_dir = cfg_path.parent().unwrap_or_else(|| Path::new("."));

    let plugins_path = config_dir.join(&cfg.runtime.plugins_path);
    fs::create_dir_all(&plugins_path)?;
    let out = plugins_path
        .canonicalize()
        .with_context(|| "configured plugins path")?;

    if let Some(only) = only {
        if !cfg.plugins.contains_key(only) {
            bail!("plugin {only} not found in {}", cfg_path.display());
        }
    }
    for (name, plugin) in cfg.plugins {
        if only.is_some_and(|only| only != name.as_ref()) {
            continue;
        }
        if plugin.path.as_os_str().is_empty() {
            if plugin.prebuilt.is_some() {
                println!("· {name}: no source, a prebuilt plugin; `tangent plugin fetch` gets it");
                continue;
            }
            bail!("plugin {name} has no `path`");
        }
        let entry_point_path = config_dir
            .join(&plugin.path)
            .canonicalize()
            .with_context(|| "configured plugin path")?;
        println!("⚙️ Compiling {}", entry_point_path.display());
        let wit_path = match &wit {
            Wit::Dir(dir) => dir.to_path_buf(),
            Wit::Write(write) => {
                let root = if entry_point_path.is_dir() {
                    entry_point_path.clone()
                } else {
                    entry_point_path
                        .parent()
                        .unwrap_or_else(|| Path::new("."))
                        .to_path_buf()
                };
                let dir = root.join(PLUGIN_WIT);
                // A Rust crate reads its WIT where its Cargo.toml says; one
                // that keeps it somewhere else is left alone.
                if plugin.module_type != "rust" || reads_plugin_wit(&root.join("Cargo.toml"))? {
                    if dir.exists() {
                        fs::remove_dir_all(&dir)
                            .with_context(|| format!("removing {}", dir.display()))?;
                    }
                    write(&dir).with_context(|| format!("writing the WIT to {}", dir.display()))?;
                }
                dir
            }
        };

        let full_out = &out.join(format!("{}.component.wasm", name));

        match plugin.module_type.as_str() {
            "python" => run_componentize_py(&wit_path, WORLD, &entry_point_path, &full_out)?,
            "go" => run_go_compile(&wit_path, WORLD, &entry_point_path, &full_out)?,
            "rust" => run_rust_compile(&entry_point_path, &full_out)?,
            ext => anyhow::bail!(
                "unsupported filetype: {} for wasm entrypoint: {}",
                ext,
                entry_point_path.display()
            ),
        }

        let cwasm_out = &out.join(format!("{name}.cwasm"));
        precompile(full_out, cwasm_out)?;

        println!(
            "✅ Compiled {} → {}",
            entry_point_path.display(),
            cwasm_out.display()
        );
    }
    Ok(())
}

/// The component at `component` serialized for this machine's wasmtime at
/// `cwasm`: what `tangent run` loads. A component is portable; its `.cwasm`
/// is not, so it is made where it runs.
pub fn precompile(component: &Path, cwasm: &Path) -> Result<()> {
    let engine = tangent_shared::wasm_engine::build()?;
    let c = Component::from_file(&engine, component)
        .map_err(anyhow::Error::from)
        .with_context(|| format!("loading component {}", component.display()))?;
    let bytes = c.serialize()?;
    fs::write(cwasm, bytes).with_context(|| format!("writing {}", cwasm.display()))?;
    Ok(())
}

fn ensure_cargo_component() -> Result<()> {
    let status = Command::new("cargo")
        .arg("component")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();

    match status {
        Ok(s) if s.success() => Ok(()),
        _ => bail!(
            "`cargo component` not found. Install via `cargo install cargo-component` and ensure the wasm32-wasip2 target is installed with `rustup target add wasm32-wasip2`."
        ),
    }
}

/// Whether the crate's `cargo component` target reads `.tangent/wit`.
fn reads_plugin_wit(manifest_path: &Path) -> Result<bool> {
    let contents = fs::read_to_string(manifest_path)
        .with_context(|| format!("reading {}", manifest_path.display()))?;
    let parsed: Value = contents
        .parse::<Value>()
        .with_context(|| format!("parsing {}", manifest_path.display()))?;
    Ok(parsed
        .get("package")
        .and_then(|p| p.get("metadata"))
        .and_then(|m| m.get("component"))
        .and_then(|c| c.get("target"))
        .and_then(|t| t.get("path"))
        .and_then(|p| p.as_str())
        == Some(PLUGIN_WIT))
}

fn package_name(manifest_path: &Path) -> Result<String> {
    let contents = fs::read_to_string(manifest_path)
        .with_context(|| format!("reading {}", manifest_path.display()))?;
    let parsed: Value = contents
        .parse::<Value>()
        .context("parsing Cargo.toml for package name")?;

    parsed
        .get("package")
        .and_then(|pkg| pkg.get("name"))
        .and_then(|n| n.as_str())
        .map(str::to_string)
        .ok_or_else(|| anyhow!("package.name not found in {}", manifest_path.display()))
}

fn ensure_tinygo() -> Result<()> {
    which("tinygo")
        .map(|_| ())
        .map_err(|_| anyhow!("`tinygo` not found in PATH. Install directions: https://tinygo.org/getting-started/install/"))
}

fn find_host_python() -> Result<PathBuf> {
    if let Ok(python_env) = std::env::var("PYTHON") {
        let candidate = PathBuf::from(python_env);
        if candidate.exists() {
            return Ok(candidate);
        }
    }
    which("python3")
        .or_else(|_| which("python"))
        .map_err(|_| {
            anyhow!(
                "No suitable Python interpreter found. Install `python3` and ensure it's on PATH, or set the PYTHON env var."
            )
        })
}

/// The plugin's `.venv`, created if missing, with its requirements.txt
/// installed.
fn prepare_python_env(py_dir: &Path) -> Result<PathBuf> {
    let venv_dir = py_dir.join(".venv");
    let py_bin = venv_dir.join("bin/python");

    if !py_bin.exists() {
        let host_python = find_host_python()?;
        Command::new(host_python)
            .arg("-m")
            .arg("venv")
            .arg(&venv_dir)
            .status()
            .context("creating venv")?;
    }

    // pip runs only when there is something to install, so a plugin with the
    // empty requirements.txt `plugin scaffold` writes compiles even where
    // .venv has no pip (Debian or Ubuntu without python3-venv).
    let reqs = py_dir.join("requirements.txt");
    let wants_packages = reqs.exists()
        && !fs::read_to_string(&reqs)
            .with_context(|| format!("reading {}", reqs.display()))?
            .trim()
            .is_empty();
    if wants_packages {
        let status = Command::new(&py_bin)
            .args(["-m", "pip", "install", "-r"])
            .arg(&reqs)
            .status()
            .context("installing Python requirements")?;
        if !status.success() {
            bail!(
                "`pip install -r {}` failed (exit: {:?})",
                reqs.display(),
                status.code()
            );
        }
    }
    Ok(venv_dir)
}

fn run_componentize_py(
    wit_path: &Path,
    world: &str,
    entry_point_path: &Path,
    out_component: &Path,
) -> anyhow::Result<()> {
    let py_dir = entry_point_path.parent().unwrap_or(Path::new("."));
    let venv_dir = prepare_python_env(py_dir)?;
    let app_module = file_stem(&entry_point_path)?;

    // componentize-py bundles the site-packages of `$VIRTUAL_ENV`, so it runs
    // as if `.venv` were activated: what requirements.txt installed there goes
    // into the component whether or not the shell that ran compile activated
    // it, and a componentize-py installed there is the one that runs.
    let inherited = std::env::var_os("PATH").unwrap_or_default();
    let path = std::env::join_paths(
        std::iter::once(venv_dir.join("bin")).chain(std::env::split_paths(&inherited)),
    )
    .context("building PATH for componentize-py")?;

    let status = Command::new("componentize-py")
        .current_dir(&py_dir)
        .env("VIRTUAL_ENV", &venv_dir)
        .env("PATH", path)
        .arg("--wit-path")
        .arg(wit_path)
        .arg("--world")
        .arg(world)
        .arg("componentize")
        .arg("--python-path")
        .arg(py_dir)
        .arg("--output")
        .arg(out_component)
        .arg(app_module)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .with_context(|| "running componentize-py")?;

    if !status.success() {
        bail!(
            "componentize-py failed for module `{}`",
            entry_point_path.display()
        );
    }
    Ok(())
}

fn run_go_compile(
    wit_path: &Path,
    world: &str,
    entry_point_path: &Path,
    out_component: &Path,
) -> Result<()> {
    ensure_tinygo()?;

    let gen = format!("{}/gen", go_sdk_module(entry_point_path));
    let status = Command::new("go")
        .current_dir(&entry_point_path)
        .arg("run")
        .arg(&gen)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .with_context(|| "running go gen")?;

    if !status.success() {
        bail!("`go run {gen}` failed (exit: {:?})", status.code());
    }

    let status = Command::new("tinygo")
        .current_dir(&entry_point_path)
        .arg("build")
        .arg("-target=wasip2")
        .arg("-opt=2")
        .arg("-scheduler=none")
        .arg("-o")
        .arg(&out_component)
        .arg("--wit-package")
        .arg(&wit_path)
        .arg("--wit-world")
        .arg(world)
        .arg("-no-debug")
        .arg(".")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .with_context(|| "running tinygo build -target=wasip2")?;

    if !status.success() {
        bail!("tinygo build failed");
    }

    // what tangentgen wrote for this build: T's JSON encoding and its schema
    for generated in ["json_generated.go", "schema_generated.go"] {
        let path = entry_point_path.join(generated);
        if std::fs::exists(&path)? {
            std::fs::remove_file(&path)?;
        }
    }

    Ok(())
}

const GO_SDK: &str = "github.com/ethanblackburn/tangent-sdk-go";
const GO_SDK_OLD: &str = "github.com/telophasehq/tangent-sdk-go";

/// The Go SDK's module path as the plugin's go.mod requires it. A plugin
/// written before the SDK left telophasehq requires the old path, and `go
/// run` finds only the module the plugin requires.
fn go_sdk_module(entry_point_path: &Path) -> &'static str {
    let go_mod = entry_point_path
        .ancestors()
        .map(|dir| dir.join("go.mod"))
        .find(|p| p.is_file())
        .and_then(|p| fs::read_to_string(p).ok())
        .unwrap_or_default();
    if go_mod.contains(GO_SDK_OLD) && !go_mod.contains(GO_SDK) {
        GO_SDK_OLD
    } else {
        GO_SDK
    }
}

fn run_rust_compile(entry_point_path: &Path, out_component: &Path) -> Result<()> {
    ensure_cargo_component()?;

    let manifest_path = if entry_point_path.is_dir() {
        entry_point_path.join("Cargo.toml")
    } else {
        entry_point_path.to_path_buf()
    };

    if !manifest_path.exists() {
        bail!(
            "`{}` does not exist; specify a Cargo.toml or a directory containing one",
            manifest_path.display()
        );
    }

    let manifest_dir = manifest_path
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."));

    let rustup_status = Command::new("rustup")
        .current_dir(&manifest_dir)
        .arg("target")
        .arg("add")
        .arg("wasm32-wasip2")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .with_context(|| "adding wasm32-wasip2 target")?;

    if !rustup_status.success() {
        bail!("rustup target add wasm32-wasip2 failed");
    }

    let pkg_name = package_name(&manifest_path)?;

    let status = Command::new("cargo")
        .current_dir(&manifest_dir)
        .arg("component")
        .arg("build")
        .arg("--release")
        .arg("--target")
        .arg("wasm32-wasip2")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .with_context(|| "running cargo component build")?;

    if !status.success() {
        bail!("cargo component build failed");
    }

    let artifact = manifest_dir
        .join("target")
        .join("wasm32-wasip2")
        .join("release")
        .join(format!("{pkg_name}.wasm"));

    if !artifact.exists() {
        bail!("expected build artifact at {}", artifact.display());
    }

    fs::copy(&artifact, out_component)
        .with_context(|| format!("copying {}", artifact.display()))?;

    Ok(())
}

fn file_stem(p: &Path) -> Result<String> {
    Ok(p.file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| anyhow!("bad filename: {}", p.display()))?
        .to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A crate gets the WIT written beside it only when its `cargo component`
    /// target reads `.tangent/wit`.
    #[test]
    fn a_crate_reads_the_plugin_wit_only_when_its_target_says_so() {
        let dir = tempfile::tempdir().unwrap();
        let manifest = dir.path().join("Cargo.toml");
        let with = |target: &str| {
            fs::write(&manifest, format!("[package]\nname = \"p\"\n{target}")).unwrap();
            reads_plugin_wit(&manifest).unwrap()
        };
        assert!(with(
            "\n[package.metadata.component.target]\npath = \".tangent/wit\"\n"
        ));
        assert!(!with(
            "\n[package.metadata.component.target]\npath = \"../wit\"\n"
        ));
        assert!(!with(""));
    }

    #[test]
    fn go_sdk_module_follows_the_plugins_go_mod() {
        let dir = tempfile::tempdir().unwrap();
        let plugin = dir.path().join("plugin");
        fs::create_dir(&plugin).unwrap();
        assert_eq!(go_sdk_module(&plugin), GO_SDK);

        let go_mod = dir.path().join("go.mod");
        fs::write(
            &go_mod,
            format!("module p\n\nrequire {GO_SDK_OLD} v0.0.0\n"),
        )
        .unwrap();
        assert_eq!(go_sdk_module(&plugin), GO_SDK_OLD);

        fs::write(&go_mod, format!("module p\n\nrequire {GO_SDK} v0.0.0\n")).unwrap();
        assert_eq!(go_sdk_module(&plugin), GO_SDK);
    }
}
