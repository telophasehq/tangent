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

pub fn compile_from_config(cfg_path: &PathBuf, wit_path: &PathBuf) -> Result<()> {
    let cfg = Config::from_file(cfg_path)?;

    let config_dir = cfg_path.parent().unwrap_or_else(|| Path::new("."));

    let plugins_path = config_dir.join(&cfg.runtime.plugins_path);
    fs::create_dir_all(&plugins_path)?;
    let out = plugins_path
        .canonicalize()
        .with_context(|| "configured plugins path")?;

    for (name, plugin) in cfg.plugins {
        let entry_point_path = config_dir
            .join(&plugin.path)
            .canonicalize()
            .with_context(|| "configured plugin path")?;
        println!("⚙️ Compiling {}", entry_point_path.display());

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

        let engine = tangent_shared::wasm_engine::build()?;
        let c = Component::from_file(&engine, full_out)?;
        let bytes = c.serialize()?;

        let cwasm_out = &out.join(format!("{name}.cwasm"));
        std::fs::write(cwasm_out, bytes)?;

        println!(
            "✅ Compiled {} → {}",
            entry_point_path.display(),
            cwasm_out.display()
        );
    }
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
            "`cargo component` not found. Install via `cargo install cargo-component` and ensure wasm32-wasi target is installed with `rustup target add wasm32-wasi`."
        ),
    }
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

    let reqs = py_dir.join("requirements.txt");
    if reqs.exists() {
        Command::new(&py_bin)
            .args(["-m", "pip", "install", "-r"])
            .arg(&reqs)
            .status()
            .context("installing Python requirements")?;
    }
    Ok(py_bin)
}

fn run_componentize_py(
    wit_path: &Path,
    world: &str,
    entry_point_path: &Path,
    out_component: &Path,
) -> anyhow::Result<()> {
    let py_dir = entry_point_path.parent().unwrap_or(Path::new("."));
    let _ = prepare_python_env(py_dir)?;
    let app_module = file_stem(&entry_point_path)?;

    let status = Command::new("componentize-py")
        .current_dir(&py_dir)
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

    let status = Command::new("go")
        .current_dir(&entry_point_path)
        .arg("run")
        .arg("github.com/telophasehq/tangent-sdk-go/gen")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .with_context(|| "running go gen")?;

    if !status.success() {
        bail!(
            "`go run github.com/telophasehq/tangent-sdk-go/gen` failed (exit: {:?})",
            status.code()
        );
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

    let json_generated = entry_point_path.join("json_generated.go");
    if std::fs::exists(&json_generated)? {
        std::fs::remove_file(&json_generated)?;
    }

    Ok(())
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
