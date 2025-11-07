use anyhow::{anyhow, bail, Context, Result};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};
use tangent_shared::Config;
use wasmtime::component::Component;
use which::which;

const WORLD: &str = "processor";

pub fn compile_from_config(cfg_path: &PathBuf, wit_path: &PathBuf) -> Result<()> {
    let cfg = Config::from_file(cfg_path)?;

    let config_dir = cfg_path.parent().unwrap_or_else(|| Path::new("."));
    let out = config_dir
        .join(&cfg.runtime.plugins_path)
        .canonicalize()
        .with_context(|| "configured plugins path")?;
    fs::create_dir_all(&out)?;

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

fn ensure_tinygo() -> Result<()> {
    which("tinygo")
        .map(|_| ())
        .map_err(|_| anyhow!("`tinygo` not found in PATH. Install directions: https://tinygo.org/getting-started/install/"))
}

fn prepare_python_env(py_dir: &Path) -> Result<PathBuf> {
    let venv_dir = py_dir.join(".venv");
    let py_bin = venv_dir.join("bin/python");

    if !py_bin.exists() {
        Command::new("python")
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

    let comp = py_dir.join(".venv/bin/componentize-py");
    let status = Command::new(comp)
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

    Ok(())
}

fn file_stem(p: &Path) -> Result<String> {
    Ok(p.file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| anyhow!("bad filename: {}", p.display()))?
        .to_string())
}
