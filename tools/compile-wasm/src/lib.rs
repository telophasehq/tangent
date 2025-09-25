use anyhow::{Context, Result, anyhow, bail};
use serde::Deserialize;
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};
use which::which;

const OUT_DIR: &str = "compiled";
const WORLD: &str = "processor";

#[derive(Debug, Deserialize)]
pub struct Config {
    pub entry_point: String,
    pub module_type: String,
}

pub fn compile_from_config(cfg_path: &PathBuf, wit_path: &PathBuf) -> Result<()> {
    let bytes = fs::read(&cfg_path).with_context(|| format!("reading {}", cfg_path.display()))?;
    let cfg: Config = serde_yaml::from_slice(&bytes)
        .with_context(|| format!("parsing YAML {}", cfg_path.display()))?;

    let config_dir = cfg_path.parent().unwrap_or_else(|| Path::new("."));
    let entry_point_path = config_dir.join(&cfg.entry_point);

    fs::create_dir_all(&OUT_DIR)?;

    // Build only the entry point
    let out = PathBuf::from(format!("{OUT_DIR}/app.component.wasm"));

    match cfg.module_type.as_str() {
        "py" => run_componentize_py(&wit_path, WORLD, &entry_point_path, &out)?,
        "go" => run_go_compile(&wit_path, WORLD, &entry_point_path, &out)?,
        ext => anyhow::bail!(
            "unsupported filetype: {} for wasm entrypoint: {}",
            ext,
            entry_point_path.display()
        ),
    }

    println!("✅ Compiled {} → {}", cfg.entry_point, OUT_DIR);
    println!("   Entry: app.component.wasm");
    Ok(())
}

fn ensure_componentize_available() -> Result<()> {
    which("componentize-py").map(|_| ()).map_err(|_| {
        anyhow!("`componentize-py` not found in PATH. Install with: python -m pip install componentize-py")
    })
}

fn ensure_tinygo() -> Result<()> {
    which("tinygo")
        .map(|_| ())
        .map_err(|_| anyhow!("`tinygo` not found in PATH. Install directions: https://tinygo.org/getting-started/install/"))
}

fn run_componentize_py(
    wit_path: &Path,
    world: &str,
    entry_point_path: &Path,
    out_component: &Path,
) -> anyhow::Result<()> {
    ensure_componentize_available()?;

    let py_dir = entry_point_path.parent().unwrap_or(Path::new("."));
    let stem = file_stem(&entry_point_path)?;
    let app_module = stem.clone();
    let status = Command::new("componentize-py")
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

    let wasm_out = out_component.with_file_name("app.component.wasm");
    let status = Command::new("tinygo")
        .arg("build")
        .arg("-x")
        .arg("-target=wasip2")
        .arg("-o")
        .arg(&wasm_out)
        .arg("--wit-package")
        .arg(&wit_path)
        .arg("--wit-world")
        .arg(world)
        .arg("-no-debug")
        .arg(entry_point_path)
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
