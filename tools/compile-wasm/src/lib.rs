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
}

pub fn compile_from_config(cfg_path: impl AsRef<Path>) -> Result<()> {
    ensure_componentize_available()?;

    let bytes =
        fs::read(&cfg_path).with_context(|| format!("reading {}", cfg_path.as_ref().display()))?;
    let cfg: Config = serde_yaml::from_slice(&bytes)
        .with_context(|| format!("parsing YAML {}", cfg_path.as_ref().display()))?;

    let config_dir = cfg_path.as_ref().parent().unwrap_or_else(|| Path::new("."));
    let entry_point_path = config_dir.join(&cfg.entry_point);

    fs::create_dir_all(&OUT_DIR)?;

    // Build only the entry point
    let stem = file_stem(&entry_point_path)?;
    let module_name = stem.clone();
    let out = PathBuf::from(format!("{OUT_DIR}/app.component.wasm"));
    let py_dir = entry_point_path.parent().unwrap_or(Path::new("."));
    let wit_path = Path::new("wit");
    run_componentize_py(&wit_path, WORLD, &module_name, py_dir, &out)?;

    println!("✅ Compiled {} → {}", cfg.entry_point, OUT_DIR);
    println!("   Entry: app.component.wasm");
    Ok(())
}

fn ensure_componentize_available() -> Result<()> {
    which("componentize-py").map(|_| ()).map_err(|_| {
        anyhow!("`componentize-py` not found in PATH. Install with: python3.11 -m pip install componentize-py")
    })
}

fn run_componentize_py(
    wit_path: &Path,
    world: &str,
    app_module: &str,
    py_dir: &Path,
    out_component: &Path,
) -> anyhow::Result<()> {
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
        bail!("componentize-py failed for module `{app_module}`");
    }
    Ok(())
}

fn file_stem(p: &Path) -> Result<String> {
    Ok(p.file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| anyhow!("bad filename: {}", p.display()))?
        .to_string())
}
