use anyhow::{anyhow, bail, Context, Result};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};
use tangent_shared::Config;
use which::which;

const WORLD: &str = "processor";

pub fn compile_from_config(cfg_path: &PathBuf, wit_path: &PathBuf) -> Result<()> {
    let cfg = Config::from_file(cfg_path)?;

    let config_dir = cfg_path.parent().unwrap_or_else(|| Path::new("."));
    let out = config_dir.join(&cfg.runtime.plugins_path).canonicalize()?;
    fs::create_dir_all(&out)?;

    for (name, plugin) in cfg.plugins {
        let entry_point_path = config_dir.join(&plugin.path).canonicalize()?;
        println!("⚙️ Compiling {}", entry_point_path.display());

        let full_out = &out.join(format!("{}.component.wasm", name));

        match plugin.module_type.as_str() {
            "py" => run_componentize_py(&wit_path, WORLD, &entry_point_path, &full_out)?,
            "go" => run_go_compile(&wit_path, WORLD, &entry_point_path, &full_out)?,
            ext => anyhow::bail!(
                "unsupported filetype: {} for wasm entrypoint: {}",
                ext,
                entry_point_path.display()
            ),
        }

        println!(
            "✅ Compiled {} → {}",
            entry_point_path.display(),
            full_out.display()
        );
    }
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
        .current_dir(&entry_point_path)
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
        .arg("-x")
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
