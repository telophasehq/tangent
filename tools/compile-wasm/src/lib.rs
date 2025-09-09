use anyhow::{Context, Result, anyhow, bail};
use dunce::canonicalize;
use serde::Deserialize;
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};
use walkdir::WalkDir;
use which::which;

const OUT_DIR: &str = "compiled";

#[derive(Debug, Deserialize)]
pub struct Config {
    pub modules: Vec<PathBuf>,
    pub modules_dir: Option<PathBuf>,
    pub entry_point: String,
}

pub fn compile_from_config(cfg_path: impl AsRef<Path>) -> Result<()> {
    ensure_py2wasm_available()?;

    let bytes =
        fs::read(&cfg_path).with_context(|| format!("reading {}", cfg_path.as_ref().display()))?;
    let cfg: Config = serde_yaml::from_slice(&bytes)
        .with_context(|| format!("parsing YAML {}", cfg_path.as_ref().display()))?;

    let mut files: Vec<PathBuf> = Vec::new();
    if !cfg.modules.is_empty() && cfg.modules_dir.is_some() {
        bail!("Specify either `modules` or `modules_dir`, not both.");
    }

    let config_dir = cfg_path.as_ref().parent().unwrap_or_else(|| Path::new("."));

    if !cfg.modules.is_empty() {
        for p in &cfg.modules {
            let resolved_path = if p.is_absolute() {
                p.clone()
            } else {
                config_dir.join(p)
            };
            match canonicalize(&resolved_path) {
                Ok(path) => {
                    ensure_py(&path)?;
                    files.push(path);
                }
                Err(message) => {
                    bail!("Error with path: {message}");
                }
            }
        }
    } else if let Some(dir) = &cfg.modules_dir {
        let root = canonicalize(dir).unwrap_or_else(|_| dir.clone());
        for e in WalkDir::new(&root).into_iter().filter_map(Result::ok) {
            let p = e.path();
            if p.is_file() && p.extension().and_then(|s| s.to_str()) == Some("py") {
                files.push(p.to_path_buf());
            }
        }
        if files.is_empty() {
            bail!("No *.py files found under {}", root.display());
        }
    } else {
        bail!("You must set either `modules` or `modules_dir` in config.");
    }

    fs::create_dir_all(&OUT_DIR).with_context(|| format!("creating {}", OUT_DIR))?;

    // Build all in parallel
    files.iter().try_for_each(|py| {
        let stem = file_stem(py)?;
        let out = PathBuf::from(format!("{OUT_DIR}/{stem}.wasm"));
        run_py2wasm(py, &out)
    })?;

    // Copy entry to app.wasm for convenience
    let ep_stem = entry_stem(&cfg.entry_point)?;
    let entry_wasm = PathBuf::from(format!("{OUT_DIR}/{ep_stem}.wasm"));
    if !entry_wasm.exists() {
        bail!(
            "Entry wasm not found at {} (did you include the entry module in the set?)",
            entry_wasm.display()
        );
    }
    fs::copy(&entry_wasm, format!("{OUT_DIR}/app.wasm"))
        .with_context(|| format!("copy {} -> app.wasm", entry_wasm.display()))?;

    println!("✅ Compiled {} modules → {}", files.len(), OUT_DIR);
    println!("   Entry: {}  (also at app.wasm)", ep_stem);
    Ok(())
}

fn ensure_py2wasm_available() -> Result<()> {
    which("py2wasm").map(|_| ()).map_err(|_| {
        anyhow!("`py2wasm` not found in PATH. Install with: python3.11 -m pip install py2wasm")
    })
}

fn run_py2wasm(py: &Path, out_wasm: &Path) -> Result<()> {
    println!("{py:?}");
    let status = Command::new("py2wasm")
        .arg(py)
        .arg("-o")
        .arg(out_wasm)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .with_context(|| format!("running py2wasm on {}", py.display()))?;

    if !status.success() {
        bail!("py2wasm failed for {}", py.display());
    }
    Ok(())
}

fn ensure_py(p: &Path) -> Result<()> {
    match p.extension().and_then(|s| s.to_str()) {
        Some("py") => Ok(()),
        _ => bail!("Not a .py file: {}", p.display()),
    }
}

fn file_stem(p: &Path) -> Result<String> {
    Ok(p.file_stem()
        .and_then(|s| s.to_str())
        .ok_or_else(|| anyhow!("bad filename: {}", p.display()))?
        .to_string())
}

fn entry_stem(s: &str) -> Result<String> {
    let p = Path::new(s);
    if p.extension().and_then(|e| e.to_str()) == Some("py") {
        Ok(p.file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or(s)
            .to_string())
    } else {
        Ok(s.to_string())
    }
}
