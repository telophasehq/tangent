//! The packs built into tangent, which a config includes by name: `include:
//! [pack:aws]`. `pack:<dir>` is `packs/<dir>/ingest.yaml` and
//! `pack:<dir>/<file>` is `packs/<dir>/<file>.yaml`, as this tangent was
//! built with them, so an installed tangent needs no checkout.
//!
//! The files are written out under the cache ([`crate::cache::dir`]) in
//! packs/'s layout, so a pack's paths (`prompt_file: judge.md`, `include:
//! [../enrich-ip/ingest.yaml]`) name what they name in the repo, and whatever
//! reads one reads it from disk as it always has. A plugin's sources and
//! fixtures are not built in, nor anything outside packs/: a pack's path to
//! one names nothing in the cache.

use anyhow::{anyhow, bail, Context, Result};
use std::fs;
use std::io::{self, Write as _};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

/// What starts an `include` entry that names a pack built in.
pub const PREFIX: &str = "pack:";

macro_rules! files {
    ($($f:literal),* $(,)?) => {
        &[$(($f, include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../packs/", $f)))),*]
    };
}

/// Every file of packs/ built into tangent, by its path there, and its text:
/// each pack's YAML, and each file one names but its plugins' sources and
/// fixtures. The tests below fail when a file is left out.
pub const FILES: &[(&str, &str)] = files![
    "enrich-ip/ingest.yaml",
    "behavior-baseline/ingest.yaml",
    "judge/ingest.yaml",
    "judge/hosted.yaml",
    "judge/judge.md",
    "judge/judge.schema.json",
    "aws/ingest.yaml",
    "github/ingest.yaml",
    "okta/ingest.yaml",
    "google_workspace/ingest.yaml",
];

/// A file's name after `pack:`: `judge/hosted.yaml` is `judge/hosted`, and a
/// pack's ingest.yaml is the pack's own, `aws/ingest.yaml` `aws`. A file that
/// is not YAML has none.
fn name_of(file: &str) -> Option<&str> {
    let stem = file.strip_suffix(".yaml")?;
    Some(stem.strip_suffix("/ingest").unwrap_or(stem))
}

/// The file a name after `pack:` is, by its path under packs/.
fn path_of(name: &str) -> Option<&'static str> {
    FILES
        .iter()
        .map(|(f, _)| *f)
        .find(|f| name_of(f) == Some(name))
}

/// Every name, as `include` writes it, sorted.
pub fn names() -> Vec<String> {
    let mut names: Vec<String> = FILES
        .iter()
        .filter_map(|(f, _)| name_of(f))
        .map(|n| format!("{PREFIX}{n}"))
        .collect();
    names.sort();
    names
}

/// The copy of the pack file `name` (after `pack:`) names, written out with
/// every other file the first time one is asked for.
pub fn file(name: &str) -> Result<PathBuf> {
    let Some(rel) = path_of(name) else {
        bail!(
            "no such pack in tangent {}; it has {}",
            env!("CARGO_PKG_VERSION"),
            names().join(", ")
        );
    };
    Ok(root()?.join(rel))
}

/// The name of `path` (canonical) when it is a pack file this process wrote
/// out, so a message says `pack:judge` rather than a cache path.
pub fn name(path: &Path) -> Option<String> {
    let Some(Ok(root)) = ROOT.get() else {
        return None;
    };
    let rel = path.strip_prefix(root).ok()?.to_str()?;
    let name = name_of(rel).filter(|n| path_of(n) == Some(rel))?;
    Some(format!("{PREFIX}{name}"))
}

/// Where this build's packs are written: `packs-<digest>` under the cache,
/// the digest of [`FILES`], so two builds of one version whose packs differ
/// (two branches, a release and a checkout) never write over each other's.
pub fn dir() -> Result<PathBuf> {
    Ok(crate::cache::dir()?.join(format!("packs-{:016x}", digest(FILES))))
}

/// FNV-1a, 64 bits, over each file's path and text, each ended by 0xff,
/// which UTF-8 never has: one list of files is one stream of bytes.
fn digest(files: &[(&str, &str)]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for (rel, text) in files {
        for part in [rel.as_bytes(), text.as_bytes()] {
            for &b in part.iter().chain(&[0xff]) {
                h = (h ^ u64::from(b)).wrapping_mul(0x0000_0100_0000_01b3);
            }
        }
    }
    h
}

/// Where the files were written: [`dir`], canonical. Written once per
/// process.
static ROOT: OnceLock<Result<PathBuf, String>> = OnceLock::new();

fn root() -> Result<PathBuf> {
    ROOT.get_or_init(|| {
        let written = dir().and_then(|root| {
            write_out(&root, FILES)?;
            root.canonicalize()
                .with_context(|| format!("reading {}", root.display()))
        });
        written.map_err(|e| format!("{e:#}"))
    })
    .clone()
    .map_err(|e| anyhow!(e))
}

/// `files` under `root` in packs/'s layout, each one not already there as
/// built in: a file edited by hand goes back to the binary's text.
fn write_out(root: &Path, files: &[(&str, &str)]) -> Result<()> {
    files
        .iter()
        .try_for_each(|(rel, text)| write(&root.join(rel), text))
        .with_context(|| {
            format!(
                "writing tangent's built-in packs under {}; set XDG_CACHE_HOME to a writable directory",
                root.display()
            )
        })
}

/// `text` at `path`, unless it is there already, through a temporary file
/// of this writer's own and a rename: a reader never sees half of one.
fn write(path: &Path, text: &str) -> Result<()> {
    if fs::read(path).is_ok_and(|have| have == text.as_bytes()) {
        return Ok(());
    }
    let dir = path.parent().unwrap_or(Path::new("/"));
    fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    let (tmp, mut file) = temp(path)?;
    file.write_all(text.as_bytes())
        .with_context(|| format!("writing {}", tmp.display()))?;
    drop(file);
    fs::rename(&tmp, path).with_context(|| format!("writing {}", path.display()))
}

/// A new file beside `path`, created here and opened by no one else: the
/// pid alone is not enough, since two containers sharing a cache both run
/// tangent as pid 1, so a name another writer has taken is passed over.
fn temp(path: &Path) -> Result<(PathBuf, fs::File)> {
    for attempt in 0..1000 {
        let tmp = temp_name(path, attempt);
        match fs::File::options().write(true).create_new(true).open(&tmp) {
            Ok(file) => return Ok((tmp, file)),
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(e) => return Err(e).with_context(|| format!("writing {}", tmp.display())),
        }
    }
    bail!("writing {}: every temporary name is taken", path.display())
}

/// `<file>.<pid>.<attempt>.tmp`, the whole file name kept: judge.md's and
/// judge.schema.json's differ.
fn temp_name(path: &Path, attempt: u32) -> PathBuf {
    let mut name = path.file_name().unwrap_or_default().to_os_string();
    name.push(format!(".{}.{attempt}.tmp", std::process::id()));
    path.with_file_name(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::include;
    use serde_yaml::Value;
    use std::collections::BTreeSet;
    use std::time::{Duration, SystemTime};

    fn packs() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../packs")
            .canonicalize()
            .unwrap()
    }

    fn built_in(rel: &str) -> bool {
        FILES.iter().any(|(f, _)| *f == rel)
    }

    /// Each pack's YAML, packs/<dir>/<file>.yaml, can be included by name.
    /// test.yaml (behavior-baseline's story) and deeper files
    /// (tests/profile.yaml) are a plugin's tests.
    #[test]
    fn every_pack_yaml_is_built_in() {
        let mut left_out = Vec::new();
        for dir in fs::read_dir(packs()).unwrap().flatten() {
            if !dir.path().is_dir() {
                continue;
            }
            for f in fs::read_dir(dir.path()).unwrap().flatten() {
                let base = f.file_name().to_string_lossy().into_owned();
                let rel = format!("{}/{base}", dir.file_name().to_string_lossy());
                let yaml = base.ends_with(".yaml") || base.ends_with(".yml");
                if yaml && base != "test.yaml" && !built_in(&rel) {
                    left_out.push(rel);
                }
            }
        }
        assert!(
            left_out.is_empty(),
            "pack files not in packs.rs's FILES: {left_out:?}"
        );
    }

    /// The values under `pattern` (as in include::PATHS) that are strings.
    fn strings_at(v: &Value, pattern: &[&str], out: &mut Vec<String>) {
        match (pattern.split_first(), v) {
            (None, Value::String(s)) => out.push(s.clone()),
            (Some((&"[]", rest)), Value::Sequence(items)) => {
                for item in items {
                    strings_at(item, rest, out);
                }
            }
            (Some((&"*", rest)), Value::Mapping(m)) => {
                for child in m.values() {
                    strings_at(child, rest, out);
                }
            }
            (Some((key, rest)), Value::Mapping(m)) => {
                if let Some(child) = m.get(*key) {
                    strings_at(child, rest, out);
                }
            }
            _ => {}
        }
    }

    /// A file a built-in pack names under packs/, a prompt or a file it
    /// includes, is built in beside it, or the copy would name nothing. A
    /// plugin's `path` and `tests` are its sources and fixtures, which are
    /// not.
    #[test]
    fn every_file_a_pack_names_is_built_in() {
        let packs = packs();
        let mut named: BTreeSet<String> = BTreeSet::new();
        let mut left_out = Vec::new();
        for (rel, text) in FILES.iter().filter(|(f, _)| f.ends_with(".yaml")) {
            let Value::Mapping(mut map) = serde_yaml::from_str(text).unwrap() else {
                panic!("{rel}: not a map");
            };
            let dir = packs.join(rel).parent().unwrap().to_path_buf();
            if let Some(Value::Mapping(plugins)) = map.get_mut("plugins") {
                for p in plugins.values_mut().filter_map(Value::as_mapping_mut) {
                    p.remove("path");
                    p.remove("tests");
                }
            }
            include::rebase(&mut map, &dir);
            let mut paths = Vec::new();
            let map = Value::Mapping(map);
            for pattern in include::PATHS {
                strings_at(&map, pattern, &mut paths);
            }
            for inc in map
                .get("include")
                .and_then(Value::as_sequence)
                .into_iter()
                .flatten()
            {
                let inc = inc.as_str().unwrap();
                match inc.strip_prefix(PREFIX) {
                    Some(name) => assert!(path_of(name).is_some(), "{rel} includes {inc}"),
                    None => {
                        let joined = dir.join(inc);
                        paths.push(
                            joined
                                .canonicalize()
                                .unwrap_or(joined)
                                .display()
                                .to_string(),
                        );
                    }
                }
            }
            for p in paths {
                let Ok(under) = Path::new(&p).strip_prefix(&packs) else {
                    continue;
                };
                let under = under.to_string_lossy().into_owned();
                if !built_in(&under) {
                    left_out.push(format!("{rel} names {under}"));
                }
                named.insert(under);
            }
        }
        assert!(
            left_out.is_empty(),
            "files a pack names, not in packs.rs's FILES: {left_out:?}"
        );
        // the walk finds what the packs name
        for f in [
            "judge/judge.md",
            "judge/judge.schema.json",
            "judge/ingest.yaml",
            "enrich-ip/ingest.yaml",
            "behavior-baseline/ingest.yaml",
        ] {
            assert!(named.contains(f), "{f} not found among {named:?}");
        }
    }

    #[test]
    fn a_pack_name_is_its_file() {
        assert_eq!(path_of("enrich-ip"), Some("enrich-ip/ingest.yaml"));
        assert_eq!(
            path_of("behavior-baseline"),
            Some("behavior-baseline/ingest.yaml")
        );
        assert_eq!(path_of("judge"), Some("judge/ingest.yaml"));
        assert_eq!(path_of("judge/hosted"), Some("judge/hosted.yaml"));
        assert_eq!(path_of("aws"), Some("aws/ingest.yaml"));
        assert_eq!(
            path_of("google_workspace"),
            Some("google_workspace/ingest.yaml")
        );
        for unknown in [
            "awss",
            "",
            "judge/ingest",
            "judge/hosted.yaml",
            "judge/judge.md",
            "aws/",
            "/aws",
        ] {
            assert_eq!(path_of(unknown), None, "{unknown:?}");
        }
        let all = [
            "pack:aws",
            "pack:behavior-baseline",
            "pack:enrich-ip",
            "pack:github",
            "pack:google_workspace",
            "pack:judge",
            "pack:judge/hosted",
            "pack:okta",
        ];
        assert_eq!(names(), all);
        // an unknown name writes nothing: it fails first
        assert_eq!(
            file("awss").unwrap_err().to_string(),
            format!(
                "no such pack in tangent {}; it has {}",
                env!("CARGO_PKG_VERSION"),
                all.join(", ")
            )
        );
    }

    /// The files go out as built in, and again when one is missing or its
    /// text differs (edited by hand); one as built in is left as it is.
    #[test]
    fn a_changed_file_is_written_again() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("packs");
        write_out(&root, FILES).unwrap();
        for (rel, text) in FILES {
            assert_eq!(fs::read_to_string(root.join(rel)).unwrap(), *text, "{rel}");
        }

        let changed = root.join("judge/judge.md");
        let missing = root.join("judge/judge.schema.json");
        let same = root.join("aws/ingest.yaml");
        fs::write(&changed, "a brief edited by hand").unwrap();
        fs::remove_file(&missing).unwrap();
        let then = SystemTime::UNIX_EPOCH + Duration::from_secs(1_000_000_000);
        fs::File::options()
            .write(true)
            .open(&same)
            .unwrap()
            .set_modified(then)
            .unwrap();

        write_out(&root, FILES).unwrap();
        let text = |rel: &str| FILES.iter().find(|(f, _)| *f == rel).unwrap().1;
        assert_eq!(fs::read_to_string(&changed).unwrap(), text("judge/judge.md"));
        assert_eq!(
            fs::read_to_string(&missing).unwrap(),
            text("judge/judge.schema.json")
        );
        assert_eq!(fs::metadata(&same).unwrap().modified().unwrap(), then);
        // and no temporary file is left beside them
        let judge: Vec<String> = fs::read_dir(root.join("judge"))
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .filter(|n| n.ends_with(".tmp"))
            .collect();
        assert!(judge.is_empty(), "{judge:?}");
    }

    /// Two builds of one version whose packs differ, a file's text or its
    /// name, write to two directories, so neither reads the other's files;
    /// one build's packs always go to one.
    #[test]
    fn packs_that_differ_have_a_directory_each() {
        let mut edited = FILES.to_vec();
        edited[4].1 = "a brief another branch changed";
        let mut renamed = FILES.to_vec();
        renamed[4].0 = "judge/brief.md";
        assert_ne!(digest(FILES), digest(&edited));
        assert_ne!(digest(FILES), digest(&renamed));
        // where a path ends and its text begins counts
        assert_ne!(digest(&[("a", "bc")]), digest(&[("ab", "c")]));
        assert_ne!(digest(&[("a", ""), ("b", "")]), digest(&[("a", "b")]));

        let dir = dir().unwrap();
        let name = dir.file_name().unwrap().to_str().unwrap();
        assert_eq!(name, format!("packs-{:016x}", digest(FILES)));
        assert_eq!(dir.parent().unwrap(), crate::cache::dir().unwrap());
    }

    /// A writer writes through a temporary file no other writer has opened:
    /// one another process left under the name this one would try first
    /// (the same pid, in another container) is neither cut short nor
    /// renamed into place.
    #[test]
    fn a_writer_never_writes_through_another_s_temporary_file() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("packs");
        let half = "another writer's half";
        for (rel, _) in FILES {
            let path = root.join(rel);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(temp_name(&path, 0), half).unwrap();
        }
        write_out(&root, FILES).unwrap();
        for (rel, text) in FILES {
            let path = root.join(rel);
            assert_eq!(fs::read_to_string(&path).unwrap(), *text, "{rel}");
            assert_eq!(fs::read_to_string(temp_name(&path, 0)).unwrap(), half);
        }
        // judge.md and judge.schema.json, in one directory, never share one
        assert_ne!(
            temp_name(&root.join("judge/judge.md"), 0),
            temp_name(&root.join("judge/judge.schema.json"), 0)
        );
    }

    /// A cache tangent cannot write says which variable moves it.
    #[cfg(unix)]
    #[test]
    fn an_unwritable_cache_says_how_to_move_it() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let locked = dir.path().join("locked");
        fs::create_dir(&locked).unwrap();
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o555)).unwrap();
        // root writes anywhere, so there is nothing to see
        if fs::write(locked.join("probe"), "").is_ok() {
            return;
        }
        let root = locked.join("packs");
        let e = format!("{:#}", write_out(&root, FILES).unwrap_err());
        fs::set_permissions(&locked, fs::Permissions::from_mode(0o755)).unwrap();
        assert!(
            e.starts_with(&format!(
                "writing tangent's built-in packs under {}; set XDG_CACHE_HOME to a writable directory: creating ",
                root.display()
            )),
            "{e}"
        );
    }
}
