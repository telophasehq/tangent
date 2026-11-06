//! What tangent writes for itself, kept per version: the packs built into it
//! (`include: [pack:aws]`), written out so that every reader of a file finds
//! one.

use anyhow::{bail, Result};
use std::ffi::OsString;
use std::path::PathBuf;

/// This tangent's cache: `$XDG_CACHE_HOME/tangent/<version>`, else
/// `$HOME/.cache/tangent/<version>`. A version has its own, so an upgrade
/// never reads what another version wrote.
pub fn dir() -> Result<PathBuf> {
    dir_from(|name| std::env::var_os(name))
}

/// [`dir`], with `var` for the environment. A variable that is unset, empty
/// or relative is not one (as the XDG spec says of a relative path).
fn dir_from(var: impl Fn(&str) -> Option<OsString>) -> Result<PathBuf> {
    let var = |name| var(name).map(PathBuf::from).filter(|p| p.is_absolute());
    let Some(cache) = var("XDG_CACHE_HOME").or_else(|| Some(var("HOME")?.join(".cache"))) else {
        bail!("tangent keeps its cache in $XDG_CACHE_HOME or $HOME/.cache, and neither variable is set to an absolute path");
    };
    Ok(cache.join("tangent").join(env!("CARGO_PKG_VERSION")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dir_with(vars: &[(&str, &str)]) -> Result<PathBuf> {
        dir_from(|name| {
            vars.iter()
                .find(|(n, _)| *n == name)
                .map(|(_, v)| OsString::from(v))
        })
    }

    #[test]
    fn the_cache_is_xdg_s_else_home_s_per_version() {
        let version = env!("CARGO_PKG_VERSION");
        let both = [("XDG_CACHE_HOME", "/x/cache"), ("HOME", "/home/me")];
        assert_eq!(
            dir_with(&both).unwrap(),
            PathBuf::from(format!("/x/cache/tangent/{version}"))
        );
        for xdg in ["", "relative/cache"] {
            assert_eq!(
                dir_with(&[("XDG_CACHE_HOME", xdg), ("HOME", "/home/me")]).unwrap(),
                PathBuf::from(format!("/home/me/.cache/tangent/{version}")),
                "XDG_CACHE_HOME={xdg:?}"
            );
        }
        for vars in [&[][..], &[("HOME", "")][..], &[("HOME", "me")][..]] {
            let e = dir_with(vars).unwrap_err().to_string();
            assert!(e.contains("XDG_CACHE_HOME") && e.contains("HOME"), "{e}");
        }
    }
}
