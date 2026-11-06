use serde::{Deserialize, Serialize};
use std::path::{Component, Path, PathBuf};

/// Records as whole files in a directory, a new file each time the WAL
/// seals one, as `s3` makes objects: a write is acked once it is in the
/// WAL's open file (synced only when sealed; a file left open by a stop
/// without flush is sealed at the next start), a WAL file is sealed by
/// `object_max_bytes` or `max_file_age_seconds`, and each sealed file
/// becomes one file in the directory, in the sink's `encoding`, named as
/// `s3` names its objects. `path` may hold strftime specifiers
/// (`lake/%Y/%m/%d/%H`), rendered in UTC when a write reaches the sink.
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RollingFileConfig {
    pub path: String,

    /// The WAL's own directory: no other sink's, and not in `path`, where a
    /// reader would find the files it stages (tangent will not start
    /// otherwise).
    pub wal_path: PathBuf,

    #[serde(default = "max_file_age_seconds")]
    pub max_file_age_seconds: u64,
}

const fn max_file_age_seconds() -> u64 {
    60
}

/// The directory below the sink's root that `path`'s specifiers rendered
/// to, for a write: relative, and never up.
pub fn subdir(prefix: &str) -> anyhow::Result<&Path> {
    let p = Path::new(prefix);
    if !p.components().all(|c| matches!(c, Component::Normal(_))) {
        anyhow::bail!(
            "directory {prefix:?}: a rolling_file sink wants a relative path, without `..`"
        );
    }
    Ok(p)
}
