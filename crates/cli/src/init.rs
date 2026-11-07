//! `tangent init`: a tangent.yaml that replays the AWS account's CloudTrail
//! and has Claude Haiku 5.5 on Bedrock judge it, with the packs built in.

use anyhow::{bail, Context, Result};
use std::fs::OpenOptions;
use std::io::{ErrorKind, Write as _};
use std::path::Path;

/// The config `tangent init` writes.
const CONFIG: &str = include_str!("init.yaml");

/// Writes [`CONFIG`] to `dir`/tangent.yaml, unless a tangent.yaml is there:
/// the file is created only if missing, in one step, so one already there is
/// never written over. A write that fails removes the part written, which
/// is this call's alone, so a later init is not refused by it.
pub fn run_in(dir: &Path) -> Result<()> {
    let path = dir.join("tangent.yaml");
    let mut file = match OpenOptions::new().write(true).create_new(true).open(&path) {
        Ok(file) => file,
        Err(e) if e.kind() == ErrorKind::AlreadyExists => bail!(
            "tangent.yaml is already here; init leaves it as it is (edit it, or run tangent init in another directory)"
        ),
        Err(e) => return Err(e).with_context(|| format!("creating {}", path.display())),
    };
    if let Err(e) = file.write_all(CONFIG.as_bytes()) {
        let _ = std::fs::remove_file(&path);
        return Err(e).with_context(|| format!("writing {}", path.display()));
    }
    println!(
        "Wrote tangent.yaml. It reads CloudTrail and calls Bedrock as $AWS_PROFILE, or your default AWS profile when that is unset; to use another, set it on the line marked EDIT. Then:"
    );
    println!("  tangent replay --config tangent.yaml --from 1d");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tangent_shared::sources::common::SourceConfig;

    /// The source's profile and the judge's, as the config loads.
    fn profiles(path: &Path) -> (Option<String>, Option<String>) {
        let cfg = tangent_shared::Config::load(path).unwrap().config;
        cfg.validate().unwrap();
        let SourceConfig::Cloudtrail(source) = &cfg.sources["cloudtrail"] else {
            panic!("cloudtrail is not a cloudtrail source");
        };
        let judge = &cfg.inference["judge"].aws;
        assert_eq!(judge.region.as_deref(), Some("us-west-2"));
        (source.profile.clone(), judge.profile.clone())
    }

    /// The only test here that reads or sets AWS_PROFILE, so setting it
    /// races no other.
    #[test]
    fn writes_a_config_whose_source_and_judge_use_aws_profile_or_default() {
        let dir = tempfile::tempdir().unwrap();
        run_in(dir.path()).unwrap();
        let path = dir.path().join("tangent.yaml");
        assert_eq!(std::fs::read_to_string(&path).unwrap(), CONFIG);

        std::env::set_var("AWS_PROFILE", "work");
        let work = Some("work".to_string());
        assert_eq!(profiles(&path), (work.clone(), work), "the shell's profile");

        std::env::remove_var("AWS_PROFILE");
        let default = Some("default".to_string());
        assert_eq!(
            profiles(&path),
            (default.clone(), default),
            "unset is default"
        );
    }

    #[test]
    fn refuses_a_tangent_yaml_already_there_and_leaves_it_as_it_was() {
        let dir = tempfile::tempdir().unwrap();
        run_in(dir.path()).unwrap();
        let path = dir.path().join("tangent.yaml");
        let edited = CONFIG.replace("${AWS_PROFILE:-default}", "prod");
        assert_ne!(edited, CONFIG);
        std::fs::write(&path, &edited).unwrap();

        let err = format!("{:#}", run_in(dir.path()).unwrap_err());
        assert!(err.contains("tangent.yaml is already here"), "{err}");
        assert_eq!(std::fs::read(&path).unwrap(), edited.as_bytes());
    }
}
