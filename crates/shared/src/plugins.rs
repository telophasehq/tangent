use ahash::HashMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::enrichment::TableConfig;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginConfig {
    pub module_type: String,

    /// The plugin's source, relative to the file that names it, for `tangent
    /// plugin compile`. A plugin with `prebuilt` and no source leaves it out.
    #[serde(default)]
    pub path: PathBuf,

    /// Where the plugin is published compiled, for a machine without the
    /// toolchain: `tangent plugin fetch` downloads the component from the
    /// release and checks GitHub's attestation of it before it is kept, in
    /// tangent's cache.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prebuilt: Option<Prebuilt>,

    #[serde(default)]
    pub tests: Vec<PluginTests>,

    #[serde(default)]
    pub config: HashMap<String, Value>,

    /// Namespace of this plugin's state. Unset, every plugin shares one
    /// (`plugins`); set it to give this plugin, or a group of plugins, keys
    /// of their own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state_namespace: Option<String>,

    /// A directory of files the plugin may read, relative to the file that names it,
    /// mounted read-only in the guest at `/data`. For reference data too big
    /// for `config`: an IP database, published address ranges, an org profile.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<PathBuf>,

    /// When a call fails (the guest traps or returns an error), its records
    /// are dropped; false passes them on unchanged, as if the plugin were
    /// not there.
    #[serde(default = "default_true")]
    pub drop_on_error: bool,

    /// Records reaching the plugin wait in a group per value of these
    /// fields, and the plugin gets a group at a time, on workers of its own.
    /// Empty: no grouping.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_by: Vec<String>,

    /// A group closes at this many records. Unset: only time closes it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_events: Option<usize>,

    /// A group closes once no record has joined it for this long, by the
    /// wall clock.
    #[serde(default = "default_expire_after_ms")]
    pub expire_after_ms: u64,

    /// A group closes this long after it opened, by the wall clock, however
    /// often records join it. Unset: only `max_events` and `expire_after_ms`
    /// close it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_age_ms: Option<u64>,

    /// How many of a key's latest records are kept in the state store and
    /// sent again, marked `"_context": true`, ahead of its next group.
    #[serde(default)]
    pub history: usize,
}

const fn default_true() -> bool {
    true
}

/// A group closes after 30 s with no new record.
const fn default_expire_after_ms() -> u64 {
    30_000
}

impl Default for PluginConfig {
    fn default() -> Self {
        Self {
            module_type: String::new(),
            path: PathBuf::new(),
            prebuilt: None,
            tests: Vec::new(),
            config: HashMap::default(),
            state_namespace: None,
            data: None,
            drop_on_error: true,
            group_by: Vec::new(),
            max_events: None,
            expire_after_ms: default_expire_after_ms(),
            max_age_ms: None,
            history: 0,
        }
    }
}

/// A compiled plugin on a GitHub release, with its build's attestation. The
/// component is the release asset `asset`, its attestation bundle the asset
/// `<asset>.sigstore.json` beside it; `tangent plugin fetch` downloads both
/// and has `gh attestation verify` check that `repo`'s workflow built the
/// component from the release's tag before it is precompiled into tangent's
/// cache ([`crate::cache::plugins`]).
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Prebuilt {
    /// The GitHub repository, `owner/name`, whose release has the component
    /// and whose workflow must have built it.
    pub repo: String,

    /// The release's tag. Unset, this tangent's own version (`v0.1.9`): the
    /// packs in this repository are released with it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub release: Option<String>,

    /// The asset's name. Unset, `<plugin>.component.wasm`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub asset: Option<String>,

    /// The workflow that must have signed it, as `.github/workflows/x.yml`.
    /// Unset, any workflow of `repo`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workflow: Option<String>,

    /// The component's SHA-256, hex, to pin it beyond its attestation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sha256: Option<String>,

    /// The GitHub host. Unset, `github.com`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host: Option<String>,
}

impl Prebuilt {
    pub fn host(&self) -> &str {
        self.host.as_deref().unwrap_or("github.com")
    }

    /// The release's tag: `release`, or `v` and `version` (this tangent's).
    pub fn release(&self, version: &str) -> String {
        self.release
            .clone()
            .unwrap_or_else(|| format!("v{version}"))
    }

    pub fn asset(&self, plugin: &str) -> String {
        self.asset
            .clone()
            .unwrap_or_else(|| format!("{plugin}.component.wasm"))
    }

    /// The URL of release asset `name`.
    pub fn asset_url(&self, version: &str, name: &str) -> String {
        format!(
            "https://{}/{}/releases/download/{}/{}",
            self.host(),
            self.repo,
            self.release(version),
            name
        )
    }

    /// `sha256` as lower-case hex without a `sha256:` prefix.
    pub fn pinned_sha256(&self) -> Option<String> {
        self.sha256.as_deref().map(|s| {
            s.trim()
                .strip_prefix("sha256:")
                .unwrap_or(s.trim())
                .to_ascii_lowercase()
        })
    }
}

/// The `.cwasm` plugin `name` runs: its local build ([`built`]) when there
/// is one, else the copy `tangent plugin fetch` keeps for its `prebuilt`
/// under `cache` ([`crate::cache::plugins`], see [`fetched`]).
pub fn compiled(
    name: &str,
    plugin: &PluginConfig,
    plugins_dir: &Path,
    cache: Option<&Path>,
) -> Option<PathBuf> {
    if let Some(built) = built(name, plugin, plugins_dir).filter(|b| b.is_file()) {
        return Some(built);
    }
    let cwasm = fetched(cache?, plugin.prebuilt.as_ref()?).join(format!("{name}.cwasm"));
    cwasm.is_file().then_some(cwasm)
}

/// Where `tangent plugin compile` puts plugin `name`'s build:
/// `plugins_dir/<name>.cwasm` (`runtime.plugins_path`, beside the config).
/// Only a plugin with a source (`path`) has one: this config cannot have
/// built one without, so a file there by that name (another config's, an
/// old fetch's) is not this plugin's.
pub fn built(name: &str, plugin: &PluginConfig, plugins_dir: &Path) -> Option<PathBuf> {
    (!plugin.path.as_os_str().is_empty()).then(|| plugins_dir.join(format!("{name}.cwasm")))
}

/// The directory under `cache` where `tangent plugin fetch` keeps what it
/// fetched for `prebuilt`, named by a hash of it: two configs that fetch
/// one plugin name from two places (another release, a fork, a pinned
/// digest) never write or read each other's. The hash is std's, the same
/// for every run of one tangent.
pub fn fetched(cache: &Path, prebuilt: &Prebuilt) -> PathBuf {
    let mut h = DefaultHasher::new();
    prebuilt.hash(&mut h);
    cache.join(format!("{:016x}", h.finish()))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginTests {
    pub input: PathBuf,
    pub expected: PathBuf,

    /// Tables this test reads in place of the config's of the same name: the
    /// data its expected output was written against, so the fixture means the
    /// same in every config that includes it.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub enrichment_tables: BTreeMap<Arc<str>, TableConfig>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_prebuilt_plugin_needs_no_source() {
        let p: PluginConfig = serde_yaml::from_str(
            "module_type: rust\nprebuilt: { repo: EthanBlackburn/tangent, workflow: .github/workflows/attest-packs.yml }\n",
        )
        .unwrap();
        assert!(p.path.as_os_str().is_empty());
        let pre = p.prebuilt.unwrap();
        assert_eq!(pre.release("0.1.10"), "v0.1.10");
        assert_eq!(pre.asset("aws"), "aws.component.wasm");
        assert_eq!(
            pre.asset_url("0.1.10", "aws.component.wasm"),
            "https://github.com/EthanBlackburn/tangent/releases/download/v0.1.10/aws.component.wasm"
        );
        assert_eq!(pre.pinned_sha256(), None);
    }

    #[test]
    fn a_prebuilt_names_its_release_asset_host_and_digest() {
        let pre: Prebuilt = serde_yaml::from_str(
            "repo: acme/packs\nrelease: packs-3\nasset: aws-pack.wasm\nhost: github.acme.com\nsha256: \"sha256:5B3B31\"\n",
        )
        .unwrap();
        assert_eq!(pre.release("0.1.10"), "packs-3");
        assert_eq!(pre.asset("aws"), "aws-pack.wasm");
        assert_eq!(
            pre.asset_url("0.1.10", "aws-pack.wasm"),
            "https://github.acme.com/acme/packs/releases/download/packs-3/aws-pack.wasm"
        );
        assert_eq!(pre.pinned_sha256().as_deref(), Some("5b3b31"));
    }

    const PACK: &str =
        "repo: EthanBlackburn/tangent\nworkflow: .github/workflows/attest-packs.yml\n";

    fn plugin(prebuilt: Option<&str>) -> PluginConfig {
        PluginConfig {
            module_type: "rust".into(),
            prebuilt: prebuilt.map(|p| serde_yaml::from_str(p).unwrap()),
            ..PluginConfig::default()
        }
    }

    /// `aws` as `tangent plugin fetch` keeps it, fetched as `prebuilt` says.
    fn fetch(cache: &Path, prebuilt: &str) -> PathBuf {
        let dir = fetched(cache, &serde_yaml::from_str(prebuilt).unwrap());
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("aws.cwasm"), "fetched").unwrap();
        dir.join("aws.cwasm")
    }

    #[test]
    fn a_local_build_wins_and_else_the_copy_fetched_for_this_prebuilt() {
        let dir = tempfile::tempdir().unwrap();
        let (local, cache) = (dir.path().join("plugins"), dir.path().join("cache"));
        let aws = PluginConfig {
            path: ".".into(),
            ..plugin(Some(PACK))
        };
        assert_eq!(compiled("aws", &aws, &local, Some(&cache)), None);

        let copy = fetch(&cache, PACK);
        assert_eq!(compiled("aws", &aws, &local, Some(&cache)), Some(copy));
        assert_eq!(compiled("aws", &aws, &local, None), None, "no cache");
        let source_only = PluginConfig {
            path: ".".into(),
            ..plugin(None)
        };
        assert_eq!(
            compiled("aws", &source_only, &local, Some(&cache)),
            None,
            "a plugin without `prebuilt` is compiled, never fetched"
        );

        std::fs::create_dir_all(&local).unwrap();
        std::fs::write(local.join("aws.cwasm"), "compiled").unwrap();
        for p in [&aws, &source_only] {
            assert_eq!(
                compiled("aws", p, &local, Some(&cache)),
                Some(local.join("aws.cwasm"))
            );
        }
    }

    /// A pack built into tangent names no source, so `tangent plugin
    /// compile` never built it: a plugins/aws.cwasm beside its config is
    /// another's, and the copy fetched for it runs.
    #[test]
    fn a_plugin_without_a_source_has_no_local_build() {
        let dir = tempfile::tempdir().unwrap();
        let (local, cache) = (dir.path().join("plugins"), dir.path().join("cache"));
        std::fs::create_dir_all(&local).unwrap();
        std::fs::write(local.join("aws.cwasm"), "stray").unwrap();
        let aws = plugin(Some(PACK));
        assert_eq!(built("aws", &aws, &local), None);
        assert_eq!(compiled("aws", &aws, &local, Some(&cache)), None);
        let copy = fetch(&cache, PACK);
        assert_eq!(compiled("aws", &aws, &local, Some(&cache)), Some(copy));
    }

    #[test]
    fn a_copy_fetched_from_elsewhere_is_not_this_plugins() {
        let dir = tempfile::tempdir().unwrap();
        let (local, cache) = (dir.path().join("plugins"), dir.path().join("cache"));
        let aws = plugin(Some(PACK));
        let mut dirs = vec![fetched(&cache, aws.prebuilt.as_ref().unwrap())];
        for other in [
            "repo: acme/packs\nworkflow: .github/workflows/attest-packs.yml",
            "repo: EthanBlackburn/tangent",
            &format!("{PACK}release: v0.1.8"),
            &format!("{PACK}asset: aws-pack.wasm"),
            &format!("{PACK}sha256: 5b3b31"),
            &format!("{PACK}host: github.acme.com"),
        ] {
            let copy = fetch(&cache, other);
            assert_eq!(compiled("aws", &aws, &local, Some(&cache)), None, "{other}");
            dirs.push(copy.parent().unwrap().to_path_buf());
        }
        dirs.sort();
        dirs.dedup();
        assert_eq!(dirs.len(), 7, "a directory for each `prebuilt`");
        assert_eq!(
            fetched(&cache, aws.prebuilt.as_ref().unwrap()),
            fetched(&cache, &serde_yaml::from_str(PACK).unwrap()),
            "one `prebuilt`, one directory"
        );
    }

    #[test]
    fn a_prebuilt_rejects_a_key_it_does_not_know() {
        let err =
            serde_yaml::from_str::<Prebuilt>("repo: acme/packs\nurl: https://x\n").unwrap_err();
        assert!(err.to_string().contains("url"), "{err}");
    }
}
