use ahash::HashMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::PathBuf;
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
    /// release and checks GitHub's attestation of it before it is used.
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
    /// fields, and the plugin gets a group at a time, on workers of its own:
    /// Vector's `reduce`, without the merge. Empty: no grouping.
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

/// Vector's `reduce` default.
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
/// component from the release's tag before it is precompiled into
/// `runtime.plugins_path`.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

    #[test]
    fn a_prebuilt_rejects_a_key_it_does_not_know() {
        let err =
            serde_yaml::from_str::<Prebuilt>("repo: acme/packs\nurl: https://x\n").unwrap_err();
        assert!(err.to_string().contains("url"), "{err}");
    }
}
