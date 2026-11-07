//! `tangent plugin fetch`: each plugin's compiled component from the release
//! its `prebuilt` names, checked against GitHub's attestation of the build,
//! and precompiled into `runtime.plugins_path`, where `tangent plugin
//! compile` puts it and `tangent run` reads it. Only a pack's author needs
//! the WASM toolchain: a component built once runs on every machine.
//!
//! The check is `gh attestation verify`, GitHub's own verifier: the
//! component's attestation (a Sigstore bundle, the asset beside it on the
//! release) must be signed by `repo`'s workflow, for the release's tag, and
//! name the component's digest. With the bundle on the release nothing asks
//! GitHub's API, so no login is needed. The statement must also name the
//! file as the asset asked for, not only carry its digest, so one attested
//! component of a release cannot be passed off as another. There is no way
//! to skip the check: a plugin reads every record and this one came off the
//! network.

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{anyhow, bail, Context, Result};
use serde_json::Value;
use sha2::{Digest, Sha256};
use tangent_shared::plugins::Prebuilt;
use tangent_shared::Config;

/// This tangent's version: the release a `prebuilt` without `release` is on.
const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug)]
pub struct FetchOptions {
    pub config_path: PathBuf,
    /// One plugin, or every plugin with `prebuilt`.
    pub plugin: Option<String>,
}

pub async fn run(opts: FetchOptions) -> Result<()> {
    let cfg = Config::from_file(&opts.config_path)?;
    let config_dir = opts.config_path.parent().unwrap_or_else(|| Path::new("."));
    let out = config_dir.join(&cfg.runtime.plugins_path);
    fs::create_dir_all(&out).with_context(|| format!("creating {}", out.display()))?;
    let out = out.canonicalize().context("configured plugins path")?;

    if let Some(only) = &opts.plugin {
        if !cfg.plugins.contains_key(only.as_str()) {
            bail!("plugin {only} not found in {}", opts.config_path.display());
        }
    }
    gh_present()?;

    let client = reqwest::Client::builder()
        .user_agent(format!("tangent/{VERSION}"))
        .build()?;
    let mut fetched: Vec<Verified> = Vec::new();
    for (name, plugin) in &cfg.plugins {
        if opts
            .plugin
            .as_deref()
            .is_some_and(|only| only != name.as_ref())
        {
            continue;
        }
        let Some(prebuilt) = &plugin.prebuilt else {
            if opts.plugin.is_some() {
                bail!("plugin {name} has no `prebuilt`: `tangent plugin compile` builds it from its source");
            }
            println!(
                "· {name}: no `prebuilt`; `tangent plugin compile` builds it from {}",
                plugin.path.display()
            );
            continue;
        };
        fetched.push(fetch_one(&client, name, prebuilt, &out).await?);
    }

    if let Some(v) = fetched.first() {
        println!(
            "\nEach component and its attestation bundle are in {}; to check one again:\n  {}",
            out.display(),
            v.by_hand
        );
    }
    Ok(())
}

/// What `gh attestation verify` said of a component.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Verified {
    /// The workflow that signed it, `owner/repo/.github/workflows/x.yml@ref`.
    signer: String,
    /// The commit it was built from.
    commit: String,
    /// The git ref it was built from, `refs/tags/v0.1.10`.
    source_ref: String,
    /// The `gh attestation verify` line that checks it again.
    by_hand: String,
}

async fn fetch_one(
    client: &reqwest::Client,
    name: &str,
    p: &Prebuilt,
    out: &Path,
) -> Result<Verified> {
    let release = p.release(VERSION);
    let asset = p.asset(name);
    let bundle_name = format!("{asset}.sigstore.json");
    // Written beside their destination, so the move into place is a rename.
    let tmp = tempfile::tempdir_in(out)?;

    println!("⬇️  {name}: {asset} from {}/{} {release}", p.host(), p.repo);
    let url = p.asset_url(VERSION, &asset);
    let Some(bytes) = download(client, &url).await? else {
        bail!(
            "{name}: {url} is not there: {} {release} has no {asset}. A release before the \
             plugin was prebuilt, a tangent built from a checkout whose version has no release yet \
             (set `prebuilt.release`), or a pack of its own; `tangent plugin compile` builds it from source",
            p.repo
        );
    };
    let digest = hex::encode(Sha256::digest(&bytes));
    if let Some(pin) = p.pinned_sha256() {
        if pin != digest {
            bail!("{name}: {asset} is sha256 {digest}, and the config pins {pin}");
        }
    }
    let component = tmp.path().join(&asset);
    fs::write(&component, &bytes)?;

    let bundle = match download(client, &p.asset_url(VERSION, &bundle_name)).await? {
        Some(b) => {
            let path = tmp.path().join(&bundle_name);
            fs::write(&path, b)?;
            Some(path)
        }
        None => {
            println!(
                "   no {bundle_name} on the release; asking GitHub for the attestation (needs `gh auth login`)"
            );
            None
        }
    };

    let dest_component = out.join(format!("{name}.component.wasm"));
    let dest_bundle = out.join(format!("{name}.component.wasm.sigstore.json"));
    let verified = verify(
        name,
        &asset,
        &digest,
        &component,
        bundle.as_deref(),
        p,
        &release,
        &dest_component,
        &dest_bundle,
    )?;
    println!(
        "✅ {name}: sha256 {digest}\n   built by {} from {} ({})",
        verified.signer,
        short(&verified.commit),
        verified.source_ref
    );

    let cwasm = tmp.path().join(format!("{name}.cwasm"));
    compile_wasm::precompile(&component, &cwasm)
        .with_context(|| format!("{name}: {asset} is not a component tangent can load"))?;

    fs::rename(&component, &dest_component)?;
    match bundle {
        Some(b) => fs::rename(b, &dest_bundle)?,
        None => {
            let _ = fs::remove_file(&dest_bundle);
        }
    }
    fs::rename(&cwasm, out.join(format!("{name}.cwasm")))?;
    Ok(verified)
}

/// The body at `url`, or `None` when there is nothing there.
async fn download(client: &reqwest::Client, url: &str) -> Result<Option<Vec<u8>>> {
    let resp = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;
    if resp.status() == reqwest::StatusCode::NOT_FOUND {
        return Ok(None);
    }
    let resp = resp
        .error_for_status()
        .with_context(|| format!("GET {url}"))?;
    let bytes = resp
        .bytes()
        .await
        .with_context(|| format!("reading {url}"))?;
    Ok(Some(bytes.to_vec()))
}

fn gh_present() -> Result<()> {
    match Command::new("gh").arg("--version").output() {
        Ok(o) if o.status.success() => Ok(()),
        Ok(o) => bail!(
            "`gh --version` failed: {}",
            String::from_utf8_lossy(&o.stderr).trim()
        ),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => bail!(
            "`gh` (GitHub CLI) not found: it checks each component's attestation. \
             Install it (brew install gh; https://cli.github.com) and run this again; no login is needed"
        ),
        Err(e) => Err(e).context("running gh"),
    }
}

/// The `gh attestation verify` arguments for `component`: signed by `repo`
/// (and its `workflow`, when named) for the release's tag, from `bundle`
/// when the release has one, else from GitHub's API.
fn verify_args(
    component: &Path,
    bundle: Option<&Path>,
    p: &Prebuilt,
    release: &str,
) -> Vec<String> {
    let mut args = vec![
        "attestation".to_string(),
        "verify".to_string(),
        component.display().to_string(),
        "--repo".to_string(),
        p.repo.clone(),
        "--source-ref".to_string(),
        format!("refs/tags/{release}"),
    ];
    if let Some(b) = bundle {
        args.push("--bundle".to_string());
        args.push(b.display().to_string());
    }
    if let Some(w) = &p.workflow {
        args.push("--signer-workflow".to_string());
        args.push(format!("{}/{}", p.repo, w.trim_start_matches('/')));
    }
    if let Some(h) = &p.host {
        args.push("--hostname".to_string());
        args.push(h.clone());
    }
    args
}

#[allow(clippy::too_many_arguments)]
fn verify(
    name: &str,
    asset: &str,
    digest: &str,
    component: &Path,
    bundle: Option<&Path>,
    p: &Prebuilt,
    release: &str,
    dest_component: &Path,
    dest_bundle: &Path,
) -> Result<Verified> {
    let args = verify_args(component, bundle, p, release);
    let output = Command::new("gh")
        .args(&args)
        .args(["--format", "json"])
        .output()
        .context("running gh attestation verify")?;
    if !output.status.success() {
        let err = String::from_utf8_lossy(&output.stderr).trim().to_string();
        if err.contains("unknown flag") {
            bail!(
                "{name}: this gh does not know a flag tangent needs; upgrade GitHub CLI (2.90 does):\n{err}"
            );
        }
        if bundle.is_none() && err.contains("gh auth login") {
            bail!(
                "{name}: {} {release} has no {asset}.sigstore.json beside {asset}, and asking GitHub for \
                 the attestation needs `gh auth login`",
                p.repo
            );
        }
        bail!("{name}: {asset} did not verify, and is not kept:\n{err}");
    }
    let mut verified = attested_as(&output.stdout, digest, asset)
        .with_context(|| format!("{name}: {asset} did not verify, and is not kept"))?;
    // The same check on the files as they are kept.
    verified.by_hand = format!(
        "gh {}",
        verify_args(dest_component, bundle.map(|_| dest_bundle), p, release).join(" ")
    );
    Ok(verified)
}

/// The attestation, among those `gh attestation verify --format json`
/// accepted, whose statement names the file with `digest` as `asset`, and
/// who built it. gh accepts a file whose digest is any subject of a
/// statement, and a statement may cover several files; checked by digest
/// alone, one attested component of a release passed off as another (the
/// asset swapped on the release) would be kept under the other's name and
/// run with its config. So the subject must also be named as the asset.
fn attested_as(stdout: &[u8], digest: &str, asset: &str) -> Result<Verified> {
    let v: Value =
        serde_json::from_slice(stdout).context("reading gh attestation verify's output")?;
    let attestations = v
        .as_array()
        .filter(|a| !a.is_empty())
        .ok_or_else(|| anyhow!("gh attestation verify accepted no attestation"))?;
    let mut named = Vec::new();
    for a in attestations {
        let subjects = a
            .pointer("/verificationResult/statement/subject")
            .and_then(Value::as_array)
            .ok_or_else(|| anyhow!("an accepted attestation's statement has no subjects"))?;
        for s in subjects {
            let sha = s.pointer("/digest/sha256").and_then(Value::as_str);
            let name = s.get("name").and_then(Value::as_str).unwrap_or("");
            if sha.is_some_and(|sha| sha.eq_ignore_ascii_case(digest)) {
                if name == asset {
                    return summarize(a);
                }
                named.push(name.to_string());
            }
        }
    }
    if named.is_empty() {
        bail!("no accepted attestation names a file with sha256 {digest}");
    }
    bail!(
        "the file with sha256 {digest} is attested as {}, not as {asset}: another file of the \
         release under this asset's name",
        named.join(", ")
    )
}

/// Who built it, from one attestation gh accepted.
fn summarize(attestation: &Value) -> Result<Verified> {
    let cert = attestation
        .pointer("/verificationResult/signature/certificate")
        .ok_or_else(|| anyhow!("the attestation has no certificate in gh's output"))?;
    let field = |k: &str| -> Result<String> {
        cert.get(k)
            .and_then(Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| anyhow!("the attestation's certificate has no {k}"))
    };
    let signer = field("buildSignerURI")?;
    let signer = signer
        .strip_prefix("https://")
        .and_then(|s| s.split_once('/').map(|(_, rest)| rest.to_string()))
        .unwrap_or(signer);
    Ok(Verified {
        signer,
        commit: field("sourceRepositoryDigest")?,
        source_ref: field("sourceRepositoryRef")?,
        by_hand: String::new(),
    })
}

fn short(commit: &str) -> &str {
    commit.get(..12).unwrap_or(commit)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prebuilt() -> Prebuilt {
        serde_yaml::from_str("repo: EthanBlackburn/tangent").unwrap()
    }

    #[test]
    fn verify_checks_the_repo_and_the_releases_tag_from_the_bundle() {
        let args = verify_args(
            Path::new("plugins/cloudtrail.component.wasm"),
            Some(Path::new("plugins/cloudtrail.component.wasm.sigstore.json")),
            &prebuilt(),
            "v0.1.10",
        );
        assert_eq!(
            args,
            [
                "attestation",
                "verify",
                "plugins/cloudtrail.component.wasm",
                "--repo",
                "EthanBlackburn/tangent",
                "--source-ref",
                "refs/tags/v0.1.10",
                "--bundle",
                "plugins/cloudtrail.component.wasm.sigstore.json",
            ]
        );
    }

    #[test]
    fn verify_pins_the_workflow_and_the_host_when_named() {
        let p: Prebuilt = serde_yaml::from_str(
            "repo: acme/packs\nworkflow: .github/workflows/release.yml\nhost: github.acme.com\nrelease: packs-3",
        )
        .unwrap();
        let args = verify_args(Path::new("c.wasm"), None, &p, &p.release(VERSION));
        assert_eq!(
            args,
            [
                "attestation",
                "verify",
                "c.wasm",
                "--repo",
                "acme/packs",
                "--source-ref",
                "refs/tags/packs-3",
                "--signer-workflow",
                "acme/packs/.github/workflows/release.yml",
                "--hostname",
                "github.acme.com",
            ]
        );
    }

    const CLOUDTRAIL: &str = "791384b07102b8b4f42972513bec962bc44d6a812668fcd718eef3e3c83a3c0f";
    const FRESH: &str = "8fdda3d978509a6e84dad3e2f7e21bfe09e66e4b932910c28a26f38093e6e11e";

    /// What gh prints for a statement over two of a release's components.
    fn accepted() -> Vec<u8> {
        serde_json::json!([{
            "attestation": {},
            "verificationResult": {
                "statement": { "subject": [
                    { "name": "cloudtrail.component.wasm", "digest": { "sha256": CLOUDTRAIL } },
                    { "name": "fresh.component.wasm", "digest": { "sha256": FRESH } }
                ]},
                "signature": { "certificate": {
                    "buildSignerURI": "https://github.com/EthanBlackburn/tangent/.github/workflows/attest-packs.yml@refs/tags/v0.1.10",
                    "sourceRepositoryDigest": "63837fb778d6a39f02be0ce76ff89f2bc8d74bdc",
                    "sourceRepositoryRef": "refs/tags/v0.1.10"
                }}
            }
        }])
        .to_string()
        .into_bytes()
    }

    #[test]
    fn the_file_must_be_attested_under_the_assets_name() {
        let v = attested_as(&accepted(), CLOUDTRAIL, "cloudtrail.component.wasm").unwrap();
        assert_eq!(
            v.signer,
            "EthanBlackburn/tangent/.github/workflows/attest-packs.yml@refs/tags/v0.1.10"
        );
        assert_eq!(short(&v.commit), "63837fb778d6");
        assert_eq!(v.source_ref, "refs/tags/v0.1.10");

        // fresh's bytes on the release as cloudtrail.component.wasm: attested, but not as cloudtrail
        let err = format!(
            "{:#}",
            attested_as(&accepted(), FRESH, "cloudtrail.component.wasm").unwrap_err()
        );
        assert!(
            err.contains("attested as fresh.component.wasm, not as cloudtrail.component.wasm"),
            "{err}"
        );
        // a digest no statement has, though gh accepted something
        let other = "0".repeat(64);
        let err = format!(
            "{:#}",
            attested_as(&accepted(), &other, "cloudtrail.component.wasm").unwrap_err()
        );
        assert!(
            err.contains("no accepted attestation names a file with sha256 000"),
            "{err}"
        );
        assert!(attested_as(b"[]", CLOUDTRAIL, "cloudtrail.component.wasm").is_err());
        assert!(attested_as(b"not json", CLOUDTRAIL, "cloudtrail.component.wasm").is_err());
    }

    #[test]
    fn a_certificate_missing_a_field_is_an_error_not_a_placeholder() {
        let a = serde_json::json!({ "verificationResult": { "signature": { "certificate": {
            "buildSignerURI": "https://github.com/x/y/.github/workflows/r.yml@refs/tags/v1"
        }}}});
        let err = format!("{:#}", summarize(&a).unwrap_err());
        assert!(err.contains("no sourceRepositoryDigest"), "{err}");
    }
}
