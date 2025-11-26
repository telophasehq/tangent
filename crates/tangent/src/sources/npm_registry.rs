// tangent_runtime/src/sources/npm_registry.rs

use anyhow::{anyhow, Context, Result};
use bytes::BytesMut;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tangent_shared::dag::NodeRef;
use tangent_shared::sources::npm_registry::NpmRegistryConfig;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;

use crate::cache::CacheHandle;
use crate::router::Router;
use crate::wasm::host::tangent::logs::log::Scalar;

#[derive(Debug, Deserialize, Serialize)]
struct NpmTimeMap {
    // we only care about dynamic keys, so use a generic map
    #[serde(flatten)]
    entries: HashMap<String, String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct NpmDist {
    #[serde(default)]
    shasum: Option<String>,
    #[serde(default)]
    integrity: Option<String>,
    #[serde(default)]
    tarball: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
struct NpmVersion {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    dist: Option<NpmDist>,
}

#[derive(Debug, Deserialize, Serialize)]
struct NpmPackageDoc {
    #[serde(default)]
    name: Option<String>,

    #[serde(default)]
    versions: HashMap<String, NpmVersion>,

    #[serde(default)]
    time: Option<NpmTimeMap>,
}

/// Poll the NPM registry for configured packages and emit new versions as NDJSON.
pub async fn run_consumer(
    name: Arc<str>,
    cfg: NpmRegistryConfig,
    router: Arc<Router>,
    shutdown: CancellationToken,
    cache: Arc<CacheHandle>,
) -> Result<()> {
    let from = NodeRef::Source { name };

    let client = reqwest::Client::new();

    let mut seen = SeenTracker::new(cache);

    let interval_secs = cfg.interval_secs.max(10); // sane minimum
    let mut ticker = interval(Duration::from_secs(interval_secs));

    if cfg.packages.is_none() && cfg.orgs.is_none() {
        anyhow::bail!("must configure either npm packages or orgs.")
    }

    tracing::info!(
        "npm_registry source starting: packages={:?}, interval={}s",
        cfg.packages,
        interval_secs
    );

    loop {
        tokio::select! {
            () = shutdown.cancelled() => {
                tracing::info!("npm_registry source shutting down");
                break;
            }

            _ = ticker.tick() => {
                let mut packages = Vec::<String>::new();
                if cfg.packages.as_ref().is_some() {
                    packages.extend_from_slice(cfg.packages.as_ref().unwrap());
                } else {
                    for org in cfg.orgs.as_ref().unwrap() {
                        let org_packages = list_org_packages(org, &client, &cfg).await?;
                        packages.extend_from_slice(&org_packages);
                    }
                }

                for pkg in packages {
                    if let Err(e) =
                        poll_package(&pkg, &client, &cfg, &mut seen, &router, &from).await
                    {
                        tracing::warn!(package = %pkg, "npm_registry poll error: {e:#}");
                    }
                }
            }
        }
    }

    Ok(())
}

async fn poll_package(
    package: &str,
    client: &reqwest::Client,
    cfg: &NpmRegistryConfig,
    seen: &mut SeenTracker,
    router: &Arc<Router>,
    from: &NodeRef,
) -> Result<()> {
    let url = format!("https://registry.npmjs.org/{}", package);

    let mut req = client.get(&url);
    if let Some(token) = &cfg.token {
        req = req.bearer_auth(token);
    }

    let resp = req.send().await.context("request to npm registry failed")?;
    if !resp.status().is_success() {
        return Err(anyhow!(
            "npm registry returned status {} for {}",
            resp.status(),
            url
        ));
    }

    let bytes = resp
        .bytes()
        .await
        .context("failed to read npm response body")?;

    let doc: NpmPackageDoc =
        serde_json::from_slice(&bytes).context("failed to parse npm package doc")?;

    let package_name = doc.name.clone().unwrap_or_else(|| package.to_string());
    let time_map = doc.time.as_ref().map(|t| &t.entries);

    let mut frames: Vec<BytesMut> = Vec::new();

    for (version, vinfo) in &doc.versions {
        if seen.contains(&package_name, version)? {
            continue;
        }

        let ts = time_map.and_then(|m| m.get(version)).cloned();

        let mut v_enriched =
            serde_json::to_value(vinfo).context("failed to convert npm version to json")?;
        let obj = v_enriched
            .as_object_mut()
            .ok_or_else(|| anyhow!("npm version should be a JSON object"))?;

        obj.entry("name".to_string())
            .or_insert_with(|| Value::String(package_name.clone()));
        obj.insert("version".to_string(), Value::String(version.clone()));

        if let Some(ts_val) = ts {
            obj.insert("time".to_string(), Value::String(ts_val));
        }

        let event = json!({
            "kind": "npm_package_version",
            "npm": v_enriched,
        });

        let mut buf = BytesMut::with_capacity(256);
        buf.extend_from_slice(event.to_string().as_bytes());
        buf.extend_from_slice(b"\n");
        frames.push(buf);

        seen.record(&package_name, version)?;
    }

    if !frames.is_empty() {
        router
            .forward(from, frames, Vec::new())
            .await
            .context("router.forward failed for npm_registry")?;
    }

    Ok(())
}

async fn list_org_packages(
    org: &str,
    client: &reqwest::Client,
    cfg: &NpmRegistryConfig,
) -> Result<Vec<String>> {
    let url = format!("https://registry.npmjs.org/-/user/{}/package", org);

    let mut req = client.get(&url);
    if let Some(token) = &cfg.token {
        req = req.bearer_auth(token);
    }

    let resp = req.send().await.context("request to npm registry failed")?;
    if !resp.status().is_success() {
        return Err(anyhow!(
            "npm registry returned status {} for {}",
            resp.status(),
            url
        ));
    }

    let bytes = resp
        .bytes()
        .await
        .context("failed to read npm response body")?;

    let packages: HashMap<String, String> =
        serde_json::from_slice(&bytes).context("failed to parse npm package doc")?;

    Ok(packages.keys().map(|x| x.to_owned()).collect())
}

const SEEN_VERSION_TTL_MS: u64 = 7 * 24 * 60 * 60 * 1000;
const SEEN_VERSION_NAMESPACE: &str = "sources/npm_registry/seen";

struct SeenTracker {
    handle: Arc<CacheHandle>,
}

impl SeenTracker {
    fn new(cache: Arc<CacheHandle>) -> Self {
        Self { handle: cache }
    }

    fn contains(&mut self, package: &str, version: &str) -> Result<bool> {
        let key = cache_key(package, version);
        Ok(self.handle.get(&key)?.is_some())
    }

    fn record(&mut self, package: &str, version: &str) -> Result<()> {
        let key = cache_key(package, version);
        self.handle
            .set(&key, &Scalar::Boolean(true), Some(SEEN_VERSION_TTL_MS))?;
        Ok(())
    }
}

fn cache_key(package: &str, version: &str) -> String {
    format!("{SEEN_VERSION_NAMESPACE}:{package}:{version}")
}
