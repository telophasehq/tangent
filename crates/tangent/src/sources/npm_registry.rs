// tangent_runtime/src/sources/npm_registry.rs

use ahash::HashMap;
use anyhow::{anyhow, Context, Result};
use bytes::BytesMut;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use serde_json::{json, Value};
use std::sync::Arc;
use std::time::Duration;
use tangent_shared::dag::NodeRef;
use tangent_shared::sources::npm_registry::NpmRegistryConfig;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;

use crate::router::Router;

/// Poll the NPM registry for configured packages and emit new versions as NDJSON.
pub async fn run_consumer(
    name: Arc<str>,
    cfg: NpmRegistryConfig,
    router: Arc<Router>,
    shutdown: CancellationToken,
) -> Result<()> {
    let from = NodeRef::Source { name };

    let client = reqwest::Client::new();

    let interval_secs = cfg.interval_secs.max(5); // sane minimum
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

                tracing::info!("polling {:?} packages", packages);
                for pkg in packages {
                    if let Err(e) =
                        poll_package(&pkg, &client, &cfg, &router, &from).await
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
        .text()
        .await
        .context("failed to read npm response body")?;

    let doc: Value = serde_json::from_str(&bytes).context("failed to parse npm package doc")?;

    let package_name = doc
        .get("name")
        .and_then(Value::as_str)
        .map(|s| s.to_string())
        .unwrap_or_else(|| package.to_string());
    let time_map = doc.get("time").and_then(Value::as_object);
    let cutoff = Utc::now() - ChronoDuration::minutes(RECENT_WINDOW_MINUTES);

    let versions = match doc.get("versions").and_then(Value::as_object) {
        Some(map) => map,
        None => return Ok(()),
    };

    let mut frames: Vec<BytesMut> = Vec::new();

    for (version, vinfo) in versions {
        let ts_str = match time_map
            .and_then(|m| m.get(version))
            .and_then(Value::as_str)
        {
            Some(ts) => ts,
            None => continue,
        };

        let published_at = match DateTime::parse_from_rfc3339(ts_str) {
            Ok(parsed) => parsed.with_timezone(&Utc),
            Err(err) => {
                tracing::debug!(
                    package = %package_name,
                    version = %version,
                    error = %err,
                    "skipping npm version without parsable timestamp"
                );
                continue;
            }
        };

        if published_at < cutoff {
            continue;
        }

        let event = json!({
            "kind": "npm_package_version",
            "npm": vinfo,
        });

        let mut buf = BytesMut::with_capacity(256);
        buf.extend_from_slice(event.to_string().as_bytes());
        buf.extend_from_slice(b"\n");
        frames.push(buf);
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

const RECENT_WINDOW_MINUTES: i64 = 500;
