// tangent_runtime/src/sources/npm_registry.rs

use ahash::HashMap;
use anyhow::{anyhow, Context, Result};
use bytes::BytesMut;
use chrono::{DateTime, Duration as ChronoDuration, Utc};
use secrecy::ExposeSecret;
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
    let mut sent = Sent::default();

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
                // A version older than the window is no longer read, so it
                // need not be remembered.
                let cutoff = Utc::now() - ChronoDuration::minutes(RECENT_WINDOW_MINUTES);
                sent.retain(|_, published_at| *published_at >= cutoff);
                for pkg in packages {
                    if let Err(e) = poll_package(
                        &pkg, &client, &cfg, &router, &from, cutoff, &mut sent,
                    )
                    .await
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
    cutoff: DateTime<Utc>,
    sent: &mut Sent,
) -> Result<()> {
    let url = format!("https://registry.npmjs.org/{}", package);

    let req = authorize(client.get(&url), cfg);

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

    let (frames, fresh) = unsent_versions(&doc, package, cutoff, sent);
    if !frames.is_empty() {
        router
            .forward(from, frames, Vec::new())
            .await
            .context("router.forward failed for npm_registry")?;
    }
    // Remembered once forwarded: a failed forward is sent again next poll.
    sent.extend(fresh);

    Ok(())
}

/// The versions already sent, keyed `name@version`, with when each was
/// published. Every poll reads the whole window; a version is sent once.
type Sent = HashMap<String, DateTime<Utc>>;

/// One NDJSON frame for each version in `doc` published since `cutoff` and not
/// in `sent`, with the keys to remember once the frames are forwarded.
fn unsent_versions(
    doc: &Value,
    package: &str,
    cutoff: DateTime<Utc>,
    sent: &Sent,
) -> (Vec<BytesMut>, Vec<(String, DateTime<Utc>)>) {
    let package_name = doc
        .get("name")
        .and_then(Value::as_str)
        .map(|s| s.to_string())
        .unwrap_or_else(|| package.to_string());
    let time_map = doc.get("time").and_then(Value::as_object);

    let mut frames: Vec<BytesMut> = Vec::new();
    let mut fresh = Vec::new();

    let versions = match doc.get("versions").and_then(Value::as_object) {
        Some(map) => map,
        None => return (frames, fresh),
    };

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

        let key = format!("{package_name}@{version}");
        if sent.contains_key(&key) {
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
        fresh.push((key, published_at));
    }

    (frames, fresh)
}

async fn list_org_packages(
    org: &str,
    client: &reqwest::Client,
    cfg: &NpmRegistryConfig,
) -> Result<Vec<String>> {
    let url = format!("https://registry.npmjs.org/-/user/{}/package", org);

    let req = authorize(client.get(&url), cfg);

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

/// `req` with the token as a bearer, or as it is without a token. An empty
/// token is no token: `token: "${NPM_TOKEN}"` with NPM_TOKEN unset is "",
/// and the registry answers `Bearer ` with 401 on an org listing.
fn authorize(req: reqwest::RequestBuilder, cfg: &NpmRegistryConfig) -> reqwest::RequestBuilder {
    match cfg.token.as_ref().map(|t| t.expose_secret()) {
        Some(token) if !token.is_empty() => req.bearer_auth(token),
        _ => req,
    }
}

const RECENT_WINDOW_MINUTES: i64 = 500;

#[cfg(test)]
mod tests {
    use super::*;

    /// A registry document for left-pad with these versions and publish times.
    fn doc(versions: &[(&str, DateTime<Utc>)]) -> Value {
        let time: serde_json::Map<String, Value> = versions
            .iter()
            .map(|(v, t)| (v.to_string(), json!(t.to_rfc3339())))
            .collect();
        let infos: serde_json::Map<String, Value> = versions
            .iter()
            .map(|(v, _)| (v.to_string(), json!({ "name": "left-pad", "version": v })))
            .collect();
        json!({ "name": "left-pad", "time": time, "versions": infos })
    }

    fn versions_of(frames: &[BytesMut]) -> Vec<String> {
        frames
            .iter()
            .map(|f| {
                let event: Value = serde_json::from_slice(f).unwrap();
                event["npm"]["version"].as_str().unwrap().to_string()
            })
            .collect()
    }

    #[test]
    fn sends_each_version_in_the_window_once() {
        let now = Utc::now();
        let cutoff = now - ChronoDuration::minutes(RECENT_WINDOW_MINUTES);
        let before_window = cutoff - ChronoDuration::minutes(1);
        let mut sent = Sent::default();

        let first = doc(&[
            ("1.0.0", before_window),
            ("1.1.0", now - ChronoDuration::minutes(10)),
        ]);
        let (frames, fresh) = unsent_versions(&first, "left-pad", cutoff, &sent);
        assert_eq!(versions_of(&frames), ["1.1.0"]);
        sent.extend(fresh);

        // The next poll reads the same window: 1.1.0 is not sent again.
        let (frames, fresh) = unsent_versions(&first, "left-pad", cutoff, &sent);
        assert!(frames.is_empty());
        assert!(fresh.is_empty());

        // A version published since is sent, and only it.
        let second = doc(&[
            ("1.0.0", before_window),
            ("1.1.0", now - ChronoDuration::minutes(10)),
            ("1.2.0", now),
        ]);
        let (frames, fresh) = unsent_versions(&second, "left-pad", cutoff, &sent);
        assert_eq!(versions_of(&frames), ["1.2.0"]);
        assert_eq!(fresh, [("left-pad@1.2.0".to_string(), now)]);
    }

    #[test]
    fn empty_token_sends_no_authorization_header() {
        let authorization = |token: Option<&str>| {
            let cfg = NpmRegistryConfig {
                packages: Some(vec!["left-pad".to_string()]),
                orgs: None,
                interval_secs: 5,
                token: token.map(secrecy::SecretString::from),
            };
            let req = reqwest::Client::new().get("https://registry.npmjs.org/left-pad");
            authorize(req, &cfg)
                .build()
                .unwrap()
                .headers()
                .get(reqwest::header::AUTHORIZATION)
                .cloned()
        };

        assert_eq!(authorization(None), None);
        assert_eq!(authorization(Some("")), None);
        assert_eq!(authorization(Some("npm_abc")).unwrap(), "Bearer npm_abc");
    }
}
