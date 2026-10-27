//! A GitHub organization's audit log, polled from the REST API.

use std::sync::Arc;
use std::time::Duration as StdDuration;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use serde_json::Value;
use tangent_shared::sources::github_audit::GithubAuditConfig;
use tokio_util::sync::CancellationToken;

use crate::router::Router;
use crate::sources::poll::{idle, next_link, opt, parse_time, urlencode, Poller};
use crate::state::StateStore;

const SOURCE: &str = "github_audit";
/// The log is not instantaneous; look back this far past the cursor.
const LATE_MS: i64 = 5 * 60 * 1000;

pub async fn run_consumer(
    name: Arc<str>,
    cfg: GithubAuditConfig,
    router: Arc<Router>,
    store: Arc<dyn StateStore>,
    shutdown: CancellationToken,
) -> Result<()> {
    let (Some(org), Some(token)) = (opt(&cfg.org), opt(&cfg.token)) else {
        idle(&name, "github_audit org or token");
        shutdown.cancelled().await;
        return Ok(());
    };
    let poller = Poller::new(
        Arc::clone(&name),
        store,
        router,
        Duration::milliseconds(LATE_MS),
    );
    let client = reqwest::Client::builder()
        .user_agent("tangent")
        .timeout(StdDuration::from_secs(30))
        .build()?;
    let mut ticker = tokio::time::interval(StdDuration::from_secs(cfg.interval_secs.max(10)));
    let mut failing = false;
    tracing::info!(source = %name, org, since = %cfg.since, "github_audit source polling");

    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            _ = ticker.tick() => match poll(&client, &cfg, org, token, &poller).await {
                Ok(n) => {
                    failing = false;
                    if n > 0 {
                        tracing::debug!(source = %name, events = n, "github_audit poll");
                    }
                }
                Err(e) if !failing => {
                    failing = true;
                    tracing::warn!(source = %name, "github_audit poll failed (retrying each interval): {e:#}");
                }
                Err(e) => tracing::debug!(source = %name, "github_audit poll failed: {e:#}"),
            },
        }
    }
    Ok(())
}

async fn poll(
    client: &reqwest::Client,
    cfg: &GithubAuditConfig,
    org: &str,
    token: &str,
    poller: &Poller,
) -> Result<usize> {
    let start = poller.start(&cfg.since).await;
    let end = Utc::now();
    let mut newest: Option<DateTime<Utc>> = None;
    let mut out: Vec<(DateTime<Utc>, Value)> = Vec::new();
    let mut url = Some(format!(
        "{}/orgs/{}/audit-log?phrase={}&include={}&order=asc&per_page=100",
        cfg.api.trim_end_matches('/'),
        org,
        urlencode(&format!(
            "created:>={}",
            start.format("%Y-%m-%dT%H:%M:%S+00:00")
        )),
        cfg.include
    ));
    while let Some(u) = url.take() {
        let resp = client
            .get(&u)
            .bearer_auth(token)
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28")
            .send()
            .await
            .context("audit-log request")?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            bail!(
                "audit-log {status}: {}",
                body.chars().take(200).collect::<String>()
            );
        }
        url = resp
            .headers()
            .get("link")
            .and_then(|h| h.to_str().ok())
            .and_then(next_link);
        let page: Vec<Value> = resp.json().await.context("audit-log body")?;
        if page.is_empty() {
            break;
        }
        for v in page {
            let t = parse_time(&v["@timestamp"]).unwrap_or(end);
            let id = match v["_document_id"].as_str() {
                Some(id) if !id.is_empty() => id.to_string(),
                _ => format!(
                    "{}:{}:{}",
                    t.timestamp_millis(),
                    v["action"].as_str().unwrap_or(""),
                    v["actor"].as_str().unwrap_or("")
                ),
            };
            if !poller.first_sight(&id).await {
                continue;
            }
            newest = Some(newest.map_or(t, |n| n.max(t)));
            out.push((t, v));
        }
        if out.len() >= 5_000 {
            break;
        }
    }
    let n = poller.emit(SOURCE, out).await?;
    poller.finish(newest, end).await;
    Ok(n)
}
