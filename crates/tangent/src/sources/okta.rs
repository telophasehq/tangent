//! Okta's System Log, polled.

use std::sync::Arc;
use std::time::Duration as StdDuration;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use serde_json::Value;
use tangent_shared::sources::okta::OktaConfig;
use tokio_util::sync::CancellationToken;

use crate::router::Router;
use crate::sources::poll::{idle, next_link, opt, parse_time, secret, Poller};
use crate::state::StateStore;

const SOURCE: &str = "okta";
const LATE_MS: i64 = 5 * 60 * 1000;

pub async fn run_consumer(
    name: Arc<str>,
    cfg: OktaConfig,
    router: Arc<Router>,
    store: Arc<dyn StateStore>,
    shutdown: CancellationToken,
) -> Result<()> {
    let (Some(org_url), Some(token)) = (opt(&cfg.org_url), secret(&cfg.token)) else {
        idle(&name, "okta org_url or token");
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
    tracing::info!(source = %name, org = org_url, since = %cfg.since, "okta source polling the System Log");

    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            _ = ticker.tick() => match poll(&client, &cfg, org_url, token, &poller).await {
                Ok(n) => {
                    failing = false;
                    if n > 0 {
                        tracing::debug!(source = %name, events = n, "okta poll");
                    }
                }
                Err(e) if !failing => {
                    failing = true;
                    tracing::warn!(source = %name, "okta poll failed (retrying each interval): {e:#}");
                }
                Err(e) => tracing::debug!(source = %name, "okta poll failed: {e:#}"),
            },
        }
    }
    Ok(())
}

async fn poll(
    client: &reqwest::Client,
    cfg: &OktaConfig,
    org_url: &str,
    token: &str,
    poller: &Poller,
) -> Result<usize> {
    let start = poller.start(&cfg.since).await;
    let end = Utc::now();
    let mut newest: Option<DateTime<Utc>> = None;
    let mut out: Vec<(DateTime<Utc>, String, Value)> = Vec::new();
    let mut url = Some(format!(
        "{}/api/v1/logs?since={}&sortOrder=ASCENDING&limit=1000",
        org_url.trim_end_matches('/'),
        start.format("%Y-%m-%dT%H:%M:%S%.3fZ")
    ));
    while let Some(u) = url.take() {
        let resp = client
            .get(&u)
            .header("Authorization", format!("SSWS {token}"))
            .header("Accept", "application/json")
            .send()
            .await
            .context("System Log request")?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            bail!(
                "System Log {status}: {}",
                body.chars().take(200).collect::<String>()
            );
        }
        url = resp
            .headers()
            .get("link")
            .and_then(|h| h.to_str().ok())
            .and_then(next_link);
        let page: Vec<Value> = resp.json().await.context("System Log body")?;
        if page.is_empty() {
            // Okta's `next` link is a bookmark: an empty page means we are
            // current. The cursor carries us from here next time.
            break;
        }
        for v in page {
            let t = parse_time(&v["published"]).unwrap_or(end);
            let id = match v["uuid"].as_str() {
                Some(id) if !id.is_empty() => id.to_string(),
                _ => format!(
                    "{}:{}",
                    t.timestamp_millis(),
                    v["eventType"].as_str().unwrap_or("")
                ),
            };
            if poller.seen(&id).await {
                continue;
            }
            newest = Some(newest.map_or(t, |n| n.max(t)));
            out.push((t, id, v));
        }
        if out.len() >= 10_000 {
            break;
        }
    }
    let n = poller.emit(SOURCE, out).await?;
    poller.finish(newest, end).await;
    Ok(n)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::sources::poll::testing::{throttled_on_page_two, Keeper};

    #[tokio::test]
    async fn a_429_on_page_two_leaves_page_one_to_the_next_poll() {
        let base = throttled_on_page_two(json!([
            {"uuid": "a", "published": "2026-11-06T12:00:00.000Z", "eventType": "user.session.start"},
            {"uuid": "b", "published": "2026-11-06T12:00:01.000Z", "eventType": "user.session.start"},
        ]))
        .await;
        let cfg: OktaConfig = serde_json::from_value(json!({})).unwrap();
        let client = reqwest::Client::new();
        let keeper = Keeper::new("okta");
        let err = poll(&client, &cfg, &base, "t", &keeper.poller)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("429"), "{err:#}");
        let n = poll(&client, &cfg, &base, "t", &keeper.poller)
            .await
            .unwrap();
        assert_eq!(n, 2, "page one, on the next poll");
        let n = poll(&client, &cfg, &base, "t", &keeper.poller)
            .await
            .unwrap();
        assert_eq!(n, 0, "and only once");
        assert_eq!(keeper.sent("/uuid").await, ["a", "b"]);
    }
}
