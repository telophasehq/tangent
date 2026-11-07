//! Okta's System Log, polled. A polling request (`sortOrder=ASCENDING`, no
//! `until`) returns events in the order Okta stored them, not by
//! `published`, and every page, an empty one too, links on to the next with
//! an `after` only Okta makes
//! (<https://developer.okta.com/docs/reference/system-log-query/>, "Polling
//! requests"). That link is the source's place: once a page's records are
//! forwarded, the link after it is saved in the state store (namespace
//! `sources`, key `<name>:next`, never expiring), and the next poll, or a
//! restart, asks for it as it is. Nothing is read twice, so there is no
//! lookback and no seen ids. A first start reads from `since` ago, and so
//! does a saved link Okta turns down (a 4xx but 401, 403, 408 or 429: its
//! `after` changed format, say), which would otherwise fail every poll, and
//! a link saved while `org_url` was another org's, since the token goes
//! with every request. The link is saved with the `org_url` it was read
//! under, and that, not the link's own host, is what is checked: Okta's
//! links may name another host than `org_url` (its okta.com host for a
//! custom domain, say), and a poll follows them anyway. The link holds the
//! query and Okta's `after`, never the token, which goes in a header.

use std::sync::Arc;
use std::time::Duration as StdDuration;

use anyhow::{bail, Context, Result};
use chrono::{Duration, Utc};
use serde_json::Value;
use tangent_shared::sources::okta::OktaConfig;
use tokio_util::sync::CancellationToken;

use crate::router::Router;
use crate::sources::poll::{duration_ms, idle, next_link, opt, parse_time, secret, Poller};
use crate::state::StateStore;

const SOURCE: &str = "okta";
/// Okta's largest page.
const PAGE: usize = 1000;
/// The most one poll sends; the saved link leaves the rest to the next.
const CAP: usize = 10_000;

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
    // No lateness: the saved link, not a time, is where a poll starts.
    let poller = Poller::new(Arc::clone(&name), store, router, Duration::zero());
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
    let org = org_url.trim_end_matches('/');
    let since = || {
        let since =
            Utc::now() - Duration::milliseconds(duration_ms(&cfg.since).unwrap_or(3_600_000));
        format!(
            "{org}/api/v1/logs?since={}&sortOrder=ASCENDING&limit={PAGE}",
            since.format("%Y-%m-%dT%H:%M:%S%.3fZ")
        )
    };
    // Whether `url` is the saved link, which only the first request is.
    let (mut url, mut saved) = match poller.saved_link().await? {
        Some((base, link)) if base == org => (link, true),
        other => {
            if let Some((base, _)) = other {
                tracing::warn!(
                    source = %poller.name,
                    saved_for = base,
                    org,
                    "the saved next link was another org_url's; reading from since"
                );
            }
            (since(), false)
        }
    };
    let mut sent = 0;
    loop {
        let resp = client
            .get(&url)
            .header("Authorization", format!("SSWS {token}"))
            .header("Accept", "application/json")
            .send()
            .await
            .context("System Log request")?;
        let status = resp.status();
        if !status.is_success() {
            let body: String = resp
                .text()
                .await
                .unwrap_or_default()
                .chars()
                .take(200)
                .collect();
            // Okta turned down the saved link itself (its `after` changed
            // format, or the link went bad): asked for again, it would fail
            // every poll for good. A 401 or 403 (the token), 408 or 429 is
            // not the link's fault and keeps it.
            if saved
                && status.is_client_error()
                && !matches!(status.as_u16(), 401 | 403 | 408 | 429)
            {
                tracing::warn!(
                    source = %poller.name,
                    link = url,
                    "Okta turned down the saved next link ({status}: {body}); reading from \
                     since ago, so events in that window may be sent again"
                );
                url = since();
                saved = false;
                continue;
            }
            bail!("System Log {status}: {body}");
        }
        saved = false;
        let next = resp
            .headers()
            .get("link")
            .and_then(|h| h.to_str().ok())
            .and_then(next_link);
        let page: Vec<Value> = resp.json().await.context("System Log body")?;
        let read = page.len();
        let now = Utc::now();
        let records = page
            .into_iter()
            .map(|v| (parse_time(&v["published"]).unwrap_or(now), v))
            .collect();
        sent += poller.forward(SOURCE, records).await?;
        // The page is sent: only now is the link after it the place to
        // resume. A crash before here reads the page again rather than loses
        // it; a 429 on the next page leaves that page to the next poll.
        let Some(next) = next else { break };
        if next != url {
            poller.save_link(org, &next).await?;
        }
        // An empty page: Okta has nothing newer yet.
        if read == 0 || sent >= CAP {
            break;
        }
        url = next;
    }
    Ok(sent)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Mutex;

    use axum::extract::Query;
    use axum::http::{HeaderMap, StatusCode, Uri};
    use axum::response::{IntoResponse, Response};
    use axum::Json;
    use chrono::DateTime;
    use serde_json::json;

    use super::*;
    use crate::sources::poll::testing::{oldest_first, serve, throttled_on_page_two, Keeper};

    /// Okta's System Log as a polling request sees it: `events` in the
    /// order Okta stored them, `limit` a page. `since` starts at the first
    /// one published then or later, and every page, an empty one too, links
    /// on with an `after` (here the count read so far; Okta's own is opaque).
    /// The token is `t`; any other gets a 401.
    struct Okta {
        base: String,
        events: Arc<Mutex<Vec<Value>>>,
        asked: Arc<Mutex<Vec<String>>>,
    }

    impl Okta {
        async fn serve(events: Vec<Value>) -> Self {
            let events = Arc::new(Mutex::new(events));
            let asked = Arc::new(Mutex::new(Vec::new()));
            let (stored, noted) = (Arc::clone(&events), Arc::clone(&asked));
            let base = serve(move |base| {
                let base = base.to_string();
                axum::Router::new().fallback(
                    move |uri: Uri,
                          headers: HeaderMap,
                          Query(q): Query<HashMap<String, String>>| {
                        noted.lock().unwrap().push(uri.to_string());
                        let resp = if headers["authorization"] == "SSWS t" {
                            answer(&base, &stored.lock().unwrap(), &q)
                        } else {
                            StatusCode::UNAUTHORIZED.into_response()
                        };
                        async move { resp }
                    },
                )
            })
            .await;
            Self {
                base,
                events,
                asked,
            }
        }

        /// Each request's path and query, in order.
        fn asked(&self) -> Vec<String> {
            self.asked.lock().unwrap().clone()
        }

        /// An event Okta stores now.
        fn store(&self, id: &str) {
            self.events.lock().unwrap().push(event(id));
        }
    }

    /// A page of `events` for the query `q`, or a 400 for an `after` this
    /// Okta does not take.
    fn answer(base: &str, events: &[Value], q: &HashMap<String, String>) -> Response {
        let from = match (q.get("after"), q.get("since")) {
            (Some(after), _) => match after.parse() {
                Ok(from) => from,
                Err(_) => {
                    let error = json!({"errorCode": "E0000001", "errorSummary": "Api validation failed: after"});
                    return (StatusCode::BAD_REQUEST, Json(error)).into_response();
                }
            },
            (None, Some(since)) => {
                let since: DateTime<Utc> = since.parse().unwrap();
                events
                    .iter()
                    .position(|v| parse_time(&v["published"]).unwrap() >= since)
                    .unwrap_or(events.len())
            }
            (None, None) => 0,
        };
        let limit: usize = q.get("limit").map_or(100, |l| l.parse().unwrap());
        let page: Vec<Value> = events.iter().skip(from).take(limit).cloned().collect();
        let link = format!(
            "<{base}/api/v1/logs?after={}&limit={limit}&sortOrder=ASCENDING>; rel=\"next\"",
            from + page.len()
        );
        ([("link", link)], Json(page)).into_response()
    }

    fn event(id: &str) -> Value {
        json!({"uuid": id, "published": Utc::now().to_rfc3339(), "eventType": "user.session.start"})
    }

    #[tokio::test]
    async fn a_second_poll_resumes_from_the_saved_link_and_sends_nothing_twice() {
        let okta = Okta::serve(vec![event("a"), event("b")]).await;
        let cfg: OktaConfig = serde_json::from_value(json!({})).unwrap();
        let client = reqwest::Client::new();
        let keeper = Keeper::new("okta");
        let n = poll(&client, &cfg, &okta.base, "t", &keeper.poller)
            .await
            .unwrap();
        assert_eq!(n, 2);
        okta.store("c");
        let n = poll(&client, &cfg, &okta.base, "t", &keeper.poller)
            .await
            .unwrap();
        assert_eq!(n, 1, "only what Okta stored since");
        let asked = okta.asked();
        assert!(asked[0].contains("since="), "a first start: {asked:?}");
        assert_eq!(
            asked[2], asked[1],
            "the second poll asks for the link the first ended on"
        );
        assert!(
            !asked[2..].iter().any(|u| u.contains("since=")),
            "and never for a time again: {asked:?}"
        );
        assert_eq!(keeper.keys().await, ["okta:next"], "no seen ids");
        assert_eq!(keeper.sent("/uuid").await, ["a", "b", "c"]);
    }

    #[tokio::test]
    async fn a_restart_resumes_from_the_saved_link() {
        let okta = Okta::serve(vec![event("a"), event("b")]).await;
        let cfg: OktaConfig = serde_json::from_value(json!({})).unwrap();
        let client = reqwest::Client::new();
        let before = Keeper::new("okta");
        let n = poll(&client, &cfg, &okta.base, "t", &before.poller)
            .await
            .unwrap();
        assert_eq!(n, 2);
        let after = before.restarted();
        okta.store("c");
        let n = poll(&client, &cfg, &okta.base, "t", &after.poller)
            .await
            .unwrap();
        assert_eq!(n, 1, "only what Okta stored since");
        let asked = okta.asked();
        assert_eq!(
            asked[2], asked[1],
            "the restarted source asks for the link the last poll ended on: {asked:?}"
        );
        assert_eq!(before.sent("/uuid").await, ["a", "b"]);
        assert_eq!(after.sent("/uuid").await, ["c"]);
    }

    /// A link saved while `org_url` was another org's is not asked for: the
    /// token for this one does not go there.
    #[tokio::test]
    async fn a_link_saved_for_another_org_url_is_ignored() {
        let other = Okta::serve(vec![event("x")]).await;
        let okta = Okta::serve(vec![event("a"), event("b")]).await;
        let cfg: OktaConfig = serde_json::from_value(json!({})).unwrap();
        let client = reqwest::Client::new();
        let keeper = Keeper::new("okta");
        let theirs = format!(
            "{}/api/v1/logs?after=0&limit=1000&sortOrder=ASCENDING",
            other.base
        );
        keeper.poller.save_link(&other.base, &theirs).await.unwrap();
        let n = poll(&client, &cfg, &okta.base, "t", &keeper.poller)
            .await
            .unwrap();
        assert_eq!(n, 2);
        assert!(other.asked().is_empty(), "the other org is never asked");
        assert!(okta.asked()[0].contains("since="), "{:?}", okta.asked());
        let (base, link) = keeper.poller.saved_link().await.unwrap().unwrap();
        assert_eq!(base, okta.base);
        assert!(link.starts_with(&okta.base), "{link}");
        assert_eq!(keeper.sent("/uuid").await, ["a", "b"]);
    }

    /// `org_url` may be a custom domain while Okta's links name another host
    /// (here `localhost` against links on `127.0.0.1`, the same server): a
    /// link saved for this `org_url` is still where the next poll starts.
    #[tokio::test]
    async fn a_saved_link_on_another_host_than_org_url_is_followed() {
        let okta = Okta::serve(vec![event("a"), event("b")]).await;
        let org_url = okta.base.replace("127.0.0.1", "localhost");
        let cfg: OktaConfig = serde_json::from_value(json!({})).unwrap();
        let client = reqwest::Client::new();
        let keeper = Keeper::new("okta");
        let n = poll(&client, &cfg, &org_url, "t", &keeper.poller)
            .await
            .unwrap();
        assert_eq!(n, 2);
        okta.store("c");
        let n = poll(&client, &cfg, &org_url, "t", &keeper.poller)
            .await
            .unwrap();
        assert_eq!(n, 1, "only what Okta stored since");
        let asked = okta.asked();
        assert_eq!(asked[2], asked[1], "the saved link: {asked:?}");
        assert_eq!(keeper.sent("/uuid").await, ["a", "b", "c"]);
    }

    /// Okta turns down the saved link (its `after` changed format, or the
    /// link went bad): the poll reads from `since` and saves Okta's new link,
    /// rather than asking for the dead one every poll for good.
    #[tokio::test]
    async fn a_saved_link_okta_turns_down_gives_way_to_since() {
        let okta = Okta::serve(vec![event("a"), event("b")]).await;
        let cfg: OktaConfig = serde_json::from_value(json!({})).unwrap();
        let client = reqwest::Client::new();
        let keeper = Keeper::new("okta");
        let dead = format!(
            "{}/api/v1/logs?after=gone&limit=1000&sortOrder=ASCENDING",
            okta.base
        );
        keeper.poller.save_link(&okta.base, &dead).await.unwrap();
        let n = poll(&client, &cfg, &okta.base, "t", &keeper.poller)
            .await
            .unwrap();
        assert_eq!(n, 2);
        let asked = okta.asked();
        assert!(asked[0].contains("after=gone"), "{asked:?}");
        assert!(asked[1].contains("since="), "{asked:?}");
        let (_, link) = keeper.poller.saved_link().await.unwrap().unwrap();
        assert_ne!(link, dead);
        okta.store("c");
        let n = poll(&client, &cfg, &okta.base, "t", &keeper.poller)
            .await
            .unwrap();
        assert_eq!(n, 1, "from Okta's new link");
        assert_eq!(keeper.sent("/uuid").await, ["a", "b", "c"]);
    }

    /// A 401 is the token's fault, not the link's: the saved link stays, so
    /// a fixed token resumes where the source stopped.
    #[tokio::test]
    async fn a_401_keeps_the_saved_link() {
        let okta = Okta::serve(vec![event("a"), event("b")]).await;
        let cfg: OktaConfig = serde_json::from_value(json!({})).unwrap();
        let client = reqwest::Client::new();
        let keeper = Keeper::new("okta");
        poll(&client, &cfg, &okta.base, "t", &keeper.poller)
            .await
            .unwrap();
        let before = keeper.poller.saved_link().await.unwrap();
        let err = poll(&client, &cfg, &okta.base, "revoked", &keeper.poller)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("401"), "{err:#}");
        assert_eq!(keeper.poller.saved_link().await.unwrap(), before);
        assert_eq!(okta.asked().len(), 3, "one request for the 401, no since");
    }

    #[tokio::test]
    async fn the_page_cap_leaves_the_rest_to_the_next_poll() {
        let t0 = DateTime::from_timestamp(Utc::now().timestamp() - 2 * 86_400, 0).unwrap();
        let events = (0..10_500)
            .map(|i| {
                let t = t0 + Duration::seconds(i);
                let v = json!({
                    "uuid": format!("e{i}"),
                    "published": t.to_rfc3339(),
                    "eventType": "user.session.start",
                });
                (t, v)
            })
            .collect();
        let base = oldest_first(events, |q| q["since"].parse().unwrap()).await;
        let cfg: OktaConfig = serde_json::from_value(json!({"since": "7d"})).unwrap();
        let client = reqwest::Client::new();
        let keeper = Keeper::new("okta");
        let n = poll(&client, &cfg, &base, "t", &keeper.poller)
            .await
            .unwrap();
        assert_eq!(n, 10_000, "up to the cap");
        let n = poll(&client, &cfg, &base, "t", &keeper.poller)
            .await
            .unwrap();
        assert_eq!(n, 500, "the rest, on the next poll");
        let all: Vec<String> = (0..10_500).map(|i| format!("e{i}")).collect();
        assert_eq!(keeper.sent("/uuid").await, all);
    }

    /// Page one is sent before page two answers 429; the next poll asks for
    /// page two (empty by then), not for page one again.
    #[tokio::test]
    async fn a_429_on_page_two_resumes_at_page_twos_link() {
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
        assert_eq!(n, 0, "page two's link, not page one again");
        assert_eq!(keeper.sent("/uuid").await, ["a", "b"]);
    }
}
