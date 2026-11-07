//! Google Workspace audit activity, polled from the Admin SDK Reports API as
//! a service account with domain-wide delegation acting for an admin.
//!
//! Each application is read on its own, from a cursor of its own less as
//! long as Google says its events can take to show up (`lateness`): four
//! hours for `token`, three for `calendar`, `groups` and
//! `groups_enterprise`, an hour for most.
//! State keys (namespace `sources`): `<source>:<application>:cursor`, and
//! `<source>:<application>:seen:<uniqueQualifier>:<ms>` for an event sent,
//! kept three times the application's lateness.

use std::sync::Arc;
use std::time::Duration as StdDuration;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tangent_shared::sources::google_workspace::GoogleWorkspaceConfig;
use tokio_util::sync::CancellationToken;

use crate::router::Router;
use crate::sources::poll::{idle, opt, parse_time, Poller};
use crate::state::StateStore;

const SOURCE: &str = "google_workspace";
const SCOPE: &str = "https://www.googleapis.com/auth/admin.reports.audit.readonly";

/// How late an application's events can show up, so how far back each of
/// its polls reads: the most Google's table of lag times gives for its log
/// events (not its reports), rounded up to the hour,
/// <https://knowledge.workspace.google.com/admin/reports/data-retention-and-lag-times>.
/// An application the table gives in minutes, or does not list, gets an
/// hour.
fn lateness(app: &str) -> Duration {
    Duration::hours(match app {
        // Token log events: "A couple of hours". OAuth log events (an
        // app's grants and API calls, which the Reports API serves as
        // `token`): "Up to a few hours".
        "token" => 4,
        // "Up to a few hours"
        "cloud_search" => 4,
        // "Tens of minutes (can also go up to a couple of hours)". The
        // table has no row for `groups_enterprise` (groups and memberships
        // changed in the Admin console or through the APIs), though its
        // own page sends you to the table for its lag: it gets Groups'.
        "calendar" | "groups" | "groups_enterprise" => 3,
        // a process finishing: "Depends on the size of the data, up to
        // many days" (its start: "Near real time"); there is no maximum,
        // and a week of Takeout is a page
        "takeout" => 7 * 24,
        // "Near real time (couple of minutes)" for most; user accounts
        // "Tens of minutes"; quota "5–10 minutes"
        _ => 1,
    })
}

#[derive(Deserialize)]
struct ServiceAccount {
    client_email: String,
    private_key: String,
    #[serde(default = "default_token_uri")]
    token_uri: String,
}

fn default_token_uri() -> String {
    "https://oauth2.googleapis.com/token".into()
}

#[derive(Serialize)]
struct Claims<'a> {
    iss: &'a str,
    sub: &'a str,
    scope: &'a str,
    aud: &'a str,
    iat: i64,
    exp: i64,
}

struct Token {
    value: String,
    expires: DateTime<Utc>,
}

pub async fn run_consumer(
    name: Arc<str>,
    cfg: GoogleWorkspaceConfig,
    router: Arc<Router>,
    store: Arc<dyn StateStore>,
    shutdown: CancellationToken,
) -> Result<()> {
    let (Some(credentials_file), Some(subject)) = (opt(&cfg.credentials_file), opt(&cfg.subject))
    else {
        idle(&name, "google_workspace credentials_file or subject");
        shutdown.cancelled().await;
        return Ok(());
    };
    let account: ServiceAccount = serde_json::from_str(
        &std::fs::read_to_string(credentials_file)
            .with_context(|| format!("reading {credentials_file}"))?,
    )
    .context("service account key file")?;
    let signing = jsonwebtoken::EncodingKey::from_rsa_pem(account.private_key.as_bytes())
        .context("service account private key")?;

    let apps = pollers(&name, &cfg, &store, &router);
    let client = reqwest::Client::builder()
        .user_agent("tangent")
        .timeout(StdDuration::from_secs(30))
        .build()?;
    let mut ticker = tokio::time::interval(StdDuration::from_secs(cfg.interval_secs.max(10)));
    let mut token: Option<Token> = None;
    let mut failing = false;
    tracing::info!(source = %name, subject, apps = ?cfg.applications, "google_workspace source polling");

    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            _ = ticker.tick() => {
                let result = async {
                    if token.as_ref().is_none_or(|t| t.expires <= Utc::now() + Duration::minutes(2)) {
                        token = Some(fetch_token(&client, &account, &signing, subject).await?);
                    }
                    let access = &token.as_ref().expect("token").value;
                    poll_all(&client, access, &cfg, &apps).await
                }
                .await;
                match result {
                    Ok(n) => {
                        failing = false;
                        if n > 0 {
                            tracing::debug!(source = %name, events = n, "google_workspace poll");
                        }
                    }
                    Err(e) if !failing => {
                        failing = true;
                        token = None;
                        tracing::warn!(source = %name, "google_workspace poll failed (retrying each interval): {e:#}");
                    }
                    Err(e) => {
                        token = None;
                        tracing::debug!(source = %name, "google_workspace poll failed: {e:#}");
                    }
                }
            }
        }
    }
    Ok(())
}

/// A JWT signed with the service account's key, exchanged for an access token.
async fn fetch_token(
    client: &reqwest::Client,
    account: &ServiceAccount,
    signing: &jsonwebtoken::EncodingKey,
    subject: &str,
) -> Result<Token> {
    let now = Utc::now().timestamp();
    let claims = Claims {
        iss: &account.client_email,
        sub: subject,
        scope: SCOPE,
        aud: &account.token_uri,
        iat: now,
        exp: now + 3600,
    };
    let assertion = jsonwebtoken::encode(
        &jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256),
        &claims,
        signing,
    )
    .context("signing the service account assertion")?;
    let resp = client
        .post(&account.token_uri)
        .form(&[
            ("grant_type", "urn:ietf:params:oauth:grant-type:jwt-bearer"),
            ("assertion", assertion.as_str()),
        ])
        .send()
        .await
        .context("token request")?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        bail!(
            "token {status}: {}",
            body.chars().take(200).collect::<String>()
        );
    }
    let v: Value = resp.json().await.context("token body")?;
    let value = v["access_token"]
        .as_str()
        .context("token body has no access_token")?
        .to_string();
    let secs = v["expires_in"].as_i64().unwrap_or(3600);
    Ok(Token {
        value,
        expires: Utc::now() + Duration::seconds(secs),
    })
}

/// A poller for each application, with a cursor, seen ids and lateness of
/// its own.
fn pollers(
    name: &Arc<str>,
    cfg: &GoogleWorkspaceConfig,
    store: &Arc<dyn StateStore>,
    router: &Arc<Router>,
) -> Vec<(String, Poller)> {
    cfg.applications
        .iter()
        .map(|app| {
            let poller = Poller::part(
                Arc::clone(name),
                app,
                Arc::clone(store),
                Arc::clone(router),
                lateness(app),
            );
            (app.clone(), poller)
        })
        .collect()
}

/// Every application once, each from its own cursor less its own
/// lateness, the cursor then moving on its own: how many new events were
/// forwarded. An application that fails (a 429 on its pages, say) leaves
/// its cursor where it was and does not keep the ones after it from being
/// read; the failures come back together once all were tried.
async fn poll_all(
    client: &reqwest::Client,
    access: &str,
    cfg: &GoogleWorkspaceConfig,
    apps: &[(String, Poller)],
) -> Result<usize> {
    let mut total = 0;
    let mut failed = Vec::new();
    for (app, poller) in apps {
        let start = poller.start(&cfg.since).await;
        let end = Utc::now();
        match poll(client, access, app, cfg, poller, start, end).await {
            Ok((n, newest)) => {
                poller.finish(newest, Some(end)).await;
                total += n;
            }
            Err(e) => failed.push(format!("application {app}: {e:#}")),
        }
    }
    if !failed.is_empty() {
        bail!("{}", failed.join("; "));
    }
    Ok(total)
}

/// One application's activity from `start` on: how many new events were
/// forwarded, and the newest event's time.
async fn poll(
    client: &reqwest::Client,
    access: &str,
    app: &str,
    cfg: &GoogleWorkspaceConfig,
    poller: &Poller,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<(usize, Option<DateTime<Utc>>)> {
    let mut newest: Option<DateTime<Utc>> = None;
    let mut out: Vec<(DateTime<Utc>, String, Value)> = Vec::new();
    let mut page_token: Option<String> = None;
    loop {
        let mut req = client
            .get(format!(
                "{}/admin/reports/v1/activity/users/all/applications/{app}",
                cfg.api.trim_end_matches('/')
            ))
            .bearer_auth(access)
            .query(&[
                (
                    "startTime",
                    start.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
                ),
                ("maxResults", "1000".to_string()),
            ]);
        if let Some(t) = &page_token {
            req = req.query(&[("pageToken", t)]);
        }
        let resp = req.send().await.context("activities request")?;
        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            bail!(
                "activities {status}: {}",
                body.chars().take(200).collect::<String>()
            );
        }
        let v: Value = resp.json().await.context("activities body")?;
        for item in v["items"].as_array().into_iter().flatten() {
            let t = parse_time(&item["id"]["time"]).unwrap_or(end);
            let id = format!(
                "{}:{}",
                item["id"]["uniqueQualifier"].as_str().unwrap_or(""),
                t.timestamp_millis()
            );
            if poller.seen(&id).await {
                continue;
            }
            newest = Some(newest.map_or(t, |n| n.max(t)));
            out.push((t, id, item.clone()));
        }
        page_token = v["nextPageToken"].as_str().map(str::to_string);
        if page_token.is_none() || out.len() >= 10_000 {
            break;
        }
    }
    let n = poller.emit(SOURCE, out).await?;
    Ok((n, newest))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Mutex;

    use axum::{
        extract::Query,
        http::{StatusCode, Uri},
        response::IntoResponse,
        Json,
    };
    use chrono::{SecondsFormat, SubsecRound};
    use serde_json::json;

    use super::*;
    use crate::sources::poll::testing::{serve, Keeper};
    use crate::sources::poll::SOURCES_NS;

    /// Events the API holds: (application, uniqueQualifier, time). A test
    /// adds to them between polls, as Google does when an event shows up
    /// late.
    type Events = Arc<Mutex<Vec<(&'static str, &'static str, DateTime<Utc>)>>>;

    /// A Reports API serving each application's events from the request's
    /// `startTime` on, newest first, in one page.
    async fn reports(events: Events) -> String {
        reports_throttling(events, &[]).await
    }

    /// The same, answering 429 for each application in `throttled`.
    async fn reports_throttling(events: Events, throttled: &'static [&'static str]) -> String {
        serve(|_| {
            axum::Router::new().fallback(
                move |uri: Uri, Query(q): Query<HashMap<String, String>>| {
                    let app = uri
                        .path()
                        .rsplit('/')
                        .next()
                        .unwrap_or_default()
                        .to_string();
                    if throttled.contains(&app.as_str()) {
                        let resp = StatusCode::TOO_MANY_REQUESTS.into_response();
                        return std::future::ready(resp);
                    }
                    let start: DateTime<Utc> = q["startTime"].parse().unwrap();
                    let mut items: Vec<_> = events
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|(a, _, t)| *a == app && *t >= start)
                        .map(|&(_, uq, t)| (t, uq))
                        .collect();
                    items.sort_by_key(|&(t, _)| std::cmp::Reverse(t));
                    let items: Vec<Value> = items
                        .into_iter()
                        .map(|(t, uq)| {
                            let time = t.to_rfc3339_opts(SecondsFormat::Millis, true);
                            json!({"id": {"time": time, "uniqueQualifier": uq}})
                        })
                        .collect();
                    std::future::ready(Json(json!({ "items": items })).into_response())
                },
            )
        })
        .await
    }

    /// A config reading `applications` from `api`, and their pollers, which
    /// write to `keeper`'s store and sink.
    fn workspace(
        keeper: &Keeper,
        api: &str,
        applications: &[&str],
        since: &str,
    ) -> (GoogleWorkspaceConfig, Vec<(String, Poller)>) {
        let cfg: GoogleWorkspaceConfig = serde_json::from_value(
            json!({"api": api, "applications": applications, "since": since}),
        )
        .unwrap();
        let (store, router) = keeper.parts();
        let apps = pollers(&Arc::from("google_workspace"), &cfg, &store, &router);
        (cfg, apps)
    }

    #[tokio::test]
    async fn each_applications_cursor_moves_on_its_own() {
        let now = Utc::now().trunc_subsecs(0);
        let (login, token) = (now - Duration::minutes(1), now - Duration::minutes(10));
        let events: Events = Arc::new(Mutex::new(vec![
            ("login", "l1", login),
            ("token", "t1", token),
        ]));
        let api = reports(Arc::clone(&events)).await;
        let keeper = Keeper::new("google_workspace");
        let (cfg, apps) = workspace(&keeper, &api, &["login", "token"], "1h");
        let client = reqwest::Client::new();
        let poll_both = || poll_all(&client, "t", &cfg, &apps);

        assert_eq!(poll_both().await.unwrap(), 2);
        assert_eq!(apps[0].1.cursor().await, Some(login));
        assert_eq!(apps[1].1.cursor().await, Some(token), "not login's");

        // only login has something new: only its cursor moves
        events.lock().unwrap().push(("login", "l2", now));
        assert_eq!(poll_both().await.unwrap(), 1);
        assert_eq!(apps[0].1.cursor().await, Some(now));
        assert_eq!(apps[1].1.cursor().await, Some(token));

        let (store, _) = keeper.parts();
        let t1 = format!(
            "google_workspace:token:seen:t1:{}",
            token.timestamp_millis()
        );
        for key in ["google_workspace:token:cursor", t1.as_str()] {
            assert!(store.get(SOURCES_NS, key).await.unwrap().is_some(), "{key}");
        }
        drop((apps, store));
        assert_eq!(keeper.sent("/id/uniqueQualifier").await, ["l1", "t1", "l2"]);
    }

    #[tokio::test]
    async fn an_application_that_fails_does_not_hold_up_the_ones_after_it() {
        let now = Utc::now().trunc_subsecs(0);
        let drive = now - Duration::minutes(3);
        let events: Events = Arc::new(Mutex::new(vec![
            ("login", "l1", now - Duration::minutes(1)),
            ("token", "t1", now - Duration::minutes(2)),
            ("drive", "d1", drive),
        ]));
        // token answers 429, as when its pages run into the quota
        let api = reports_throttling(events, &["token"]).await;
        let keeper = Keeper::new("google_workspace");
        let (cfg, apps) = workspace(&keeper, &api, &["login", "token", "drive"], "1h");
        let client = reqwest::Client::new();

        let err = poll_all(&client, "t", &cfg, &apps).await.unwrap_err();
        assert!(format!("{err:#}").contains("application token"), "{err:#}");
        assert_eq!(apps[1].1.cursor().await, None, "token's stays");
        assert_eq!(apps[2].1.cursor().await, Some(drive), "drive was read");
        drop(apps);
        assert_eq!(keeper.sent("/id/uniqueQualifier").await, ["l1", "d1"]);
    }

    #[tokio::test]
    async fn a_token_event_two_hours_late_is_read_and_sent_once() {
        let now = Utc::now().trunc_subsecs(0);
        let events: Events = Arc::new(Mutex::new(vec![(
            "token",
            "on-time",
            now - Duration::minutes(1),
        )]));
        let api = reports(Arc::clone(&events)).await;
        let keeper = Keeper::new("google_workspace");
        let (cfg, apps) = workspace(&keeper, &api, &["token"], "1h");
        let client = reqwest::Client::new();
        let poll_token = || poll_all(&client, "t", &cfg, &apps);

        assert_eq!(poll_token().await.unwrap(), 1);
        // an event from two hours ago shows up only now, as Google says
        // token events can
        events
            .lock()
            .unwrap()
            .push(("token", "late", now - Duration::hours(2)));
        assert_eq!(poll_token().await.unwrap(), 1, "inside token's lookback");
        assert_eq!(poll_token().await.unwrap(), 0, "and sent once");
        drop(apps);
        assert_eq!(
            keeper.sent("/id/uniqueQualifier").await,
            ["on-time", "late"]
        );
    }

    #[tokio::test]
    async fn a_repeat_inside_an_applications_lookback_is_sent_once() {
        // three hours ago: past an hour, inside token's lookback, so every
        // poll reads it again
        let then = Utc::now().trunc_subsecs(0) - Duration::hours(3);
        let events: Events = Arc::new(Mutex::new(vec![("token", "t1", then)]));
        let api = reports(events).await;
        let keeper = Keeper::new("google_workspace");
        let (cfg, apps) = workspace(&keeper, &api, &["token"], "6h");
        let client = reqwest::Client::new();

        assert_eq!(poll_all(&client, "t", &cfg, &apps).await.unwrap(), 1);
        for _ in 0..2 {
            assert!(apps[0].1.start(&cfg.since).await <= then, "read again");
            assert_eq!(poll_all(&client, "t", &cfg, &apps).await.unwrap(), 0);
        }
        drop(apps);
        assert_eq!(keeper.sent("/id/uniqueQualifier").await, ["t1"]);
    }

    #[tokio::test]
    async fn a_429_on_page_two_leaves_page_one_to_the_next_poll() {
        let page_one = json!({
            "items": [
                {"id": {"time": "2026-11-06T12:00:00.000Z", "uniqueQualifier": "a"}},
                {"id": {"time": "2026-11-06T12:00:01.000Z", "uniqueQualifier": "b"}},
            ],
            "nextPageToken": "2",
        });
        // the second page answers 429 the first time and is empty after
        let throttled = Arc::new(AtomicBool::new(false));
        let base = serve(|_| {
            axum::Router::new().fallback(move |Query(q): Query<HashMap<String, String>>| {
                let resp = if !q.contains_key("pageToken") {
                    Json(page_one.clone()).into_response()
                } else if throttled.swap(true, Ordering::SeqCst) {
                    Json(json!({})).into_response()
                } else {
                    StatusCode::TOO_MANY_REQUESTS.into_response()
                };
                async move { resp }
            })
        })
        .await;
        let cfg: GoogleWorkspaceConfig = serde_json::from_value(json!({"api": base})).unwrap();
        let client = reqwest::Client::new();
        let keeper = Keeper::new("google_workspace");
        let (start, end) = (Utc::now() - Duration::hours(1), Utc::now());
        let poll_login = || poll(&client, "t", "login", &cfg, &keeper.poller, start, end);
        let err = poll_login().await.unwrap_err();
        assert!(err.to_string().contains("429"), "{err:#}");
        let (n, _) = poll_login().await.unwrap();
        assert_eq!(n, 2, "page one, on the next poll");
        let (n, _) = poll_login().await.unwrap();
        assert_eq!(n, 0, "and only once");
        assert_eq!(keeper.sent("/id/uniqueQualifier").await, ["a", "b"]);
    }
}
