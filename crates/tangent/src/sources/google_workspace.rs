//! Google Workspace audit activity, polled from the Admin SDK Reports API as
//! a service account with domain-wide delegation acting for an admin.
//!
//! Each application is read on its own, from a cursor of its own less as
//! long as Google says its events can take to show up (`lateness`): four
//! hours for `token`, three for `calendar`, `groups` and
//! `groups_enterprise`, an hour for most.
//! State keys (namespace `sources`): `<source>:<application>:cursor`,
//! `<source>:<application>:seen:<uniqueQualifier>:<ms>` for an event sent,
//! kept three times the application's lateness, and
//! `<source>:<application>:gap` while a capped poll's older events wait.
//!
//! The Reports API serves newest first, so a poll that stops at its cap
//! (10,000 new events) has read the newest of its span and left the
//! oldest. The cursor moves on as usual, and what was left, from the
//! poll's start to its oldest event read, is kept as the application's
//! gap: the application's next polls read the gap, a cap's worth at a
//! time, before they go on from the cursor.

use std::sync::Arc;
use std::time::Duration as StdDuration;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tangent_shared::sources::google_workspace::GoogleWorkspaceConfig;
use tokio_util::sync::CancellationToken;

use crate::router::Router;
use crate::sources::poll::{idle, opt, parse_time, Poller, SOURCES_NS};
use crate::state::{StateStore, StateValue};

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

/// One application: its poller, with a cursor, seen ids and lateness of
/// its own, and the store its gap is kept in.
struct App {
    name: String,
    poller: Poller,
    store: Arc<dyn StateStore>,
}

/// Times an application's events were not read between, oldest first.
type Gap = (DateTime<Utc>, DateTime<Utc>);

impl App {
    fn gap_key(&self) -> String {
        format!("{}:gap", self.poller.name)
    }

    /// What a capped poll left unread, if anything.
    async fn gap(&self) -> Option<Gap> {
        let Ok(Some(StateValue::Str(s))) = self.store.get(SOURCES_NS, &self.gap_key()).await else {
            return None;
        };
        let (from, to) = s.split_once('/')?;
        Some((from.parse().ok()?, to.parse().ok()?))
    }

    async fn set_gap(&self, gap: Option<Gap>) {
        let key = self.gap_key();
        let saved = match gap {
            Some((from, to)) => {
                let v = StateValue::Str(format!("{}/{}", from.to_rfc3339(), to.to_rfc3339()));
                self.store.set(SOURCES_NS, &key, &v, Some(0)).await
            }
            None => self.store.del(SOURCES_NS, &key).await.map(drop),
        };
        if let Err(e) = saved {
            tracing::warn!(source = %self.poller.name, "saving gap: {e}");
        }
    }
}

/// Each application's poller.
fn pollers(
    name: &Arc<str>,
    cfg: &GoogleWorkspaceConfig,
    store: &Arc<dyn StateStore>,
    router: &Arc<Router>,
) -> Vec<App> {
    cfg.applications
        .iter()
        .map(|app| App {
            name: app.clone(),
            poller: Poller::part(
                Arc::clone(name),
                app,
                Arc::clone(store),
                Arc::clone(router),
                lateness(app),
            ),
            store: Arc::clone(store),
        })
        .collect()
}

/// Every application once, each from its own cursor less its own
/// lateness, the cursor then moving on its own (or, while it has one,
/// its gap): how many new events were forwarded. An application that
/// fails (a 429 on its pages, say) leaves its cursor where it was and
/// does not keep the ones after it from being read; the failures come
/// back together once all were tried.
async fn poll_all(
    client: &reqwest::Client,
    access: &str,
    cfg: &GoogleWorkspaceConfig,
    apps: &[App],
) -> Result<usize> {
    let mut total = 0;
    let mut failed = Vec::new();
    for app in apps {
        let gap = app.gap().await;
        let end = Utc::now();
        let (start, until) = match gap {
            Some((from, to)) => (from, Some(to)),
            None => (app.poller.start(&cfg.since).await, None),
        };
        match poll(client, access, &app.name, cfg, &app.poller, start, until).await {
            Ok(read) => {
                if gap.is_none() {
                    app.poller.finish(read.newest, Some(end)).await;
                }
                if gap.is_some() || read.unread_to.is_some() {
                    app.set_gap(read.unread_to.map(|to| (start, to))).await;
                }
                total += read.sent;
            }
            Err(e) => failed.push(format!("application {}: {e:#}", app.name)),
        }
    }
    if !failed.is_empty() {
        bail!("{}", failed.join("; "));
    }
    Ok(total)
}

/// What one poll of an application did.
#[derive(Debug)]
struct Read {
    /// New events forwarded.
    sent: usize,
    /// The newest new event's time.
    newest: Option<DateTime<Utc>>,
    /// When the poll stopped at its cap with pages left: up to when the
    /// events it asked for were not reached.
    unread_to: Option<DateTime<Utc>>,
}

/// One application's activity from `start` on, to `until` if given.
async fn poll(
    client: &reqwest::Client,
    access: &str,
    app: &str,
    cfg: &GoogleWorkspaceConfig,
    poller: &Poller,
    start: DateTime<Utc>,
    until: Option<DateTime<Utc>>,
) -> Result<Read> {
    let now = Utc::now();
    let mut newest: Option<DateTime<Utc>> = None;
    let mut oldest: Option<DateTime<Utc>> = None;
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
        if let Some(t) = until {
            req = req.query(&[(
                "endTime",
                t.to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
            )]);
        }
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
            let t = parse_time(&item["id"]["time"]).unwrap_or(now);
            oldest = Some(oldest.map_or(t, |o| o.min(t)));
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
    let sent = poller.emit(SOURCE, out).await?;
    // a second past the oldest event read, so that one's neighbours on the
    // next page (`endTime` is to the second) are not missed; the seen ids
    // drop the ones read already
    let unread_to = page_token.and(oldest).map(|o| o + Duration::seconds(1));
    Ok(Read {
        sent,
        newest,
        unread_to,
    })
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
    type Events = Arc<Mutex<Vec<(&'static str, String, DateTime<Utc>)>>>;

    /// A Reports API serving each application's events from the request's
    /// `startTime` on (to its `endTime`, if any), newest first, in pages of
    /// `maxResults`.
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
                    let end: Option<DateTime<Utc>> = q.get("endTime").map(|t| t.parse().unwrap());
                    let max: usize = q["maxResults"].parse().unwrap();
                    let skip: usize = q.get("pageToken").map_or(0, |t| t.parse().unwrap());
                    let mut items: Vec<_> = events
                        .lock()
                        .unwrap()
                        .iter()
                        .filter(|(a, _, t)| *a == app && *t >= start && end.is_none_or(|e| *t <= e))
                        .map(|(_, uq, t)| (*t, uq.clone()))
                        .collect();
                    items.sort_by_key(|&(t, _)| std::cmp::Reverse(t));
                    let more = items.len() > skip + max;
                    let items: Vec<Value> = items
                        .into_iter()
                        .skip(skip)
                        .take(max)
                        .map(|(t, uq)| {
                            let time = t.to_rfc3339_opts(SecondsFormat::Millis, true);
                            json!({"id": {"time": time, "uniqueQualifier": uq}})
                        })
                        .collect();
                    let mut body = json!({ "items": items });
                    if more {
                        body["nextPageToken"] = json!((skip + max).to_string());
                    }
                    std::future::ready(Json(body).into_response())
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
    ) -> (GoogleWorkspaceConfig, Vec<App>) {
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
            ("login", "l1".into(), login),
            ("token", "t1".into(), token),
        ]));
        let api = reports(Arc::clone(&events)).await;
        let keeper = Keeper::new("google_workspace");
        let (cfg, apps) = workspace(&keeper, &api, &["login", "token"], "1h");
        let client = reqwest::Client::new();
        let poll_both = || poll_all(&client, "t", &cfg, &apps);

        assert_eq!(poll_both().await.unwrap(), 2);
        assert_eq!(apps[0].poller.cursor().await, Some(login));
        assert_eq!(apps[1].poller.cursor().await, Some(token), "not login's");

        // only login has something new: only its cursor moves
        events.lock().unwrap().push(("login", "l2".into(), now));
        assert_eq!(poll_both().await.unwrap(), 1);
        assert_eq!(apps[0].poller.cursor().await, Some(now));
        assert_eq!(apps[1].poller.cursor().await, Some(token));

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
            ("login", "l1".into(), now - Duration::minutes(1)),
            ("token", "t1".into(), now - Duration::minutes(2)),
            ("drive", "d1".into(), drive),
        ]));
        // token answers 429, as when its pages run into the quota
        let api = reports_throttling(events, &["token"]).await;
        let keeper = Keeper::new("google_workspace");
        let (cfg, apps) = workspace(&keeper, &api, &["login", "token", "drive"], "1h");
        let client = reqwest::Client::new();

        let err = poll_all(&client, "t", &cfg, &apps).await.unwrap_err();
        assert!(format!("{err:#}").contains("application token"), "{err:#}");
        assert_eq!(apps[1].poller.cursor().await, None, "token's stays");
        assert_eq!(apps[2].poller.cursor().await, Some(drive), "drive was read");
        drop(apps);
        assert_eq!(keeper.sent("/id/uniqueQualifier").await, ["l1", "d1"]);
    }

    #[tokio::test]
    async fn a_token_event_two_hours_late_is_read_and_sent_once() {
        let now = Utc::now().trunc_subsecs(0);
        let events: Events = Arc::new(Mutex::new(vec![(
            "token",
            "on-time".into(),
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
            .push(("token", "late".into(), now - Duration::hours(2)));
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
        let events: Events = Arc::new(Mutex::new(vec![("token", "t1".into(), then)]));
        let api = reports(events).await;
        let keeper = Keeper::new("google_workspace");
        let (cfg, apps) = workspace(&keeper, &api, &["token"], "6h");
        let client = reqwest::Client::new();

        assert_eq!(poll_all(&client, "t", &cfg, &apps).await.unwrap(), 1);
        for _ in 0..2 {
            assert!(apps[0].poller.start(&cfg.since).await <= then, "read again");
            assert_eq!(poll_all(&client, "t", &cfg, &apps).await.unwrap(), 0);
        }
        drop(apps);
        assert_eq!(keeper.sent("/id/uniqueQualifier").await, ["t1"]);
    }

    #[tokio::test]
    async fn a_poll_that_stops_at_its_cap_leaves_the_older_events_to_the_next() {
        // more new drive events than a poll's cap, as after two days
        // stopped; the API serves them newest first
        let t0 = Utc::now().trunc_subsecs(0) - Duration::days(2);
        let ids: Vec<String> = (0..10_500).map(|i| format!("e{i:05}")).collect();
        let events: Events = Arc::new(Mutex::new(
            (0..10_500)
                .map(|i| ("drive", ids[i].clone(), t0 + Duration::seconds(i as i64)))
                .collect(),
        ));
        let api = reports(events).await;
        let keeper = Keeper::new("google_workspace");
        let (cfg, apps) = workspace(&keeper, &api, &["drive"], "7d");
        let client = reqwest::Client::new();
        let poll_drive = || poll_all(&client, "t", &cfg, &apps);

        assert_eq!(poll_drive().await.unwrap(), 10_000, "the newest");
        let (store, _) = keeper.parts();
        let gap = store.get(SOURCES_NS, "google_workspace:drive:gap").await;
        assert!(matches!(gap, Ok(Some(_))), "{gap:?}");
        assert_eq!(poll_drive().await.unwrap(), 500, "the rest, next");
        assert_eq!(apps[0].gap().await, None, "the gap is read");
        assert_eq!(poll_drive().await.unwrap(), 0, "and none again");
        drop((apps, store));
        let mut sent = keeper.sent("/id/uniqueQualifier").await;
        sent.sort();
        assert_eq!(sent, ids, "each once");
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
        let start = Utc::now() - Duration::hours(1);
        let poll_login = || poll(&client, "t", "login", &cfg, &keeper.poller, start, None);
        let err = poll_login().await.unwrap_err();
        assert!(err.to_string().contains("429"), "{err:#}");
        let read = poll_login().await.unwrap();
        assert_eq!(read.sent, 2, "page one, on the next poll");
        let read = poll_login().await.unwrap();
        assert_eq!(read.sent, 0, "and only once");
        assert_eq!(keeper.sent("/id/uniqueQualifier").await, ["a", "b"]);
    }
}
