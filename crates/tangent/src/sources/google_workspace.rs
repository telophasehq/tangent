//! Google Workspace audit activity, polled from the Admin SDK Reports API as
//! a service account with domain-wide delegation acting for an admin.

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
/// Some applications' activity shows up an hour late; look back that far.
const LATE_MS: i64 = 60 * 60 * 1000;

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
                    let mut total = 0;
                    // One cursor for all applications: the poll reads them all
                    // from the same start, then moves it.
                    let start = poller.start(&cfg.since).await;
                    let end = Utc::now();
                    let mut newest = None;
                    for app in &cfg.applications {
                        let (n, t) = poll(&client, access, app, &cfg, &poller, start, end)
                            .await
                            .with_context(|| format!("application {app}"))?;
                        total += n;
                        newest = match (newest, t) {
                            (Some(a), Some(b)) => Some(std::cmp::max::<DateTime<Utc>>(a, b)),
                            (a, b) => a.or(b),
                        };
                    }
                    poller.finish(newest, end).await;
                    Ok::<usize, anyhow::Error>(total)
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
    let mut out: Vec<(DateTime<Utc>, Value)> = Vec::new();
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
                "{app}:{}:{}",
                item["id"]["uniqueQualifier"].as_str().unwrap_or(""),
                t.timestamp_millis()
            );
            if !poller.first_sight(&id).await {
                continue;
            }
            newest = Some(newest.map_or(t, |n| n.max(t)));
            out.push((t, item.clone()));
        }
        page_token = v["nextPageToken"].as_str().map(str::to_string);
        if page_token.is_none() || out.len() >= 10_000 {
            break;
        }
    }
    let n = poller.emit(SOURCE, out).await?;
    Ok((n, newest))
}
