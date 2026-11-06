use std::{io::Read, sync::Arc};

use anyhow::{anyhow, Context, Result};
use axum::{
    body::Body,
    extract::State,
    http::{Method, Request, StatusCode},
    response::IntoResponse,
    routing::post,
    serve, Router as AxumRouter,
};
use bytes::{BufMut, BytesMut};
use hmac::{Hmac, Mac};
use http_body_util::BodyExt;
use lazy_static::lazy_static;
use memchr::memchr;
use regex::Regex;
use secrecy::ExposeSecret;
use serde_json::{json, Value};
use sha2::Sha256;
use tangent_shared::dag::NodeRef;
use tangent_shared::sources::github_webhook::GithubWebhookConfig;
use tokio::{net::TcpListener, sync::mpsc};
use tokio_util::sync::CancellationToken;

use crate::router::Router;

type HmacSha256 = Hmac<Sha256>;

const LOGS_FETCHER_CHANNEL_CAPACITY: usize = 512;

#[derive(Clone)]
struct WebhookState {
    cfg: Arc<GithubWebhookConfig>,
    logs_tx: mpsc::Sender<BytesMut>,
}

/// A delivery whose `X-Hub-Signature-256` is missing, malformed, or not the
/// body's under `secret`. It is answered 401.
#[derive(Debug)]
struct BadSignature(&'static str);

impl std::fmt::Display for BadSignature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0)
    }
}

impl std::error::Error for BadSignature {}

lazy_static! {
    static ref GITHUB_LOG_LINE_RE: Regex = Regex::new(
        r"^(?P<ts>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2}))\s+(?P<msg>.*)$"
    )
    .expect("github log timestamp regex should compile");
}

fn split_timestamp_and_message(line: &str) -> (Option<String>, String) {
    let sanitized = line.trim_start_matches('\u{feff}');

    if let Some(captures) = GITHUB_LOG_LINE_RE.captures(sanitized) {
        let ts = captures.name("ts").map(|m| m.as_str().to_string());
        let msg = captures
            .name("msg")
            .map(|m| m.as_str())
            .unwrap_or_default()
            .trim()
            .to_string();

        return (ts, msg);
    }

    (None, sanitized.to_string())
}

fn drain_ndjson_lines(buf: &mut BytesMut) -> Vec<BytesMut> {
    let mut out = Vec::with_capacity(16);

    while let Some(nl) = memchr(b'\n', &buf[..]) {
        let line = buf.split_to(nl + 1);
        out.push(line);
    }

    out
}

const API_BASE: &str = "https://api.github.com";

/// Run an HTTP server that accepts GitHub webhooks and forwards the payloads
/// into the Router as NDJSON frames.
///
/// Each request body is forwarded as **one line** of NDJSON (with a trailing \n).
pub async fn run_consumer(
    name: Arc<str>,
    cfg: GithubWebhookConfig,
    router: Arc<Router>,
    shutdown: CancellationToken,
) -> Result<()> {
    let router = router.clone();
    let cfg = Arc::new(cfg);
    let (err_tx, mut err_rx) = mpsc::channel::<anyhow::Error>(64);
    let (logs_tx, logs_rx) = mpsc::channel::<BytesMut>(LOGS_FETCHER_CHANNEL_CAPACITY);

    let fetcher_err_tx = err_tx;
    let fetcher_router = router.clone();
    let fetcher_shutdown = shutdown.clone();
    let fetcher_name = name.clone();
    let fetcher_token = cfg.token.expose_secret().to_string();

    tokio::spawn(async move {
        if let Err(e) = run_logs_fetcher(
            fetcher_name,
            fetcher_token,
            fetcher_router,
            logs_rx,
            fetcher_shutdown,
        )
        .await
        {
            let _ = fetcher_err_tx.send(e).await;
        }
    });

    let state = WebhookState {
        cfg: cfg.clone(),
        logs_tx,
    };

    let listener = TcpListener::bind(cfg.bind_address).await.with_context(|| {
        format!(
            "failed to bind github webhook listener on {}",
            cfg.bind_address
        )
    })?;

    let app = AxumRouter::new()
        .route(cfg.path.as_str(), post(webhook_handler))
        .with_state(state);

    let server = serve(listener, app).with_graceful_shutdown(async move {
        shutdown.cancelled().await;
    });

    tracing::info!("github webhook source listening on {:?}", &cfg.bind_address);

    tokio::select! {
        res = server => {
            if let Err(e) = res {
                return Err(anyhow!("github webhook server error: {e}"));
            }
        }
        Some(e) = err_rx.recv() => {
            return Err(e);
        }
    }

    Ok(())
}

async fn webhook_handler(
    State(state): State<WebhookState>,
    req: Request<Body>,
) -> impl IntoResponse {
    let result = handle_request(req, state.cfg.clone(), state.logs_tx.clone()).await;
    if let Err(err) = result {
        // A delivery that fails is refused, and the source keeps listening:
        // the failure is the sender's (a bad signature, a body cut short),
        // and anyone who can reach the port can send one.
        tracing::warn!("github webhook delivery refused: {err:#}");
        if err.is::<BadSignature>() {
            return (StatusCode::UNAUTHORIZED, "bad signature");
        }
        return (StatusCode::INTERNAL_SERVER_ERROR, "internal error");
    }

    (StatusCode::OK, "ok")
}

/// Handle a single GitHub webhook HTTP request.
async fn handle_request(
    req: Request<Body>,
    cfg: Arc<GithubWebhookConfig>,
    logs_tx: mpsc::Sender<BytesMut>,
) -> Result<()> {
    let (parts, body) = req.into_parts();

    if parts.method != Method::POST || parts.uri.path() != cfg.path {
        return Err(anyhow!(
            "invalid request: method={} path={}",
            parts.method,
            parts.uri.path()
        ));
    }

    let event = parts
        .headers
        .get("X-GitHub-Event")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if event == "ping" {
        consume_body(body).await?;
        return Ok(());
    }

    // Verify HMAC if secret is configured
    let sig_header = parts
        .headers
        .get("X-Hub-Signature-256")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_string());

    let mut buf = read_body(body).await?;

    if let Some(secret) = &cfg.secret {
        let provided = sig_header.ok_or(BadSignature("missing X-Hub-Signature-256 header"))?;

        if !provided.starts_with("sha256=") {
            return Err(BadSignature("invalid X-Hub-Signature-256 format").into());
        }
        let provided_sig = &provided["sha256=".len()..];

        let mut mac = HmacSha256::new_from_slice(secret.expose_secret().as_bytes())
            .map_err(|e| anyhow!("invalid HMAC key: {e}"))?;
        mac.update(&buf);

        let expected = hex::encode(mac.finalize().into_bytes());
        if !constant_time_eq::constant_time_eq(expected.as_bytes(), provided_sig.as_bytes()) {
            return Err(BadSignature("invalid webhook signature").into());
        }
    }

    if !buf.ends_with(b"\n") {
        buf.put_u8(b'\n');
    }

    let frames = drain_ndjson_lines(&mut buf);
    if frames.is_empty() {
        return Ok(());
    }

    for frame in frames {
        logs_tx
            .send(frame)
            .await
            .map_err(|_| anyhow!("github logs fetcher channel closed"))?;
    }

    Ok(())
}

async fn consume_body(body: Body) -> Result<()> {
    let _ = body.collect().await?;
    Ok(())
}

async fn read_body(body: Body) -> Result<BytesMut> {
    let bytes = body.collect().await?.to_bytes();
    Ok(BytesMut::from(bytes.as_ref()))
}

/// Main loop: consume webhook frames, fetch logs on workflow_run.completed,
/// and emit log-line events via Router.
pub async fn run_logs_fetcher(
    name: Arc<str>,
    token: String,
    router: Arc<Router>,
    mut rx: mpsc::Receiver<BytesMut>, // frames from the webhook source
    shutdown: CancellationToken,
) -> Result<()> {
    let from = NodeRef::Source { name };

    let client = reqwest::Client::new();

    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,

            maybe_frame = rx.recv() => {
                let frame = match maybe_frame {
                    Some(f) => f,
                    None => break, // channel closed
                };

                if let Err(e) = process_webhook_frame(
                    &client,
                    &token,
                    &router,
                    &from,
                    frame,
                ).await {
                    tracing::warn!("github logs fetcher error: {e:#}");
                    // non-fatal: keep going
                }
            }
        }
    }

    Ok(())
}

/// Handle a single webhook frame:
/// - parse JSON
/// - if it's workflow_run.completed, fetch logs
/// - emit log lines as new NDJSON events
async fn process_webhook_frame(
    client: &reqwest::Client,
    token: &String,
    router: &Arc<Router>,
    from: &NodeRef,
    mut frame: BytesMut,
) -> Result<()> {
    // Frame should be exactly one JSON object line from webhook source
    // Strip trailing newline (if any)
    if frame.ends_with(b"\n") {
        frame.truncate(frame.len() - 1);
    }

    if frame.is_empty() {
        return Ok(());
    }

    let v: Value = serde_json::from_slice(&frame).context("failed to parse github webhook JSON")?;

    // We only care about workflow_run events
    let Some(workflow_run) = v.get("workflow_run") else {
        router
            .forward(from, vec![frame], Vec::new())
            .await
            .context("router.forward failed for github logs")?;

        return Ok(());
    };

    let sha = workflow_run
        .get("head_sha")
        .ok_or_else(|| anyhow!("workflow_run.head_sha missing"))?;
    // action: "completed", "requested", etc
    let action = v.get("action").and_then(|a| a.as_str()).unwrap_or("");

    if action != "completed" {
        router
            .forward(from, vec![frame], Vec::new())
            .await
            .context("router.forward failed for github logs")?;

        return Ok(());
    }

    let run_id = workflow_run
        .get("id")
        .and_then(|id| id.as_u64())
        .ok_or_else(|| anyhow!("workflow_run.id missing or not a u64"))?;

    // Repo info: workflow_run.repository.{owner.login, name}
    let repo = workflow_run
        .get("repository")
        .ok_or_else(|| anyhow!("workflow_run.repository missing"))?;

    let full_name = repo.get("full_name").and_then(|f| f.as_str());

    let (owner, repo_name) = if let Some(full) = full_name {
        match full.split_once('/') {
            Some((o, r)) => (o.to_string(), r.to_string()),
            None => {
                let name = repo
                    .get("name")
                    .and_then(|n| n.as_str())
                    .ok_or_else(|| anyhow!("repository.name missing"))?;
                let owner = repo
                    .get("owner")
                    .and_then(|o| o.get("login"))
                    .and_then(|l| l.as_str())
                    .ok_or_else(|| anyhow!("repository.owner.login missing"))?;
                (owner.to_string(), name.to_string())
            }
        }
    } else {
        let name = repo
            .get("name")
            .and_then(|n| n.as_str())
            .ok_or_else(|| anyhow!("repository.name missing"))?;
        let owner = repo
            .get("owner")
            .and_then(|o| o.get("login"))
            .and_then(|l| l.as_str())
            .ok_or_else(|| anyhow!("repository.owner.login missing"))?;
        (owner.to_string(), name.to_string())
    };

    let logs_url = format!(
        "{}/repos/{owner}/{repo}/actions/runs/{run_id}/logs",
        API_BASE,
        owner = owner,
        repo = repo_name,
        run_id = run_id,
    );

    tracing::info!(%owner, %repo_name, %run_id, "fetching github actions logs");

    let resp = client
        .get(&logs_url)
        .bearer_auth(token)
        .header("User-Agent", "tangent-logs")
        .send()
        .await
        .with_context(|| format!("request failed for {logs_url}"))?;

    let resp = resp
        .error_for_status()
        .with_context(|| format!("github returned error for {logs_url}"))?;

    let bytes = resp.bytes().await.context("failed to read logs zip body")?;

    let cursor = std::io::Cursor::new(bytes);
    let mut archive = zip::ZipArchive::new(cursor).context("failed to open logs zip")?;

    let mut out_frames: Vec<BytesMut> = vec![frame];

    for i in 0..archive.len() {
        let mut file = archive
            .by_index(i)
            .with_context(|| format!("failed to read zip entry {i}"))?;

        if !file.is_file() {
            continue;
        }

        let file_name = file.name().to_string();
        let mut contents = String::new();
        file.read_to_string(&mut contents)
            .with_context(|| format!("failed to read zip entry {}", file_name))?;

        for line in contents.lines() {
            let line_trimmed = line.trim();
            if line_trimmed.is_empty() {
                continue;
            }

            let (timestamp, message) = split_timestamp_and_message(line_trimmed);

            let mut event = json!({
                "kind": "github_ci_log",
                "github": {
                    "run_id": run_id,
                    "repo": format!("{}/{}", owner, repo_name),
                    "sha": sha,
                    "log_file": file_name,
                },
                "message": message,
            });

            if let Some(ts) = timestamp {
                event
                    .as_object_mut()
                    .expect("github log event should be an object")
                    .insert("key".to_string(), json!({ "timestamp": ts }));
            }

            let mut buf = BytesMut::with_capacity(256);
            buf.extend_from_slice(event.to_string().as_bytes());
            buf.extend_from_slice(b"\n");
            out_frames.push(buf);
        }
    }

    if !out_frames.is_empty() {
        router
            .forward(from, out_frames, Vec::new())
            .await
            .context("router.forward failed for github logs")?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::{Duration, Instant};

    use super::*;
    use crate::sinks::manager::{Sink, SinkManager, SinkWrite};

    /// What reached the sink, a write at a time.
    #[derive(Default)]
    struct Deliveries(tokio::sync::Mutex<Vec<BytesMut>>);

    #[async_trait::async_trait]
    impl Sink for Deliveries {
        async fn write(&self, req: SinkWrite) -> Result<()> {
            self.0.lock().await.push(req.payload);
            Ok(())
        }
    }

    /// `X-Hub-Signature-256` for `body` under the secret `s3cret`.
    fn signature(body: &str) -> String {
        let mut mac = HmacSha256::new_from_slice(b"s3cret").unwrap();
        mac.update(body.as_bytes());
        format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
    }

    /// POST `body` to the webhook, with `signature` if any: the status.
    async fn deliver(addr: SocketAddr, body: &'static str, signature: Option<String>) -> u16 {
        let mut req = reqwest::Client::new()
            .post(format!("http://{addr}/webhook"))
            .header("X-GitHub-Event", "push")
            .body(body);
        if let Some(signature) = signature {
            req = req.header("X-Hub-Signature-256", signature);
        }
        let resp = req.send().await.expect("the webhook is listening");
        resp.status().as_u16()
    }

    /// A delivery without a good signature is refused with 401, and the
    /// source takes the next: one used to end the source, closing the port
    /// until tangent restarted.
    #[tokio::test]
    async fn a_badly_signed_delivery_is_refused_and_a_signed_one_after_it_taken() {
        let deliveries = Arc::new(Deliveries::default());
        let sink: Arc<dyn Sink> = deliveries.clone();
        let sinks = SinkManager::for_test(vec![(Arc::from("deliveries"), sink)], 1);
        let mut outs = ahash::AHashMap::default();
        outs.insert(
            NodeRef::Source {
                name: Arc::from("github"),
            },
            vec![NodeRef::Sink {
                name: Arc::from("deliveries"),
            }],
        );
        let router = Arc::new(Router::new(outs, Arc::new(sinks)));
        // a port that was free a moment ago
        let addr = std::net::TcpListener::bind("127.0.0.1:0")
            .and_then(|l| l.local_addr())
            .unwrap();
        let cfg = GithubWebhookConfig {
            bind_address: addr,
            path: "/webhook".to_string(),
            secret: Some("s3cret".into()),
            token: "unused".into(),
        };
        let shutdown = CancellationToken::new();
        let source = tokio::spawn(run_consumer(
            Arc::from("github"),
            cfg,
            router,
            shutdown.clone(),
        ));
        let deadline = Instant::now() + Duration::from_secs(5);
        while tokio::net::TcpStream::connect(addr).await.is_err() {
            assert!(Instant::now() < deadline, "the webhook never listened");
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        let unsigned = deliver(addr, r#"{"n":1}"#, None).await;
        assert_eq!(unsigned, 401, "unsigned");
        let wrong = deliver(addr, r#"{"n":2}"#, Some(signature("another body"))).await;
        assert_eq!(wrong, 401, "signed over another body");
        let malformed = deliver(addr, r#"{"n":3}"#, Some("md5=0f".to_string())).await;
        assert_eq!(malformed, 401, "not sha256=");
        let signed = deliver(addr, r#"{"n":4}"#, Some(signature(r#"{"n":4}"#))).await;
        assert_eq!(signed, 200, "signed");

        while deliveries.0.lock().await.is_empty() {
            assert!(
                Instant::now() < deadline,
                "the signed delivery never reached the sink"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let got: Vec<String> = deliveries
            .0
            .lock()
            .await
            .iter()
            .map(|w| String::from_utf8_lossy(w).trim_end().to_string())
            .collect();
        assert_eq!(got, [r#"{"n":4}"#], "the signed delivery, and only it");
        shutdown.cancel();
        source.await.unwrap().unwrap();
    }

    #[test]
    fn splits_timestamp_and_message_with_bom() {
        let line = "\u{feff}2025-11-25T20:22:08.9044760Z ##[group]Run npm publish --access public";
        let (timestamp, message) = split_timestamp_and_message(line);

        assert_eq!(timestamp.as_deref(), Some("2025-11-25T20:22:08.9044760Z"));
        assert_eq!(message, "##[group]Run npm publish --access public");
    }

    #[test]
    fn leaves_line_without_timestamp_unchanged() {
        let line = "plain log without ts";
        let (timestamp, message) = split_timestamp_and_message(line);

        assert!(timestamp.is_none());
        assert_eq!(message, "plain log without ts");
    }
}
