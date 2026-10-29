//! CloudTrail, read directly: `LookupEvents` by default (nothing to set up,
//! management events, about fifteen minutes behind), or a trail's delivered
//! files through the SQS queue its notifications land in, when `queue_url`
//! is set.

use std::sync::Arc;
use std::time::Duration as StdDuration;

use anyhow::{Context, Result};
use aws_sdk_s3::Client as S3Client;
use aws_sdk_sqs::Client as SQSClient;
use bytes::BytesMut;
use chrono::{DateTime, Duration, Utc};
use percent_encoding::percent_decode_str;
use serde_json::Value;
use tangent_shared::dag::NodeRef;
use tangent_shared::sources::cloudtrail::CloudtrailConfig;
use tangent_shared::sources::common::{DecodeCompression, DecodeFormat, Decoding};
use tokio_util::sync::CancellationToken;

use crate::router::Router;
use crate::sources::decoding;
use crate::sources::poll::{frame, opt, parse_time, stamp, Poller};
use crate::sources::sqs::SqsAck;
use crate::state::StateStore;
use crate::worker::Ack;

const SOURCE: &str = "cloudtrail";
/// `LookupEvents` shows an event up to about fifteen minutes after it happens.
const LATE_MS: i64 = 20 * 60 * 1000;

pub async fn run_consumer(
    name: Arc<str>,
    cfg: CloudtrailConfig,
    chunks: usize,
    router: Arc<Router>,
    store: Arc<dyn StateStore>,
    shutdown: CancellationToken,
) -> Result<()> {
    // app/tangent in the user agent marks our own calls in CloudTrail
    let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .app_name(aws_config::AppName::new("tangent").expect("a valid app name"));
    if let Some(region) = opt(&cfg.region) {
        loader = loader.region(aws_config::Region::new(region.to_string()));
    }
    if let Some(profile) = opt(&cfg.profile) {
        loader = loader.profile_name(profile);
    }
    let aws = loader.load().await;

    if let Some(queue_url) = opt(&cfg.queue_url) {
        return run_delivery(name, queue_url.to_string(), &aws, chunks, router, shutdown).await;
    }

    // `LookupEvents` is regional. One poller per region named, each with its
    // own cursor; none named, the SDK's region (from `region` or the default
    // chain) and the source's own cursor.
    let regions: Vec<String> = cfg
        .regions
        .iter()
        .map(|r| r.trim().to_string())
        .filter(|r| !r.is_empty())
        .collect();
    tracing::info!(
        source = %name,
        since = %cfg.since,
        regions = ?regions,
        "cloudtrail source polling LookupEvents"
    );
    if regions.is_empty() {
        let poller = Poller::new(
            Arc::clone(&name),
            store,
            router,
            Duration::milliseconds(LATE_MS),
        );
        let client = aws_sdk_cloudtrail::Client::new(&aws);
        run_region(&name, &cfg, client, poller, None, shutdown).await;
        return Ok(());
    }
    let runs = regions.iter().map(|region| {
        let conf = aws_sdk_cloudtrail::config::Builder::from(&aws)
            .region(aws_config::Region::new(region.clone()))
            .build();
        let poller = Poller::new(
            Arc::clone(&name),
            Arc::clone(&store),
            Arc::clone(&router),
            Duration::milliseconds(LATE_MS),
        )
        .scoped(region);
        run_region(
            &name,
            &cfg,
            aws_sdk_cloudtrail::Client::from_conf(conf),
            poller,
            Some(region.as_str()),
            shutdown.clone(),
        )
    });
    futures::future::join_all(runs).await;
    Ok(())
}

/// The poll loop for one region (or the SDK's), until shutdown.
async fn run_region(
    name: &Arc<str>,
    cfg: &CloudtrailConfig,
    client: aws_sdk_cloudtrail::Client,
    poller: Poller,
    region: Option<&str>,
    shutdown: CancellationToken,
) {
    let region = region.unwrap_or("default");
    let mut ticker = tokio::time::interval(StdDuration::from_secs(cfg.interval_secs.max(10)));
    let mut failing = false;
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            _ = ticker.tick() => match poll(&client, cfg, &poller).await {
                Ok(n) => {
                    failing = false;
                    if n > 0 {
                        tracing::debug!(source = %name, region, events = n, "cloudtrail poll");
                    }
                }
                Err(e) if !failing => {
                    failing = true;
                    tracing::warn!(source = %name, region, "cloudtrail poll failed (retrying each interval): {e:#}");
                }
                Err(e) => tracing::debug!(source = %name, region, "cloudtrail poll failed: {e:#}"),
            },
        }
    }
}

/// One pass over `LookupEvents` from the cursor (less the lateness window)
/// to now: new events forwarded in time order, the cursor moved up.
async fn poll(
    client: &aws_sdk_cloudtrail::Client,
    cfg: &CloudtrailConfig,
    poller: &Poller,
) -> Result<usize> {
    let start = poller.start(&cfg.since).await;
    let end = Utc::now();
    let mut newest: Option<DateTime<Utc>> = None;
    let mut out: Vec<(DateTime<Utc>, Value)> = Vec::new();
    let mut token: Option<String> = None;
    loop {
        let mut req = client
            .lookup_events()
            .start_time(aws_sdk_cloudtrail::primitives::DateTime::from_secs(
                start.timestamp(),
            ))
            .end_time(aws_sdk_cloudtrail::primitives::DateTime::from_secs(
                end.timestamp(),
            ))
            .max_results(50);
        if let Some(t) = &token {
            req = req.next_token(t);
        }
        let resp = req.send().await.context("LookupEvents")?;
        for ev in resp.events() {
            let Some(raw) = ev.cloud_trail_event() else {
                continue;
            };
            let Ok(v) = serde_json::from_str::<Value>(raw) else {
                continue;
            };
            let id = ev.event_id().unwrap_or("");
            if id.is_empty() || !poller.first_sight(id).await {
                continue;
            }
            let t = parse_time(&v["eventTime"]).unwrap_or(end);
            newest = Some(newest.map_or(t, |n| n.max(t)));
            out.push((t, v));
        }
        token = resp.next_token().map(str::to_string);
        if token.is_none() || out.len() >= 5_000 {
            break;
        }
    }
    let n = poller.emit(SOURCE, out).await?;
    poller.finish(newest, end).await;
    Ok(n)
}

/// A trail's delivery: the queue carries S3 event notifications for the
/// trail's bucket, or the trail's own SNS notifications relayed to SQS.
/// Each names gzipped files of `{"Records": [...]}`; every record is
/// forwarded, and the message is deleted once they are all delivered.
async fn run_delivery(
    name: Arc<str>,
    queue_url: String,
    aws: &aws_config::SdkConfig,
    chunks: usize,
    router: Arc<Router>,
    shutdown: CancellationToken,
) -> Result<()> {
    let sqs = SQSClient::new(aws);
    let s3 = S3Client::new(aws);
    let qurl = Arc::new(queue_url);
    let from = NodeRef::Source {
        name: Arc::clone(&name),
    };
    let decoding = Decoding {
        format: DecodeFormat::Json,
        compression: DecodeCompression::Auto,
    };
    tracing::info!(source = %name, queue = %qurl, "cloudtrail source reading the trail's delivery");

    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            res = sqs.receive_message().queue_url(qurl.as_str()).wait_time_seconds(20).max_number_of_messages(10).send() => {
                let out = match res {
                    Ok(out) => out,
                    Err(e) => {
                        tracing::warn!(source = %name, "SQS ReceiveMessage: {}", aws_sdk_sqs::error::DisplayErrorContext(&e));
                        tokio::time::sleep(StdDuration::from_millis(500)).await;
                        continue;
                    }
                };
                for msg in out.messages.unwrap_or_default() {
                    let (Some(body), Some(handle)) = (msg.body(), msg.receipt_handle()) else { continue };
                    let ack: Arc<dyn Ack> = Arc::new(SqsAck::new(sqs.clone(), Arc::clone(&qurl), handle.to_string()));
                    let mut ndjson = BytesMut::new();
                    for (bucket, key) in objects_named(body) {
                        match fetch(&s3, &decoding, &bucket, &key).await {
                            Ok(records) => for (t, mut v) in records {
                                stamp(SOURCE, t, &mut v);
                                ndjson.extend_from_slice(&frame(&v)?);
                            },
                            Err(e) => tracing::warn!(source = %name, bucket, key, "trail file: {e:#}"),
                        }
                    }
                    let frames = decoding::chunk_ndjson(&mut ndjson, chunks);
                    if frames.is_empty() {
                        if let Err(e) = ack.ack().await {
                            tracing::warn!(source = %name, "deleting an empty message: {e}");
                        }
                    } else if let Err(e) = router.forward(&from, frames, vec![ack]).await {
                        tracing::error!(source = %name, "forward: {e:#}");
                    }
                }
            }
        }
    }
    Ok(())
}

/// The `(bucket, key)` pairs a notification names. S3 event notifications
/// (`Records[].s3`), CloudTrail's SNS notifications (`s3Bucket`,
/// `s3ObjectKey[]`), either possibly inside an SNS envelope (`Message`).
/// Digest files are not events and are skipped.
fn objects_named(body: &str) -> Vec<(String, String)> {
    let Ok(v) = serde_json::from_str::<Value>(body) else {
        return Vec::new();
    };
    let v = match v.get("Message").and_then(Value::as_str) {
        Some(inner) => serde_json::from_str::<Value>(inner).unwrap_or(v),
        None => v,
    };
    let mut out = Vec::new();
    if let Some(recs) = v.get("Records").and_then(Value::as_array) {
        for r in recs {
            let bucket = r.pointer("/s3/bucket/name").and_then(Value::as_str);
            let key = r.pointer("/s3/object/key").and_then(Value::as_str);
            if let (Some(b), Some(k)) = (bucket, key) {
                let k = percent_decode_str(k).decode_utf8_lossy().replace('+', " ");
                out.push((b.to_string(), k));
            }
        }
    }
    if let Some(b) = v.get("s3Bucket").and_then(Value::as_str) {
        for k in v
            .get("s3ObjectKey")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            if let Some(k) = k.as_str() {
                out.push((b.to_string(), k.to_string()));
            }
        }
    }
    out.retain(|(_, k)| !k.contains("/CloudTrail-Digest/"));
    out
}

/// One delivered file: fetched, decompressed, its `Records` with their times.
async fn fetch(
    s3: &S3Client,
    decoding: &Decoding,
    bucket: &str,
    key: &str,
) -> Result<Vec<(DateTime<Utc>, Value)>> {
    let obj = s3
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .context("GetObject")?;
    let encoding = obj.content_encoding().map(str::to_string);
    let bytes = obj
        .body
        .collect()
        .await
        .context("reading the object")?
        .into_bytes();
    let raw = BytesMut::from(bytes.as_ref());
    let comp =
        decoding.resolve_compression(encoding.as_deref(), Some(key), &raw[..raw.len().min(8)]);
    let raw = decoding::decompress_bytes(&comp, raw).context("decompressing")?;
    let v: Value = serde_json::from_slice(&raw).context("parsing")?;
    let records = match v {
        Value::Object(mut m) => match m.remove("Records") {
            Some(Value::Array(a)) => a,
            _ => vec![Value::Object(m)],
        },
        Value::Array(a) => a,
        other => vec![other],
    };
    let now = Utc::now();
    Ok(records
        .into_iter()
        .map(|r| (parse_time(&r["eventTime"]).unwrap_or(now), r))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn notifications_in_three_shapes() {
        let s3_event = r#"{"Records":[{"s3":{"bucket":{"name":"trail-bucket"},"object":{"key":"AWSLogs/1/CloudTrail/us-west-2/2026/10/27/1_CloudTrail_us-west-2_20261027T1300Z_x.json.gz"}}},
            {"s3":{"bucket":{"name":"trail-bucket"},"object":{"key":"AWSLogs/1/CloudTrail-Digest/us-west-2/2026/10/27/d.json.gz"}}}]}"#;
        assert_eq!(objects_named(s3_event).len(), 1);
        let sns_relayed = r#"{"Type":"Notification","Message":"{\"s3Bucket\":\"trail-bucket\",\"s3ObjectKey\":[\"AWSLogs/1/CloudTrail/a.json.gz\",\"AWSLogs/1/CloudTrail/b.json.gz\"]}"}"#;
        let got = objects_named(sns_relayed);
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].0, "trail-bucket");
        assert!(objects_named(r#"{"Service":"Amazon S3","Event":"s3:TestEvent"}"#).is_empty());
        assert!(objects_named("not json").is_empty());
    }
}
