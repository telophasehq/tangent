//! CloudTrail, read from a trail's delivered files, through the SQS queue
//! its notifications land in (`queue_url`).

use std::sync::Arc;
use std::time::Duration as StdDuration;

use anyhow::{anyhow, bail, Context, Result};
use aws_sdk_s3::error::DisplayErrorContext;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_sqs::Client as SQSClient;
use bytes::{Bytes, BytesMut};
use chrono::{DateTime, Utc};
use percent_encoding::percent_decode_str;
use serde_json::Value;
use tangent_shared::dag::NodeRef;
use tangent_shared::sources::cloudtrail::CloudtrailConfig;
use tangent_shared::sources::common::{DecodeCompression, DecodeFormat, Decoding};
use tokio_util::sync::CancellationToken;

use crate::router::Router;
use crate::sources::decoding;
use crate::sources::poll::{frame, idle, opt, parse_time, stamp};
use crate::sources::sqs::SqsAck;
use crate::state::StateStore;
use crate::worker::Ack;

const SOURCE: &str = "cloudtrail";

pub async fn run_consumer(
    name: Arc<str>,
    cfg: CloudtrailConfig,
    chunks: usize,
    router: Arc<Router>,
    _store: Arc<dyn StateStore>,
    shutdown: CancellationToken,
) -> Result<()> {
    // There is no LookupEvents polling: a poller's own calls are CloudTrail
    // events it would read back.
    let Some(queue_url) = opt(&cfg.queue_url) else {
        idle(&name, "cloudtrail queue_url (the queue a trail's delivery notifications land in)");
        shutdown.cancelled().await;
        return Ok(());
    };
    let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
    if let Some(region) = opt(&cfg.region) {
        loader = loader.region(aws_config::Region::new(region.to_string()));
    }
    if let Some(profile) = opt(&cfg.profile) {
        loader = loader.profile_name(profile);
    }
    let aws = loader.load().await;
    run_delivery(
        name,
        queue_url.to_string(),
        opt(&cfg.bucket_role),
        &aws,
        chunks,
        router,
        shutdown,
    )
    .await
}

/// How long a received message stays hidden from the queue's other readers.
/// It is deleted once its records are all delivered; one whose file could
/// not be fetched comes back after this and is tried again.
const VISIBILITY_SECS: i32 = 300;

/// A trail's delivery: the queue is told of each file the trail writes to
/// its bucket; each file, gzipped `{"Records": [...]}`, is read and every
/// record forwarded, and the message is deleted once they are all delivered.
/// A message whose file cannot be fetched (a permission, the network, S3)
/// stays on the queue and comes back, so a failure loses nothing; the
/// queue's redrive policy, if it has one, sets aside what never succeeds.
async fn run_delivery(
    name: Arc<str>,
    queue_url: String,
    bucket_role: Option<&str>,
    aws: &aws_config::SdkConfig,
    chunks: usize,
    router: Arc<Router>,
    shutdown: CancellationToken,
) -> Result<()> {
    let sqs = SQSClient::new(aws);
    let mut s3conf = aws_sdk_s3::config::Builder::from(aws);
    if let Some(role) = bucket_role {
        let creds = aws_config::sts::AssumeRoleProvider::builder(role)
            .session_name("tangent")
            .configure(aws)
            .build()
            .await;
        s3conf = s3conf.credentials_provider(creds);
    }
    let mut s3 = S3Client::from_conf(s3conf.build());
    let qurl = Arc::new(queue_url);
    let from = NodeRef::Source {
        name: Arc::clone(&name),
    };
    tracing::info!(
        source = %name,
        queue = %qurl,
        bucket_role = bucket_role.unwrap_or("none"),
        "cloudtrail source reading the trail's delivery"
    );

    let mut failing = false;
    let mut receiving = Backoff::default();
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            res = sqs
                .receive_message()
                .queue_url(qurl.as_str())
                .wait_time_seconds(20)
                .max_number_of_messages(10)
                .visibility_timeout(VISIBILITY_SECS)
                .send() => {
                let out = match res {
                    Ok(out) => {
                        receiving.succeeded();
                        out
                    }
                    Err(e) => {
                        let e = aws_sdk_sqs::error::DisplayErrorContext(&e);
                        let (wait, warn) = receiving.failed();
                        if warn {
                            tracing::warn!(source = %name, "SQS ReceiveMessage: {e}; retrying, at most a minute apart");
                        } else {
                            tracing::debug!(source = %name, "SQS ReceiveMessage: {e}; retrying in {}s", wait.as_secs());
                        }
                        tokio::select! {
                            () = shutdown.cancelled() => break,
                            () = tokio::time::sleep(wait) => continue,
                        }
                    }
                };
                for msg in out.messages.unwrap_or_default() {
                    let (Some(body), Some(handle)) = (msg.body(), msg.receipt_handle()) else { continue };
                    let ack: Arc<dyn Ack> = Arc::new(SqsAck::new(sqs.clone(), Arc::clone(&qurl), handle.to_string()));
                    let files = match notice_of(body) {
                        Notice::Files(files) => files,
                        Notice::Skip(why) => {
                            tracing::debug!(source = %name, "nothing to read ({why}); deleting the message");
                            delete(&name, &ack).await;
                            continue;
                        }
                        Notice::Confirm(url) => {
                            tracing::warn!(source = %name, "the queue's SNS subscription is not confirmed yet; open this link once to confirm it: {url}");
                            delete(&name, &ack).await;
                            continue;
                        }
                        Notice::Unknown => {
                            let head: String = body.chars().take(200).collect();
                            tracing::warn!(source = %name, message = %head, "a message that names no trail file; deleting it");
                            delete(&name, &ack).await;
                            continue;
                        }
                    };
                    match read_files(&mut s3, &files).await {
                        Ok(mut ndjson) => {
                            failing = false;
                            let frames = decoding::chunk_ndjson(&mut ndjson, chunks);
                            if frames.is_empty() {
                                delete(&name, &ack).await;
                            } else if let Err(e) = router.forward(&from, frames, vec![ack]).await {
                                tracing::error!(source = %name, "forward: {e:#}");
                            }
                        }
                        Err(e) if !failing => {
                            failing = true;
                            tracing::warn!(source = %name, "{e:#}; the message stays on the queue and comes back in {VISIBILITY_SECS}s");
                        }
                        Err(e) => tracing::debug!(source = %name, "{e:#}; left on the queue"),
                    }
                }
            }
        }
    }
    Ok(())
}

/// The wait before reading the queue again after a failure (an expired
/// session, a permission, the network): a second, doubling up to a minute,
/// and a second again once a read succeeds. Only the first failure of a run
/// is a warning, so a session left expired overnight is one line, not
/// thousands.
#[derive(Debug, Default)]
struct Backoff {
    failures: u32,
}

impl Backoff {
    /// A failed read: how long to wait, and whether to warn.
    fn failed(&mut self) -> (StdDuration, bool) {
        self.failures = self.failures.saturating_add(1);
        let secs = 1u64 << (self.failures - 1).min(6);
        (StdDuration::from_secs(secs.min(60)), self.failures == 1)
    }

    fn succeeded(&mut self) {
        self.failures = 0;
    }
}

async fn delete(name: &str, ack: &Arc<dyn Ack>) {
    if let Err(e) = ack.ack().await {
        tracing::warn!(source = %name, "deleting a message: {e:#}");
    }
}

/// Every record in the files, stamped, as NDJSON. An error is a file that
/// could not be fetched, and then nothing of the message is forwarded. A
/// file fetched that is not a trail log is skipped: asking again would not
/// change it.
async fn read_files(s3: &mut S3Client, files: &[TrailFile]) -> Result<BytesMut> {
    let mut ndjson = BytesMut::new();
    for f in files {
        let bytes = fetch(s3, &f.bucket, &f.key)
            .await
            .with_context(|| format!("trail file s3://{}/{} not read", f.bucket, f.key))?;
        match records_of(&bytes) {
            Ok(records) => {
                for (t, mut v) in records {
                    stamp(SOURCE, t, &mut v);
                    ndjson.extend_from_slice(&frame(&v)?);
                }
            }
            Err(e) => {
                tracing::warn!(bucket = %f.bucket, key = %f.key, "not a trail log file, skipped: {e:#}")
            }
        }
    }
    Ok(ndjson)
}

/// What a queue message asks for.
#[derive(Debug, PartialEq, Eq)]
enum Notice {
    /// Trail log files to read.
    Files(Vec<TrailFile>),
    /// Nothing to read, and why.
    Skip(&'static str),
    /// SNS asking that the queue's subscription be confirmed: the link to
    /// confirm it, which a person (or the topic's owner) opens once.
    Confirm(String),
    /// Not a notification this source knows.
    Unknown,
}

/// One delivered file.
#[derive(Debug, PartialEq, Eq)]
struct TrailFile {
    bucket: String,
    key: String,
}

/// The trail files a message names: S3 event notifications (`Records[].s3`,
/// keys URL-encoded), CloudTrail's SNS notifications (`s3Bucket`,
/// `s3ObjectKey[]`) and EventBridge's `Object Created`, each possibly inside
/// an SNS envelope (`Message`, unless the subscription delivers raw). Only
/// log files are read: a digest, a deleted object or anything else in the
/// bucket (Control Tower's also holds AWS Config's files) is not an event,
/// and neither is AWS Config's notification on a topic it shares with the
/// trail.
fn notice_of(body: &str) -> Notice {
    let Ok(mut v) = serde_json::from_str::<Value>(body) else {
        return Notice::Unknown;
    };
    if v.get("Type").and_then(Value::as_str) == Some("SubscriptionConfirmation") {
        return match v.get("SubscribeURL").and_then(Value::as_str) {
            Some(url) => Notice::Confirm(url.to_string()),
            None => Notice::Unknown,
        };
    }
    if v.get("Type").and_then(Value::as_str) == Some("Notification") {
        match v
            .get("Message")
            .and_then(Value::as_str)
            .map(serde_json::from_str::<Value>)
        {
            Some(Ok(inner)) => v = inner,
            _ => return Notice::Unknown,
        }
    }
    if v.get("Event").and_then(Value::as_str) == Some("s3:TestEvent") {
        return Notice::Skip("S3's test event");
    }
    if v.get("messageType").is_some() {
        return Notice::Skip("an AWS Config notification");
    }
    let text = |v: &Value, at: &str| v.pointer(at).and_then(Value::as_str).map(str::to_string);
    let mut known = false;
    let mut named = Vec::new();
    for r in v
        .get("Records")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        if r.get("s3").is_none() {
            continue;
        }
        known = true;
        let created = text(r, "/eventName").is_none_or(|e| e.starts_with("ObjectCreated"));
        if !created {
            continue;
        }
        if let (Some(bucket), Some(key)) = (text(r, "/s3/bucket/name"), text(r, "/s3/object/key")) {
            named.push(TrailFile {
                bucket,
                key: form_decoded(&key),
            });
        }
    }
    if let Some(bucket) = text(&v, "/s3Bucket") {
        known = true;
        let keys: Vec<&str> = match v.get("s3ObjectKey") {
            Some(Value::Array(a)) => a.iter().filter_map(Value::as_str).collect(),
            Some(Value::String(k)) => vec![k.as_str()],
            _ => Vec::new(),
        };
        for key in keys {
            named.push(TrailFile {
                bucket: bucket.clone(),
                key: key.to_string(),
            });
        }
    }
    if text(&v, "/source").as_deref() == Some("aws.s3") {
        known = true;
        if text(&v, "/detail-type").as_deref() == Some("Object Created") {
            if let (Some(bucket), Some(key)) = (
                text(&v, "/detail/bucket/name"),
                text(&v, "/detail/object/key"),
            ) {
                named.push(TrailFile { bucket, key });
            }
        }
    }
    if !known {
        return Notice::Unknown;
    }
    named.retain(|f| is_log(&f.key));
    if named.is_empty() {
        Notice::Skip("no trail log file: a digest, a deleted object, or another file in the bucket")
    } else {
        Notice::Files(named)
    }
}

/// A trail writes its logs under `CloudTrail/` and its Insights under
/// `CloudTrail-Insight/`; its digests, under `CloudTrail-Digest/`, are not
/// events.
fn is_log(key: &str) -> bool {
    key.contains("/CloudTrail/") || key.contains("/CloudTrail-Insight/")
}

/// An S3 event notification's key is form-encoded: `+` is a space, `%2B` a
/// plus.
fn form_decoded(key: &str) -> String {
    percent_decode_str(&key.replace('+', " "))
        .decode_utf8_lossy()
        .into_owned()
}

async fn fetch(s3: &mut S3Client, bucket: &str, key: &str) -> Result<Bytes> {
    let mut res = s3.get_object().bucket(bucket).key(key).send().await;
    let moved = res
        .as_ref()
        .err()
        .and_then(|e| e.raw_response()?.headers().get("x-amz-bucket-region"))
        .map(str::to_string);
    if let Some(region) = moved {
        let conf = s3.config().to_builder().region(aws_sdk_s3::config::Region::new(region));
        *s3 = S3Client::from_conf(conf.build());
        res = s3.get_object().bucket(bucket).key(key).send().await;
    }
    let obj = res.map_err(|e| anyhow!("GetObject: {}", DisplayErrorContext(&e)))?;
    Ok(obj.body.collect().await.context("reading the object")?.into_bytes())
}

/// A delivered file's records with their times: decompressed (CloudTrail
/// gzips them; the bytes say, not the name) and its `Records` taken out. An
/// error means the file is not a trail log.
fn records_of(raw: &[u8]) -> Result<Vec<(DateTime<Utc>, Value)>> {
    let decoding = Decoding {
        format: DecodeFormat::Json,
        compression: DecodeCompression::Auto,
    };
    let comp = decoding.resolve_compression(None, None, &raw[..raw.len().min(8)]);
    let raw = decoding::decompress_bytes(&comp, BytesMut::from(raw)).context("decompressing")?;
    let mut v: Value = serde_json::from_slice(&raw).context("parsing")?;
    let Some(Value::Array(records)) = v.get_mut("Records").map(Value::take) else {
        bail!("no Records");
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
    use serde_json::json;
    use std::io::Write;

    const LOG: &str = "o-exampleorg/AWSLogs/o-exampleorg/111122223333/CloudTrail/us-east-1/2026/10/28/111122223333_CloudTrail_us-east-1_20261028T1705Z_abc.json.gz";

    fn files(n: Notice) -> Vec<TrailFile> {
        match n {
            Notice::Files(f) => f,
            other => panic!("expected files, got {other:?}"),
        }
    }

    fn file(bucket: &str, key: &str) -> TrailFile {
        TrailFile {
            bucket: bucket.into(),
            key: key.into(),
        }
    }

    /// Control Tower: the trail notifies a topic in the audit account, and
    /// the queue subscribes to it without raw delivery.
    #[test]
    fn cloudtrails_own_notification_through_sns() {
        let inner = json!({"s3Bucket": "aws-controltower-logs-444455556666-us-east-1", "s3ObjectKey": [LOG]});
        let body = json!({
            "Type": "Notification",
            "TopicArn": "arn:aws:sns:us-east-1:777788889999:aws-controltower-AllConfigNotifications",
            "Message": inner.to_string(),
        });
        assert_eq!(
            files(notice_of(&body.to_string())),
            [file("aws-controltower-logs-444455556666-us-east-1", LOG)]
        );
        // Raw delivery: the notification itself.
        assert_eq!(files(notice_of(&inner.to_string())).len(), 1);
    }

    /// AWS Config shares Control Tower's topic: its notifications, and the
    /// files it delivers, are not the trail's.
    #[test]
    fn aws_config_on_the_same_topic() {
        for inner in [
            json!({"messageType": "ConfigurationItemChangeNotification", "configurationItem": {"resourceType": "AWS::S3::Bucket"}}),
            json!({"messageType": "ConfigurationHistoryDeliveryCompleted", "s3Bucket": "b", "s3ObjectKey": "AWSLogs/111122223333/Config/us-east-1/2026/10/28/ConfigHistory/x.json.gz"}),
        ] {
            let body = json!({"Type": "Notification", "Message": inner.to_string()}).to_string();
            assert!(matches!(notice_of(&body), Notice::Skip(_)), "{body}");
        }
    }

    #[test]
    fn s3_event_notifications() {
        let event = |name: &str, key: &str| json!({"eventName": name, "awsRegion": "us-east-1", "s3": {"bucket": {"name": "trail"}, "object": {"key": key}}});
        let body = json!({"Records": [
            event("ObjectCreated:Put", "AWSLogs/111122223333/CloudTrail/us-east-1/2026/10/28/a+b%2Bc.json.gz"),
            event("ObjectCreated:Put", "AWSLogs/111122223333/CloudTrail-Digest/us-east-1/2026/10/28/d.json.gz"),
            event("ObjectCreated:Put", "AWSLogs/111122223333/Config/us-east-1/2026/10/28/ConfigSnapshot/s.json.gz"),
            event("ObjectRemoved:Delete", "AWSLogs/111122223333/CloudTrail/us-east-1/2026/10/28/gone.json.gz"),
        ]})
        .to_string();
        let want = [file(
            "trail",
            "AWSLogs/111122223333/CloudTrail/us-east-1/2026/10/28/a b+c.json.gz",
        )];
        assert_eq!(files(notice_of(&body)), want);
        let through_sns = json!({"Type": "Notification", "Message": body}).to_string();
        assert_eq!(files(notice_of(&through_sns)), want);
    }

    #[test]
    fn eventbridge_object_created() {
        let body = json!({
            "version": "0", "source": "aws.s3", "detail-type": "Object Created", "region": "eu-west-1",
            "detail": {"bucket": {"name": "trail"}, "object": {"key": LOG, "size": 1234}},
        });
        assert_eq!(
            files(notice_of(&body.to_string())),
            [file("trail", LOG)]
        );
        let deleted = json!({"source": "aws.s3", "detail-type": "Object Deleted", "detail": {"bucket": {"name": "trail"}, "object": {"key": LOG}}});
        assert!(matches!(notice_of(&deleted.to_string()), Notice::Skip(_)));
    }

    #[test]
    fn a_subscription_confirmation_gives_its_link() {
        let body = json!({"Type": "SubscriptionConfirmation", "TopicArn": "arn:aws:sns:us-east-1:777788889999:trail",
                          "SubscribeURL": "https://sns.us-east-1.amazonaws.com/?Action=ConfirmSubscription&Token=abc"});
        assert_eq!(
            notice_of(&body.to_string()),
            Notice::Confirm(
                "https://sns.us-east-1.amazonaws.com/?Action=ConfirmSubscription&Token=abc".into()
            )
        );
    }

    #[test]
    fn nothing_to_read() {
        assert!(matches!(
            notice_of(r#"{"Service":"Amazon S3","Event":"s3:TestEvent","Bucket":"trail"}"#),
            Notice::Skip(_)
        ));
        let digest = json!({"s3Bucket": "trail", "s3ObjectKey": ["AWSLogs/111122223333/CloudTrail-Digest/us-east-1/2026/10/28/d.json.gz"]});
        assert!(matches!(notice_of(&digest.to_string()), Notice::Skip(_)));
        assert_eq!(notice_of("not json"), Notice::Unknown);
        assert_eq!(notice_of(r#"{"hello":"world"}"#), Notice::Unknown);
        assert_eq!(
            notice_of(r#"{"Type":"Notification","Message":"plain text"}"#),
            Notice::Unknown
        );
    }

    #[test]
    fn a_files_records() {
        let doc = json!({"Records": [
            {"eventTime": "2026-10-28T17:05:01Z", "eventName": "AssumeRole"},
            {"eventTime": "2026-10-28T17:05:02Z", "eventName": "GetObject"},
        ]})
        .to_string();
        let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        gz.write_all(doc.as_bytes()).unwrap();
        let gz = gz.finish().unwrap();
        let got = records_of(&gz).unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(
            got[0].0,
            DateTime::parse_from_rfc3339("2026-10-28T17:05:01Z").unwrap()
        );
        assert_eq!(got[1].1["eventName"], "GetObject");
        assert_eq!(records_of(doc.as_bytes()).unwrap().len(), 2);
        assert!(records_of(br#"{"digestStartTime":"2026-10-28T17:00:00Z"}"#).is_err());
        assert!(records_of(b"\x1f\x8b\x08 not gzip").is_err());
    }

    #[test]
    fn queue_failures_back_off_and_warn_once() {
        let mut b = Backoff::default();
        let waits: Vec<(u64, bool)> = (0..9)
            .map(|_| b.failed())
            .map(|(d, warn)| (d.as_secs(), warn))
            .collect();
        assert_eq!(
            waits,
            [
                (1, true),
                (2, false),
                (4, false),
                (8, false),
                (16, false),
                (32, false),
                (60, false),
                (60, false),
                (60, false),
            ]
        );
        // A read that succeeds starts the next run of failures over.
        b.succeeded();
        assert_eq!(b.failed(), (StdDuration::from_secs(1), true));
        assert_eq!(b.failed(), (StdDuration::from_secs(2), false));
        // A failure that never ends stays a minute apart.
        b.failures = u32::MAX - 1;
        assert_eq!(b.failed(), (StdDuration::from_secs(60), false));
        assert_eq!(b.failed(), (StdDuration::from_secs(60), false));
    }

    /// A bucket in another region than the client's: S3 redirects, naming
    /// the bucket's region; the source asks there, and goes straight there
    /// for the bucket's next file.
    #[tokio::test]
    async fn follows_s3_to_the_buckets_region() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let signed_for = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let doc =
            br#"{"Records":[{"eventTime":"2026-10-28T17:05:01Z","eventName":"ConsoleLogin"}]}"#;
        let seen = Arc::clone(&signed_for);
        tokio::spawn(async move {
            loop {
                let (mut sock, _) = listener.accept().await.unwrap();
                let mut buf = vec![0u8; 16 * 1024];
                let n = sock.read(&mut buf).await.unwrap();
                let req = String::from_utf8_lossy(&buf[..n]).to_string();
                // Credential=<key>/<date>/<region>/s3/aws4_request
                let region = req
                    .split("Credential=")
                    .nth(1)
                    .and_then(|c| c.split('/').nth(2))
                    .unwrap_or_default()
                    .to_string();
                seen.lock().unwrap().push(region.clone());
                let mut resp = if region == "us-east-1" {
                    format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", doc.len()).into_bytes()
                } else {
                    b"HTTP/1.1 301 Moved Permanently\r\nx-amz-bucket-region: us-east-1\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".to_vec()
                };
                if region == "us-east-1" {
                    resp.extend_from_slice(doc);
                }
                sock.write_all(&resp).await.unwrap();
            }
        });

        let conf = aws_sdk_s3::Config::builder()
            .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
            .region(aws_sdk_s3::config::Region::new("us-west-2"))
            .credentials_provider(aws_sdk_s3::config::Credentials::new(
                "AKID", "secret", None, None, "test",
            ))
            .endpoint_url(format!("http://{addr}"))
            .force_path_style(true)
            .retry_config(aws_sdk_s3::config::retry::RetryConfig::disabled())
            .build();
        let mut s3 = S3Client::from_conf(conf);
        let bytes = fetch(&mut s3, "trail", LOG).await.unwrap();
        assert_eq!(records_of(&bytes).unwrap().len(), 1);
        fetch(&mut s3, "trail", LOG).await.unwrap();
        assert_eq!(
            *signed_for.lock().unwrap(),
            ["us-west-2", "us-east-1", "us-east-1"]
        );
    }
}
