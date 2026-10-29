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
use crate::worker::Ack;

const SOURCE: &str = "cloudtrail";

pub async fn run_consumer(
    name: Arc<str>,
    cfg: CloudtrailConfig,
    chunks: usize,
    router: Arc<Router>,
    shutdown: CancellationToken,
) -> Result<()> {
    let Some(queue_url) = opt(&cfg.queue_url) else {
        idle(&name, "cloudtrail queue_url");
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
    let sqs = SQSClient::new(&aws);
    let mut s3conf = aws_sdk_s3::config::Builder::from(&aws);
    if let Some(role) = opt(&cfg.bucket_role) {
        let creds = aws_config::sts::AssumeRoleProvider::builder(role)
            .session_name("tangent")
            .configure(&aws)
            .build()
            .await;
        s3conf = s3conf.credentials_provider(creds);
    }
    let mut s3 = S3Client::from_conf(s3conf.build());
    let qurl = Arc::new(queue_url.to_string());
    let from = NodeRef::Source {
        name: Arc::clone(&name),
    };
    tracing::info!(source = %name, queue = %qurl, "cloudtrail source reading the trail's delivery");

    let mut wait = 0;
    let mut failing = false;
    loop {
        tokio::select! {
            () = shutdown.cancelled() => break,
            res = sqs
                .receive_message()
                .queue_url(qurl.as_str())
                .wait_time_seconds(20)
                .max_number_of_messages(10)
                .visibility_timeout(300)
                .send() => {
                let out = match res {
                    Ok(out) => {
                        wait = 0;
                        out
                    }
                    Err(e) => {
                        let e = DisplayErrorContext(&e);
                        if wait == 0 {
                            tracing::warn!(source = %name, "SQS ReceiveMessage: {e}; retrying, at most a minute apart");
                        } else {
                            tracing::debug!(source = %name, "SQS ReceiveMessage: {e}");
                        }
                        wait = (wait * 2).clamp(1, 60);
                        tokio::select! {
                            () = shutdown.cancelled() => break,
                            () = tokio::time::sleep(StdDuration::from_secs(wait)) => continue,
                        }
                    }
                };
                for msg in out.messages.unwrap_or_default() {
                    let (Some(body), Some(handle)) = (msg.body(), msg.receipt_handle()) else { continue };
                    let ack: Arc<dyn Ack> = Arc::new(SqsAck::new(sqs.clone(), Arc::clone(&qurl), handle.to_string()));
                    match read_files(&mut s3, &trail_files(body)).await {
                        Ok(mut ndjson) => {
                            let frames = decoding::chunk_ndjson(&mut ndjson, chunks);
                            if frames.is_empty() {
                                tracing::debug!(source = %name, "nothing to read; deleting the message");
                                if let Err(e) = ack.ack().await {
                                    tracing::warn!(source = %name, "deleting a message: {e:#}");
                                }
                            } else {
                                failing = false;
                                if let Err(e) = router.forward(&from, frames, vec![ack]).await {
                                    tracing::error!(source = %name, "forward: {e:#}");
                                }
                            }
                        }
                        Err(e) if !failing => {
                            failing = true;
                            tracing::warn!(source = %name, "{e:#}; the message stays on the queue and comes back in 300s");
                        }
                        Err(e) => tracing::debug!(source = %name, "{e:#}; left on the queue"),
                    }
                }
            }
        }
    }
    Ok(())
}

async fn read_files(s3: &mut S3Client, files: &[(String, String)]) -> Result<BytesMut> {
    let mut ndjson = BytesMut::new();
    for (bucket, key) in files {
        let bytes = fetch(s3, bucket, key)
            .await
            .with_context(|| format!("trail file s3://{bucket}/{key} not read"))?;
        match records_of(&bytes) {
            Ok(records) => {
                for (t, mut v) in records {
                    stamp(SOURCE, t, &mut v);
                    ndjson.extend_from_slice(&frame(&v)?);
                }
            }
            Err(e) => {
                tracing::warn!(bucket, key, "not a trail log file, skipped: {e:#}")
            }
        }
    }
    Ok(ndjson)
}

fn trail_files(body: &str) -> Vec<(String, String)> {
    let Ok(v) = serde_json::from_str::<Value>(body) else {
        return Vec::new();
    };
    if let Some(url) = v.get("SubscribeURL").and_then(Value::as_str) {
        tracing::warn!("confirm the queue's SNS subscription: {url}");
    }
    let v = match v.get("Message").and_then(Value::as_str) {
        Some(inner) => serde_json::from_str::<Value>(inner).unwrap_or(v),
        None => v,
    };
    let mut out = Vec::new();
    for r in v.get("Records").and_then(Value::as_array).into_iter().flatten() {
        let created = r["eventName"].as_str().is_none_or(|e| e.starts_with("ObjectCreated"));
        let bucket = r.pointer("/s3/bucket/name").and_then(Value::as_str);
        let key = r.pointer("/s3/object/key").and_then(Value::as_str);
        if let (true, Some(b), Some(k)) = (created, bucket, key) {
            let k = percent_decode_str(&k.replace('+', " ")).decode_utf8_lossy().into_owned();
            out.push((b.to_string(), k));
        }
    }
    if let Some(b) = v.get("s3Bucket").and_then(Value::as_str) {
        for k in v.get("s3ObjectKey").and_then(Value::as_array).into_iter().flatten() {
            if let Some(k) = k.as_str() {
                out.push((b.to_string(), k.to_string()));
            }
        }
    }
    out.retain(|(_, k)| k.contains("/CloudTrail/") || k.contains("/CloudTrail-Insight/"));
    out
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

    const LOG: &str = "AWSLogs/1/CloudTrail/us-east-1/2026/10/28/1_CloudTrail_us-east-1_20261028T1705Z_x.json.gz";

    #[test]
    fn notifications() {
        let digest = "AWSLogs/1/CloudTrail-Digest/us-east-1/2026/10/28/d.json.gz";
        let config = "AWSLogs/1/Config/us-east-1/2026/10/28/ConfigHistory/x.json.gz";
        let sns = |m: &Value| json!({"Type": "Notification", "Message": m.to_string()});
        let s3 = |name: &str, key: &str| json!({"eventName": name, "s3": {"bucket": {"name": "trail"}, "object": {"key": key}}});
        let own = json!({"s3Bucket": "trail", "s3ObjectKey": [LOG, digest]});
        let events = json!({"Records": [
            s3("ObjectCreated:Put", "AWSLogs/1/CloudTrail/us-east-1/2026/10/28/a+b%2Bc.json.gz"),
            s3("ObjectCreated:Put", digest),
            s3("ObjectCreated:Put", config),
            s3("ObjectRemoved:Delete", LOG),
        ]});
        let spaced = "AWSLogs/1/CloudTrail/us-east-1/2026/10/28/a b+c.json.gz";
        for (body, want) in [
            (own.clone(), vec![LOG]),
            (sns(&own), vec![LOG]),
            (events.clone(), vec![spaced]),
            (sns(&events), vec![spaced]),
            (sns(&json!({"messageType": "ConfigurationHistoryDeliveryCompleted", "s3Bucket": "trail", "s3ObjectKey": config})), vec![]),
            (json!({"Service": "Amazon S3", "Event": "s3:TestEvent", "Bucket": "trail"}), vec![]),
            (json!({"Type": "Notification", "Message": "plain text"}), vec![]),
        ] {
            let got = trail_files(&body.to_string());
            assert!(got.iter().all(|(b, _)| b == "trail"), "{body}");
            assert_eq!(got.into_iter().map(|(_, k)| k).collect::<Vec<_>>(), want, "{body}");
        }
        assert!(trail_files("not json").is_empty());
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
