use anyhow::Result;
use async_trait::async_trait;
use aws_config;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_sqs::Client as SQSClient;
use aws_smithy_runtime_api::client::result::SdkError;
use bytes::BytesMut;
use percent_encoding::percent_decode_str;
use std::{sync::Arc, time::Duration};
use tangent_shared::{dag::NodeRef, sources::sqs::SQSConfig};
use tokio_util::sync::CancellationToken;

use crate::{router::Router, sources::decoding, worker::Ack};

pub async fn run_consumer(
    name: Arc<str>,
    cfg: SQSConfig,
    chunks: usize,
    router: Arc<Router>,
    shutdown: CancellationToken,
) -> Result<()> {
    let aws_cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let sqs_client = SQSClient::new(&aws_cfg);
    let s3_client = S3Client::new(&aws_cfg);
    consume(name, cfg, chunks, router, shutdown, sqs_client, s3_client).await
}

/// The source's loop, on the clients it is given: a test hands it clients
/// that talk to a local mock instead of AWS.
async fn consume(
    name: Arc<str>,
    cfg: SQSConfig,
    chunks: usize,
    router: Arc<Router>,
    shutdown: CancellationToken,
    sqs_client: SQSClient,
    s3_client: S3Client,
) -> Result<()> {
    let qurl = Arc::new(cfg.queue_url);
    let dc = cfg.decoding.clone();
    let from = NodeRef::Source {
        name: Arc::clone(&name),
    };

    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,

            res = sqs_client.receive_message()
                .queue_url(qurl.as_str())
                .wait_time_seconds(20)
                .max_number_of_messages(10)
                .send() => {

                match res {
                    Ok(out) => {
                        for msg in out.messages.unwrap_or_default() {
                            let (Some(body), Some(handle)) = (msg.body(), msg.receipt_handle().map(|s| s.to_string())) else {
                                continue;
                            };

                            let ack: Arc<dyn Ack> = Arc::new(SqsAck::new(
                                sqs_client.clone(),
                                qurl.clone(),
                                handle,
                            ));

                            let mut frames_all: Vec<BytesMut> = Vec::new();

                            let mut s3_notification = false;
                            let mut unread = false;
                            if let Ok(v) = serde_json::from_str::<serde_json::Value>(body) {
                                if let Some(recs) = v.get("Records").and_then(|r| r.as_array()) {
                                    s3_notification = true;

                                    for r in recs {
                                        // Only a creation names an object to read (as in
                                        // cloudtrail.rs): a removed one is not there.
                                        let created = r["eventName"].as_str().is_none_or(|e| e.starts_with("ObjectCreated"));
                                        let bucket = r.pointer("/s3/bucket/name").and_then(|x| x.as_str());
                                        let key_enc = r.pointer("/s3/object/key").and_then(|x| x.as_str());
                                        if let (true, Some(bucket), Some(key_enc)) = (created, bucket, key_enc) {
                                            // Form-encoded: '+' is a space, so it goes first.
                                            let key = percent_decode_str(&key_enc.replace('+', " ")).decode_utf8_lossy().into_owned();

                                            match s3_client.get_object().bucket(bucket).key(&key).send().await {
                                                Ok(obj) => {
                                                    let aws_sdk_s3::operation::get_object::GetObjectOutput { body: body_stream, content_encoding, .. } = obj;
                                                    match body_stream.collect().await {
                                                        Ok(collected) => {
                                                            let bytes = collected.into_bytes();
                                                            let raw = BytesMut::from(bytes.as_ref());

                                                            // Objects are often stored compressed (a trail's
                                                            // files, for one): the encoding, the key's suffix
                                                            // or the magic bytes say so.
                                                            let comp = dc.resolve_compression(content_encoding.as_deref(), Some(&key), &raw[..raw.len().min(8)]);
                                                            let raw = match decoding::decompress_bytes(&comp, raw) {
                                                                Ok(v) => v,
                                                                Err(e) => {
                                                                    tracing::warn!(error=?e, "S3 object {bucket}/{key}: decompress failed; treating as plain");
                                                                    BytesMut::from(bytes.as_ref())
                                                                }
                                                            };

                                                            match decoding::normalize_to_ndjson(&cfg.decoding.format, raw) {
                                                                Ok(mut ndjson) => frames_all.extend(decoding::chunk_ndjson(&mut ndjson, chunks)),
                                                                Err(e) => decoding::skip_undecodable(&name, &format!("S3 object {bucket}/{key}"), &e),
                                                            }
                                                        }
                                                        Err(e) => {
                                                            tracing::error!("S3 collect {bucket}/{key}: {e}; the message stays on the queue");
                                                            unread = true;
                                                            break;
                                                        }
                                                    }
                                                }
                                                // Removed or moved before it was read: no later
                                                // read finds it, so keeping the message is no use.
                                                Err(e) if e.as_service_error().is_some_and(|e| e.is_no_such_key()) => {
                                                    tracing::warn!("S3 object {bucket}/{key} is gone; skipped");
                                                }
                                                Err(e) => {
                                                    tracing::error!("S3 get {bucket}/{key}: {e}; the message stays on the queue");
                                                    unread = true;
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            // As the cloudtrail source does: the message comes
                            // back after its visibility timeout, to be read again
                            // whole.
                            if unread {
                                continue;
                            }

                            if !s3_notification {
                                let body_mut = BytesMut::from(body.as_bytes());

                                let sniff = &body_mut[..body_mut.len().min(8)];
                                let comp = dc.resolve_compression(None, None, sniff);

                                let raw = match decoding::decompress_bytes(&comp, body_mut) {
                                    Ok(v) => v,
                                    Err(e) => {
                                        tracing::warn!(error=?e, "decompress failed; treating body as already NDJSON");
                                        BytesMut::from(msg.body().unwrap_or_default().as_bytes())
                                    }
                                };

                                match decoding::normalize_to_ndjson(&cfg.decoding.format, raw) {
                                    Ok(mut ndjson) => frames_all.extend(decoding::chunk_ndjson(&mut ndjson, chunks)),
                                    Err(e) => decoding::skip_undecodable(&name, "the message body", &e),
                                }
                            }

                            if !frames_all.is_empty() {
                                if let Err(e) = router.forward(&from, frames_all, vec![ack]).await {
                                    tracing::error!("push_from_source error: {e:#}");
                                }
                            } else {
                                if let Err(e) = SqsAck::new(sqs_client.clone(), qurl.clone(), msg.receipt_handle().unwrap_or_default().to_string()).ack().await {
                                    tracing::warn!("ack empty message failed: {e}");
                                }
                            }
                        }
                    }
                    Err(e) => {
                        match &e {
                            SdkError::ServiceError(err) => {
                                let msg  = err.err().meta().message().unwrap_or("empty");
                                tracing::warn!(
                                    error_code = ?err.err().meta().code(),
                                    status = ?err.raw().status(),
                                    "SQS ReceiveMessage service error: {msg}"
                                );
                            }
                            SdkError::DispatchFailure(df) => {
                                tracing::warn!(cause = ?df, "SQS ReceiveMessage dispatch failure");
                            }
                            SdkError::TimeoutError(_) => {
                                tracing::warn!("SQS ReceiveMessage timeout");
                            }
                            SdkError::ResponseError (err) => {
                                tracing::warn!(error = ?err, "SQS ReceiveMessage response error");
                            }
                            SdkError::ConstructionFailure(err) => {
                                tracing::warn!(error = ?err, "SQS ReceiveMessage construction failure");
                            }
                            _ => {
                                tracing::warn!(error = ?e, "SQS ReceiveMessage error");
                            }
                        }
                        tokio::time::sleep(Duration::from_millis(200)).await;
                    }
                }
            }
        }
    }

    Ok(())
}

pub struct SqsAck {
    client: SQSClient,
    queue_url: Arc<String>,
    receipt_handle: String,
}

impl SqsAck {
    #[must_use]
    pub const fn new(client: SQSClient, queue_url: Arc<String>, receipt_handle: String) -> Self {
        Self {
            client,
            queue_url,
            receipt_handle,
        }
    }
}

#[async_trait]
impl Ack for SqsAck {
    async fn ack(&self) -> Result<()> {
        self.client
            .delete_message()
            .queue_url(self.queue_url.as_str())
            .receipt_handle(&self.receipt_handle)
            .send()
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sinks::manager::{Sink, SinkManager, SinkWrite};
    use crate::SOURCE_UNDECODABLE_TOTAL;
    use axum::{
        body::Body,
        extract::{Request, State},
        response::Response,
    };
    use serde_json::{json, Value};
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    };
    use tangent_shared::sources::common::{DecodeCompression, DecodeFormat, Decoding};

    /// A queue and a bucket on one local port. ReceiveMessage hands out
    /// `messages` once and nothing after; DeleteMessage is noted; a GET of
    /// `/bucket/<key>` (percent-decoded) answers with the object, 404
    /// NoSuchKey for a key in `gone`, or 403 for any other key.
    #[derive(Default)]
    struct Aws {
        messages: Mutex<Vec<Value>>,
        objects: Vec<(&'static str, &'static str)>,
        gone: Vec<&'static str>,
        receives: AtomicUsize,
        deleted: Mutex<Vec<String>>,
    }

    async fn answer(State(aws): State<Arc<Aws>>, req: Request) -> Response {
        let target = req
            .headers()
            .get("x-amz-target")
            .and_then(|t| t.to_str().ok())
            .unwrap_or_default()
            .to_string();
        let path = percent_decode_str(req.uri().path())
            .decode_utf8_lossy()
            .into_owned();
        let body = axum::body::to_bytes(req.into_body(), usize::MAX)
            .await
            .unwrap();
        let sqs = |v: Value| {
            Response::builder()
                .header("content-type", "application/x-amz-json-1.0")
                .body(Body::from(v.to_string()))
                .unwrap()
        };
        match target.as_str() {
            "AmazonSQS.ReceiveMessage" => {
                aws.receives.fetch_add(1, Ordering::SeqCst);
                let messages = std::mem::take(&mut *aws.messages.lock().unwrap());
                if messages.is_empty() {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                sqs(json!({ "Messages": messages }))
            }
            "AmazonSQS.DeleteMessage" => {
                let req: Value = serde_json::from_slice(&body).unwrap();
                let handle = req["ReceiptHandle"].as_str().unwrap().to_string();
                aws.deleted.lock().unwrap().push(handle);
                sqs(json!({}))
            }
            _ => match aws
                .objects
                .iter()
                .find(|(k, _)| path == format!("/bucket/{k}"))
            {
                Some((_, object)) => Response::new(Body::from(*object)),
                None if aws.gone.iter().any(|k| path == format!("/bucket/{k}")) => {
                    Response::builder()
                        .status(404)
                        .header("content-type", "application/xml")
                        .body(Body::from(
                            "<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>",
                        ))
                        .unwrap()
                }
                None => Response::builder()
                    .status(403)
                    .header("content-type", "application/xml")
                    .body(Body::from(
                        "<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>",
                    ))
                    .unwrap(),
            },
        }
    }

    fn message(handle: &str, body: &str) -> Value {
        json!({"MessageId": handle, "ReceiptHandle": handle, "Body": body})
    }

    /// The S3 event notification of `event` on `keys` in the bucket.
    fn notification(handle: &str, event: &str, keys: &[&str]) -> Value {
        let records: Vec<Value> = keys
            .iter()
            .map(|k| {
                json!({"eventName": event, "s3": {"bucket": {"name": "bucket"}, "object": {"key": k}}})
            })
            .collect();
        message(handle, &json!({ "Records": records }).to_string())
    }

    /// The S3 event notification of `keys` created in the bucket.
    fn created(handle: &str, keys: &[&str]) -> Value {
        notification(handle, "ObjectCreated:Put", keys)
    }

    #[derive(Default)]
    struct Lake(tokio::sync::Mutex<Vec<String>>);

    #[async_trait]
    impl Sink for Lake {
        async fn write(&self, req: SinkWrite) -> Result<()> {
            let write = String::from_utf8_lossy(&req.payload).into_owned();
            self.0.lock().await.push(write);
            Ok(())
        }
    }

    struct Run {
        result: Result<()>,
        written: Vec<String>,
        deleted: Vec<String>,
    }

    /// Runs source `name`, wired to one sink, on `aws` until it asks for
    /// more (each message handed out handled) or ends; then stops it and
    /// lets the sink finish, so every ack is in.
    async fn run(name: &str, aws: Aws) -> Run {
        let aws = Arc::new(aws);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let app = axum::Router::new()
            .fallback(answer)
            .with_state(Arc::clone(&aws));
        tokio::spawn(async move { axum::serve(listener, app).await });

        let creds = aws_sdk_sqs::config::Credentials::new("AKID", "secret", None, None, "test");
        let sqs = SQSClient::from_conf(
            aws_sdk_sqs::Config::builder()
                .behavior_version(aws_sdk_sqs::config::BehaviorVersion::latest())
                .region(aws_sdk_sqs::config::Region::new("us-east-1"))
                .credentials_provider(creds.clone())
                .endpoint_url(&endpoint)
                .build(),
        );
        let s3 = S3Client::from_conf(
            aws_sdk_s3::Config::builder()
                .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
                .region(aws_sdk_s3::config::Region::new("us-east-1"))
                .credentials_provider(creds)
                .endpoint_url(&endpoint)
                .force_path_style(true)
                .retry_config(aws_sdk_s3::config::retry::RetryConfig::disabled())
                .build(),
        );

        let lake = Arc::new(Lake::default());
        let sinks: Vec<(Arc<str>, Arc<dyn Sink>)> =
            vec![(Arc::from("lake"), Arc::clone(&lake) as Arc<dyn Sink>)];
        let manager = Arc::new(SinkManager::for_test(sinks, 1));
        let mut outs = ahash::AHashMap::default();
        outs.insert(
            NodeRef::Source {
                name: Arc::from(name),
            },
            vec![NodeRef::Sink {
                name: Arc::from("lake"),
            }],
        );
        let router = Arc::new(Router::new(outs, Arc::clone(&manager)));
        let cfg = SQSConfig {
            queue_url: format!("{endpoint}/000000000000/queue"),
            wait_time_seconds: 20,
            visibility_timeout: 60,
            decoding: Decoding {
                format: DecodeFormat::Ndjson,
                compression: DecodeCompression::Auto,
            },
        };
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(consume(
            Arc::from(name),
            cfg,
            64,
            Arc::clone(&router),
            shutdown.clone(),
            sqs,
            s3,
        ));
        tokio::time::timeout(Duration::from_secs(10), async {
            while aws.receives.load(Ordering::SeqCst) < 2 && !task.is_finished() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the source neither asked for more nor ended");
        shutdown.cancel();
        let result = task.await.unwrap();
        drop(router);
        let Ok(manager) = Arc::try_unwrap(manager) else {
            panic!("the router held the only other reference")
        };
        manager.join().await.unwrap();
        let written = lake.0.lock().await.clone();
        let deleted = aws.deleted.lock().unwrap().clone();
        Run {
            result,
            written,
            deleted,
        }
    }

    /// A message naming an object GetObject refused stays on the queue and
    /// is read again, whole, after its visibility timeout. Deleting it lost
    /// the object for good; so did deleting it once its other objects were
    /// in the sink.
    #[tokio::test]
    async fn a_message_whose_object_was_not_read_stays_on_the_queue() {
        let run = run(
            "sqs-unread",
            Aws {
                messages: Mutex::new(vec![
                    created("denied", &["secret.json"]),
                    created("half", &["a.json", "secret.json"]),
                    created("read", &["a.json"]),
                ]),
                objects: vec![("a.json", "{\"n\":1}\n")],
                ..Aws::default()
            },
        )
        .await;
        run.result.unwrap();
        assert_eq!(run.deleted, ["read"]);
        assert_eq!(run.written, ["{\"n\":1}\n"], "only the message read whole");
    }

    /// A notification of a removed object is deleted unread: there is no
    /// object to read. GetObject failed on it, so the message stayed on
    /// the queue, back every visibility timeout until its retention ended.
    #[tokio::test]
    async fn a_notification_of_a_removed_object_is_not_read() {
        let run = run(
            "sqs-removed",
            Aws {
                messages: Mutex::new(vec![
                    notification("removed", "ObjectRemoved:Delete", &["secret.json"]),
                    notification("expired", "LifecycleExpiration:Delete", &["secret.json"]),
                ]),
                ..Aws::default()
            },
        )
        .await;
        run.result.unwrap();
        assert_eq!(run.deleted, ["removed", "expired"]);
        assert!(run.written.is_empty());
    }

    /// An object removed (or moved) before it was read is skipped, and the
    /// rest of its message read: no later read can find it. Keeping the
    /// message brought it back every visibility timeout until its
    /// retention ended.
    #[tokio::test]
    async fn an_object_gone_before_it_was_read_is_skipped() {
        let run = run(
            "sqs-gone",
            Aws {
                messages: Mutex::new(vec![
                    created("gone", &["moved.json"]),
                    created("half", &["a.json", "moved.json"]),
                ]),
                objects: vec![("a.json", "{\"n\":1}\n")],
                gone: vec!["moved.json"],
                ..Aws::default()
            },
        )
        .await;
        run.result.unwrap();
        assert_eq!(run.deleted, ["gone", "half"]);
        assert_eq!(run.written, ["{\"n\":1}\n"]);
    }

    /// A notification's key is form-encoded: '+' is a space, "%2B" a '+'.
    /// Decoding the percents first made a key's '+' a space, so the
    /// object was asked for under a key it does not have.
    #[tokio::test]
    async fn a_key_with_a_plus_or_a_space_is_read() {
        let run = run(
            "sqs-plus",
            Aws {
                messages: Mutex::new(vec![
                    created("plus", &["logs/a%2Bb.json"]),
                    created("space", &["logs/c+d.json"]),
                ]),
                objects: vec![
                    ("logs/a+b.json", "{\"n\":1}\n"),
                    ("logs/c d.json", "{\"n\":2}\n"),
                ],
                ..Aws::default()
            },
        )
        .await;
        run.result.unwrap();
        assert_eq!(run.deleted, ["plus", "space"]);
        assert_eq!(run.written.concat(), "{\"n\":1}\n{\"n\":2}\n");
    }

    /// A body, or an object, that does not decode is counted and skipped,
    /// and the source reads on. It ended the source: tangent ran on with
    /// no input, and the message, back after its visibility timeout, ended
    /// the next run's source too.
    #[tokio::test]
    async fn a_message_that_does_not_decode_is_skipped() {
        let run = run(
            "sqs-undecodable",
            Aws {
                messages: Mutex::new(vec![
                    created("object", &["notes.txt"]),
                    message("body", "not ndjson"),
                    message("good", "{\"n\":2}"),
                ]),
                objects: vec![("notes.txt", "not ndjson either")],
                ..Aws::default()
            },
        )
        .await;
        run.result.unwrap();
        assert_eq!(run.written, ["{\"n\":2}\n"]);
        assert_eq!(
            run.deleted,
            ["object", "body", "good"],
            "skipped, not kept to fail again"
        );
        let skipped = SOURCE_UNDECODABLE_TOTAL.with_label_values(&["sqs-undecodable"]);
        assert_eq!(skipped.get(), 2);
    }
}
