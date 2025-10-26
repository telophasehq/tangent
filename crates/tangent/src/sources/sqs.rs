use anyhow::Result;
use async_trait::async_trait;
use aws_config;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_sqs::Client as SQSClient;
use aws_smithy_runtime_api::client::result::SdkError;
use bytes::BytesMut;
use memchr::{memchr, memrchr};
use percent_encoding::percent_decode_str;
use std::{sync::Arc, time::Duration};
use tangent_shared::sources::sqs::SQSConfig;
use tokio_util::sync::CancellationToken;

use crate::{dag::DagRuntime, sources::decoding, worker::Ack};

fn chunk_ndjson(mut data: BytesMut, max_chunk: usize) -> Vec<BytesMut> {
    let max_chunk = max_chunk.max(1);

    if !data.ends_with(b"\n") {
        data.extend_from_slice(b"\n");
    }

    let mut frames: Vec<BytesMut> = Vec::with_capacity((data.len() / max_chunk).saturating_add(1));

    while !data.is_empty() {
        let limit = max_chunk.min(data.len());

        let cut = if let Some(idx) = memrchr(b'\n', &data[..limit]) {
            idx + 1
        } else if let Some(fwd) = memchr(b'\n', &data[limit..]) {
            limit + fwd + 1
        } else {
            data.len()
        };

        frames.push(data.split_to(cut));
    }

    frames
}

pub async fn run_consumer(
    name: String,
    cfg: SQSConfig,
    max_chunk: usize,
    dag_runtime: DagRuntime,
    shutdown: CancellationToken,
) -> Result<()> {
    let aws_cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let sqs_client = SQSClient::new(&aws_cfg);
    let s3_client = S3Client::new(&aws_cfg);
    let qurl = Arc::new(cfg.queue_url);
    let dc = cfg.decoding.clone();

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
                            if let Ok(v) = serde_json::from_str::<serde_json::Value>(body) {
                                if let Some(recs) = v.get("Records").and_then(|r| r.as_array()) {
                                    s3_notification = true;

                                    for r in recs {
                                        let bucket = r.pointer("/s3/bucket/name").and_then(|x| x.as_str());
                                        let key_enc = r.pointer("/s3/object/key").and_then(|x| x.as_str());
                                        if let (Some(bucket), Some(key_enc)) = (bucket, key_enc) {
                                            let mut key = percent_decode_str(key_enc).decode_utf8_lossy().into_owned();
                                            if key.contains('+') {
                                                key = key.replace('+', " ");
                                            }

                                            match s3_client.get_object().bucket(bucket).key(&key).send().await {
                                                Ok(obj) => {
                                                    let aws_sdk_s3::operation::get_object::GetObjectOutput { body: body_stream, .. } = obj;
                                                    match body_stream.collect().await {
                                                        Ok(collected) => {
                                                            let bytes = collected.into_bytes();
                                                            let fmt = dc.resolve_format(&bytes);
                                                            let ndjson: BytesMut = decoding::normalize_to_ndjson(&fmt, BytesMut::from(bytes.as_ref()));
                                                            frames_all.extend(chunk_ndjson(ndjson, max_chunk));
                                                        }
                                                        Err(e) => {
                                                            tracing::error!("S3 collect {bucket}/{key}: {e}");
                                                        }
                                                    }
                                                }
                                                Err(e) => {
                                                    tracing::error!("S3 get {bucket}/{key}: {e}");
                                                }
                                            }
                                        }
                                    }
                                }
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

                                let fmt = dc.resolve_format(&raw);
                                let ndjson: BytesMut = decoding::normalize_to_ndjson(&fmt, raw);
                                frames_all.extend(chunk_ndjson(ndjson, max_chunk));
                            }

                            if !frames_all.is_empty() {
                                if let Err(e) = dag_runtime.push_from_source(&name, frames_all, vec![ack]).await {
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
