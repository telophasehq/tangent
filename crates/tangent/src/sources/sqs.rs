use anyhow::Result;
use async_trait::async_trait;
use aws_config;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_sqs::Client as SQSClient;
use aws_smithy_runtime_api::client::result::SdkError;
use bytes::{Bytes, BytesMut};
use memchr::memchr_iter;
use percent_encoding::percent_decode_str;
use std::{sync::Arc, time::Duration};
use tangent_shared::sqs::SQSConfig;
use tokio_util::sync::CancellationToken;

use crate::{
    sources::decoding,
    worker::{Ack, Record, WorkerPool},
};

pub async fn run_consumer(
    cfg: SQSConfig,
    max_chunk: usize,
    pool: Arc<WorkerPool>,
    shutdown: CancellationToken,
) -> Result<()> {
    let aws_cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
    let sqs_client = SQSClient::new(&aws_cfg);
    let s3_client = S3Client::new(&aws_cfg);
    let qurl = Arc::new(cfg.queue_url);
    let dc = cfg.decoding.clone();

    let fwd_shutdown = shutdown.clone();

    loop {
        tokio::select! {
            _ = fwd_shutdown.cancelled() => break,
            res = sqs_client.receive_message()
                    .queue_url(qurl.as_str())
                    .wait_time_seconds(20)
                    .max_number_of_messages(10)
                    .send() => {
                match res {
                    Ok(out) => {
                        for msg in out.messages.unwrap_or_default() {
                            if let (Some(body), Some(handle)) = (msg.body(), msg.receipt_handle().map(|s| s.to_string())){
                                let ack = Arc::new(SqsAck::new(sqs_client.clone(), qurl.clone(), handle));

                                let mut s3_notification = false;
                                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&body) {
                                    if let Some(recs) = v.get("Records").and_then(|r| r.as_array()) {
                                        for (idx, r) in recs.iter().enumerate() {
                                            let bucket = r.pointer("/s3/bucket/name").and_then(|x| x.as_str());
                                            let key_enc = r.pointer("/s3/object/key").and_then(|x| x.as_str());
                                            if let (Some(bucket), Some(key_enc)) = (bucket, key_enc) {
                                                let mut key = percent_decode_str(key_enc).decode_utf8_lossy().into_owned();
                                                if key.contains('+') { key = key.replace('+', " "); }

                                                let obj = match s3_client.get_object().bucket(bucket).key(&key).send().await {
                                                    Ok(o) => o,
                                                    Err(e) => { tracing::error!("S3 get {bucket}/{key}: {e}"); continue; }
                                                };
                                                let aws_sdk_s3::operation::get_object::GetObjectOutput { body: body_stream, .. } = obj;
                                                let collected = match body_stream.collect().await {
                                                   Ok(b) => b,
                                                   Err(e) => { tracing::error!("S3 collect {bucket}/{key}: {e}"); continue; }
                                               };

                                                let bytes = aws_smithy_types::byte_stream::AggregatedBytes::from(collected).to_vec();
                                                let fmt = dc.resolve_format(&bytes);
                                                let ndjson = decoding::normalize_to_ndjson(fmt, &bytes);

                                                let final_ack = if idx + 1 == recs.len() { Some(ack.clone() as Arc<dyn Ack>) } else { None };
                                                dispatch_ndjson_chunks(pool.clone(), ndjson, max_chunk, final_ack).await;
                                                s3_notification = true;
                                            }
                                        }
                                    }
                                }
                                if s3_notification {
                                    continue;
                                }

                                let bytes = body.as_bytes().to_vec();
                                let sniff = &bytes[..bytes.len().min(8)];
                                let comp = dc.resolve_compression(None, None, sniff);

                                let raw = match decoding::decompress_bytes(comp, &bytes) {
                                    Ok(v) => v,
                                    Err(e) => {
                                        tracing::warn!(error=?e, "decompress failed; emitting raw body");
                                        let mut b = BytesMut::with_capacity(bytes.len() + 1);
                                        b.extend_from_slice(&bytes);
                                        if !bytes.ends_with(b"\n") { b.extend_from_slice(b"\n"); }
                                        let _ = pool.dispatch(Record { payload: b.freeze(), ack: Some(ack) }).await;
                                        continue;
                                    }
                                };

                                let fmt = dc.resolve_format(&raw);
                                let ndjson = decoding::normalize_to_ndjson(fmt, &raw);
                                dispatch_ndjson_chunks(pool.clone(), ndjson, max_chunk, Some(ack)).await;
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
                                        "SQS ReceiveMessage service error: {}",
                                        msg
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

async fn dispatch_ndjson_chunks(
    pool: Arc<WorkerPool>,
    data: Vec<u8>,
    max_chunk: usize,
    ack: Option<Arc<dyn Ack>>,
) {
    let mut cur = BytesMut::with_capacity(max_chunk.min(data.len()).max(1));

    let mut start = 0usize;
    let mut line_ends: Vec<usize> = memchr_iter(b'\n', &data).collect();

    let had_trailing_newline = line_ends
        .last()
        .map(|&i| i + 1 == data.len())
        .unwrap_or(false);
    if !had_trailing_newline {
        line_ends.push(data.len());
    }

    let mut chunks_to_go_estimate = 0usize;
    let mut est_size = 0usize;
    for &end in &line_ends {
        let line_len = end - start;
        if line_len > max_chunk {
            chunks_to_go_estimate += 1;
            start = end + 0;
            continue;
        }
        if est_size + line_len > max_chunk {
            chunks_to_go_estimate += 1;
            est_size = 0;
        }
        est_size += line_len;
        start = end + 0;
    }
    if est_size > 0 {
        chunks_to_go_estimate += 1;
    }

    cur.clear();
    start = 0;
    let mut chunks_left = chunks_to_go_estimate;

    tracing::debug!(
        chunks = chunks_left,
        est_size = est_size,
        buf_size = data.len(),
        "dispatching chunks"
    );

    for &end in &line_ends {
        let line = &data[start..end];

        if line.len() > max_chunk && cur.is_empty() {
            chunks_left -= 1;
            let final_ack = if chunks_left == 0 { ack.clone() } else { None };
            pool.dispatch(Record {
                payload: Bytes::copy_from_slice(line),
                ack: final_ack,
            })
            .await;
        } else {
            if cur.len() + line.len() > max_chunk {
                chunks_left -= 1;
                let final_ack = if chunks_left == 0 { ack.clone() } else { None };
                pool.dispatch(Record {
                    payload: cur.split().freeze(),
                    ack: final_ack,
                })
                .await;
            }
            cur.extend_from_slice(line);
        }

        start = end + 1;
    }

    if !cur.is_empty() {
        chunks_left -= 1;
        let final_ack = if chunks_left == 0 { ack } else { None };
        pool.dispatch(Record {
            payload: cur.freeze(),
            ack: final_ack,
        })
        .await;
    }
}

pub struct SqsAck {
    client: SQSClient,
    queue_url: Arc<String>,
    receipt_handle: String,
}

impl SqsAck {
    pub fn new(client: SQSClient, queue_url: Arc<String>, receipt_handle: String) -> Self {
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
