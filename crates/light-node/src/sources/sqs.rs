use anyhow::Result;
use async_trait::async_trait;
use aws_config;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_sqs::Client as SQSClient;
use bytes::{Bytes, BytesMut};
use memchr::memchr_iter;
use std::{sync::Arc, time::Duration};
use tangent_shared::sqs::SQSConfig;
use tokio_util::sync::CancellationToken;

use crate::worker::{Ack, Incoming, Record, WorkerPool};

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

    let fwd_shutdown = shutdown.clone();

    tokio::spawn(async move {
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
                                if let (Some(body), Some(handle)) = (msg.body(), msg.receipt_handle().map(|s| s.to_string())) {
                                    let ack = Arc::new(SqsAck::new(sqs_client.clone(), qurl.clone(), handle));
                                    let v: serde_json::Value = match serde_json::from_str(&body) {
                                        Ok(v) => v,
                                        Err(_) => {
                                            let mut b = BytesMut::with_capacity(body.len() + 1);
                                            b.extend_from_slice(body.as_bytes());
                                            b.extend_from_slice(b"\n");

                                            let _ = pool.dispatch(Incoming::Record(Record {
                                                payload: b.into(),
                                                ack: Some(ack),
                                            })).await;
                                            continue;
                                        }
                                    };

                                    // S3 notifications
                                    if let Some(recs) = v.get("Records").and_then(|r| r.as_array()) {
                                        for (idx, r) in recs.iter().enumerate() {
                                            if let (Some(bucket), Some(key)) = (
                                                r.pointer("/s3/bucket/name").and_then(|x| x.as_str()),
                                                r.pointer("/s3/object/key").and_then(|x| x.as_str()),
                                            ) {
                                                let obj = match s3_client.get_object().bucket(bucket).key(key).send().await {
                                                    Ok(o) => o,
                                                    Err(e) => {
                                                        tracing::error!("Error fetching S3 object {bucket}/{key}: {e}");
                                                        continue;
                                                    }
                                                };

                                                let body = match obj.body.collect().await {
                                                    Ok(b) => b,
                                                    Err(e) => {
                                                        tracing::error!("Error collecting S3 object body {bucket}/{key}: {e}");
                                                        continue;
                                                    }
                                                };

                                                let bytes = aws_smithy_types::byte_stream::AggregatedBytes::from(body).to_vec();

                                                let mut final_ack: Option<Arc<dyn Ack>> = None;
                                                if idx + 1 == recs.len() {
                                                    if let Some(a) = ack.clone().into() {
                                                        final_ack = Some(a);
                                                    }
                                                }
                                                dispatch_ndjson_chunks(pool.clone(), bytes, max_chunk, final_ack).await;
                                            }
                                        }
                                        continue;
                                    }
                                }
                            }
                        }
                        Err(e) => {
                            tracing::warn!("sqs receive error: {e}");
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                    }
                }
            }
        }
    });

    shutdown.cancelled().await;
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

    for &end in &line_ends {
        let line = &data[start..end];

        if line.len() > max_chunk && cur.is_empty() {
            chunks_left -= 1;
            let final_ack = if chunks_left == 0 { ack.clone() } else { None };
            pool.dispatch(Incoming::Record(Record {
                payload: Bytes::copy_from_slice(line),
                ack: final_ack,
            }))
            .await;
        } else {
            if cur.len() + line.len() > max_chunk {
                chunks_left -= 1;
                let final_ack = if chunks_left == 0 { ack.clone() } else { None };
                pool.dispatch(Incoming::Record(Record {
                    payload: cur.split().freeze(),
                    ack: final_ack,
                }))
                .await;
            }
            cur.extend_from_slice(line);
        }

        start = end + 1;
    }

    if !cur.is_empty() {
        chunks_left -= 1;
        let final_ack = if chunks_left == 0 { ack } else { None };
        pool.dispatch(Incoming::Record(Record {
            payload: cur.freeze(),
            ack: final_ack,
        }))
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
