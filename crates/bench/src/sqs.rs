use anyhow::{Context, Result};
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::primitives::ByteStream;
use bytes::{BufMut, BytesMut};
use serde_json::Value;
use std::{
    fs,
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tangent_shared::sqs::SQSConfig;
use tokio::task::JoinHandle;
use tracing::info;

pub async fn run_bench(
    cfg: &SQSConfig,
    bucket: String,
    object_prefix: Option<String>,
    max_bytes: usize,
    connections: u16,
    payload_path: PathBuf,
    seconds: u64,
) -> Result<()> {
    let payload = fs::read_to_string(&payload_path)
        .with_context(|| format!("failed to read payload file {}", payload_path.display()))?;
    let one_line = serde_json::to_string(&serde_json::from_str::<Value>(&payload)?)
        .context("payload is not valid JSON")?;

    let mut line = BytesMut::from(one_line.as_bytes());
    line.put_u8(b'\n');

    while line.len() <= max_bytes {
        line.extend_from_slice(one_line.as_bytes());
        line.put_u8(b'\n');
    }
    let line_len = line.len();

    info!("===Starting S3+SQS benchmark===");
    info!(
        "bucket={} queue={} payload={} object_bytes={} connections={} duration={}s",
        bucket,
        cfg.queue_url,
        payload_path.display(),
        line.len(),
        connections,
        seconds
    );

    let aws_cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;

    let s3 = Arc::new(S3Client::new(&aws_cfg));

    let deadline = Instant::now() + Duration::from_secs(seconds);
    let prefix = object_prefix.unwrap_or_else(|| "bench/".to_string());

    let mut handles: Vec<JoinHandle<Result<u64>>> = Vec::with_capacity(connections as usize);
    for worker_id in 0..connections {
        let s3c = s3.clone();
        let bucket_cl = bucket.clone();
        let prefix_cl = prefix.clone();
        let bytes = line.clone().to_vec();

        handles.push(tokio::spawn(async move {
            let mut counter: u64 = 0;

            let base = format!(
                "{}{}-w{}-{}",
                prefix_cl,
                chrono::Utc::now().format("%Y%m%dT%H%M%S"),
                worker_id,
                std::process::id()
            );

            while Instant::now() < deadline {
                let key = format!("{}-{:08}.ndjson", base, counter);
                counter += 1;

                let put_res = s3c
                    .put_object()
                    .bucket(&bucket_cl)
                    .key(&key)
                    .body(ByteStream::from(bytes.clone()))
                    .send()
                    .await;

                let _ = match put_res {
                    Ok(_) => (),
                    Err(e) => {
                        tracing::warn!("put_object failed for {}/{}: {e}", bucket_cl, key);
                        continue;
                    }
                };
            }

            Ok(counter)
        }));
    }

    // Aggregate results
    let mut total_objects: u64 = 0;
    for h in handles {
        let objs = h.await??;
        total_objects += objs;
    }

    let total_bytes = total_objects * (line_len as u64);
    let mb_per_sec = (total_bytes as f64 / (1024.0 * 1024.0)) / (seconds as f64);

    info!(
        "objects/sec = {},  MB/sec = {:.2}",
        total_objects / seconds,
        mb_per_sec
    );

    Ok(())
}
