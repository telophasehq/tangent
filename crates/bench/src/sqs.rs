use anyhow::Result;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client as S3Client;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tangent_shared::sources::sqs::SQSConfig;
use tokio::task::JoinHandle;
use tracing::info;

pub async fn run_bench(
    name: &String,
    cfg: &SQSConfig,
    bucket: String,
    object_prefix: Option<String>,
    connections: u16,
    payload: Vec<u8>,
    seconds: u64,
) -> Result<()> {
    info!("===Starting benchmark===");
    info!(
        "source={} bucket={} queue={} connections={} duration={}s",
        name, bucket, cfg.queue_url, connections, seconds
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
        let bytes = payload.clone().to_vec();

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

    for h in handles {
        h.await??;
    }

    Ok(())
}
