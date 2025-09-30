use anyhow::{bail, Result};
use aws_sdk_s3::Client;
use aws_smithy_types::byte_stream::ByteStream;
use bytes::Bytes;
use tangent_shared::s3::S3Config;

use crate::sinks::manager::Sink;

pub struct S3Sink {
    name: String,
    client: Client,
    bucket_name: String,
}

#[async_trait::async_trait]
impl Sink for S3Sink {
    async fn write(&self, payload: Bytes) -> Result<()> {
        let id = ulid::Ulid::new().to_string();
        let key = format!("tangent-{id}.ndjson");

        let put_res = self
            .client
            .put_object()
            .bucket(&self.bucket_name)
            .key(&key)
            .body(ByteStream::from(payload))
            .send()
            .await;

        let _ = match put_res {
            Ok(_) => {
                tracing::info!("successfully wrote {} to {}", key, self.bucket_name);
            }
            Err(e) => {
                tracing::warn!(
                    "put_object failed for sink {} {}/{}: {e}",
                    self.name,
                    self.bucket_name,
                    key
                );
                bail!(
                    "write failed for sink {} {}/{}: {e}",
                    self.name,
                    self.bucket_name,
                    key
                )
            }
        };

        Ok(())
    }
}

impl S3Sink {
    pub async fn new(name: &String, cfg: &S3Config) -> Result<Self> {
        let aws_cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let client = Client::new(&aws_cfg);

        Ok(S3Sink {
            name: name.to_string(),
            client: client,
            bucket_name: cfg.bucket_name.clone(),
        })
    }
}
