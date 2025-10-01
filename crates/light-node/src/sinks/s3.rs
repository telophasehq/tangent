use anyhow::{bail, Result};
use aws_sdk_s3::Client;
use aws_smithy_types::byte_stream::ByteStream;
use std::path::Path;
use tangent_shared::s3::S3Config;

use crate::sinks::wal::WALSink;

pub struct S3Sink {
    name: String,
    client: Client,
    bucket_name: String,
}

#[async_trait::async_trait]
impl WALSink for S3Sink {
    async fn write_path(&self, path: &Path) -> Result<()> {
        let body = ByteStream::from_path(path).await?;
        let key = path
            .file_name()
            .and_then(|os| os.to_str()) // convert OsStr → &str
            .map(|s| s.to_string())
            .unwrap_or_else(|| {
                let id = ulid::Ulid::new().to_string();
                format!("tangent-{id}.ndjson")
            });

        let put_res = self
            .client
            .put_object()
            .bucket(&self.bucket_name)
            .key(&key)
            .body(body)
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
