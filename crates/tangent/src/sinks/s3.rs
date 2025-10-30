use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client;
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_types::byte_stream::ByteStream;
use std::path::Path;
use tangent_shared::sinks::common::{Compression, Encoding};
use tangent_shared::sinks::s3::S3Config;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

use crate::sinks::wal::{base_for, WALSink};

pub struct S3Sink {
    name: String,
    client: Client,
    bucket_name: String,
    part_size: usize,
}

#[derive(Clone)]

pub struct S3SinkItem {
    pub bucket_name: String,
    pub key_prefix: Option<String>,
}

#[async_trait]
impl WALSink for S3Sink {
    async fn write_path_with(
        &self,
        path: &Path,
        encoding: &Encoding,
        compression: &Compression,
        meta: &S3SinkItem,
    ) -> Result<()> {
        let key = object_key_from(path, meta.key_prefix.as_deref(), encoding, compression);

        let content_type = Encoding::content_type(encoding);
        let content_encoding = match compression {
            Compression::None => None,
            Compression::Gzip { .. } => Some("gzip"),
            Compression::Zstd { .. } => Some("zstd"),
            Compression::Snappy { .. } => None,
            Compression::Deflate { .. } => None,
        };

        let size = tokio::fs::metadata(path).await?.len();

        if size < 5 * 1024 * 1024 {
            let mut put = self
                .client
                .put_object()
                .bucket(&self.bucket_name)
                .key(&key)
                .content_type(content_type)
                .body(ByteStream::from_path(path).await?);

            if let Some(enc) = content_encoding {
                put = put.content_encoding(enc);
            }
            put.send().await.map_err(|e| {
                if let SdkError::ServiceError(se) = &e {
                    let err = se.err();
                    tracing::warn!(
                        target: "s3",
                        code = ?err.meta().code(),
                        msg  = ?err.meta().message(),
                        status = ?se.raw().status(),
                        bucket = %self.bucket_name,
                        key = %key,
                        "put_object failed"
                    );
                }
                anyhow::anyhow!("put_object {} {}: {}", self.bucket_name, key, e)
            })?;
            return Ok(());
        }

        let mut create = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket_name)
            .key(&key)
            .content_type(content_type);

        if let Some(enc) = content_encoding {
            create = create.content_encoding(enc);
        }

        let create = create.send().await.map_err(|e| {
            if let SdkError::ServiceError(se) = &e {
                let err = se.err();
                tracing::warn!(
                    "create MPU failed: code={:?} msg={:?} status={:?}",
                    err.meta().code(),
                    err.meta().message(),
                    se.raw().status()
                );
            }
            anyhow::anyhow!("create_multipart_upload {}/{}: {e}", self.bucket_name, key)
        })?;

        let upload_id = create
            .upload_id()
            .context("missing upload_id from create_multipart_upload")?
            .to_string();

        let mut file = File::open(path)
            .await
            .with_context(|| format!("open {}", path.display()))?;
        let mut parts: Vec<CompletedPart> = Vec::new();
        let mut buf = vec![0u8; self.part_size];
        let mut part_number: i32 = 1;

        loop {
            let mut filled = 0usize;
            while filled < buf.len() {
                let n = file.read(&mut buf[filled..]).await?;
                if n == 0 {
                    break;
                }
                filled += n;
            }
            if filled == 0 {
                break;
            }

            let chunk = &buf[..filled];
            let body = ByteStream::from(chunk.to_vec());

            let up = self
                .client
                .upload_part()
                .bucket(&self.bucket_name)
                .key(&key)
                .upload_id(&upload_id)
                .part_number(part_number)
                .body(body)
                .send()
                .await;

            let up = match up {
                Ok(res) => res,
                Err(e) => {
                    let _ = self
                        .client
                        .abort_multipart_upload()
                        .bucket(&self.bucket_name)
                        .key(&key)
                        .upload_id(&upload_id)
                        .send()
                        .await;
                    bail!(
                        "upload_part failed for sink {} key {} part {}: {e}",
                        self.name,
                        key,
                        part_number,
                    );
                }
            };

            let etag = up.e_tag().unwrap_or_default().to_string();
            parts.push(
                CompletedPart::builder()
                    .e_tag(etag)
                    .part_number(part_number)
                    .build(),
            );

            part_number += 1;
        }

        if parts.is_empty() {
            let _ = self
                .client
                .abort_multipart_upload()
                .bucket(&self.bucket_name)
                .key(&key)
                .upload_id(&upload_id)
                .send()
                .await;
            bail!("no data read for multipart upload: {}", path.display());
        }

        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket_name)
            .key(&key)
            .upload_id(&upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build(),
            )
            .send()
            .await
            .with_context(|| format!("complete_multipart_upload {}/{}", self.bucket_name, key))?;

        tracing::info!("upload completed {} to {}", key, self.bucket_name);
        Ok(())
    }
}

impl S3Sink {
    pub async fn new(name: &str, cfg: &S3Config) -> Result<Self> {
        let aws_cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let client = Client::new(&aws_cfg);

        Ok(Self {
            name: name.to_owned(),
            client,
            bucket_name: cfg.bucket_name.clone(),
            part_size: 8 * 1024 * 1024,
        })
    }
}

fn object_key_from(
    local_path: &Path,
    prefix: Option<&str>,
    enc: &Encoding,
    comp: &Compression,
) -> String {
    let base = base_for(local_path);
    let stem = base.file_name().unwrap().to_string_lossy();

    let mut name = String::from(stem.as_ref());
    name.push_str(enc.extension());
    name.push_str(comp.extension());

    if let Some(p) = prefix {
        if p.is_empty() {
            name
        } else {
            format!("{}/{}", p.trim_end_matches('/'), name)
        }
    } else {
        name
    }
}
