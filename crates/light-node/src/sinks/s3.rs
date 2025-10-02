use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client;
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_types::byte_stream::ByteStream;
use std::path::Path;
use tangent_shared::Encoding;
use tokio::fs::File;
use tokio::io::AsyncReadExt;
use ulid::Ulid;

use crate::sinks::wal::WALSink;

pub struct S3Sink {
    name: String,
    client: Client,
    bucket_name: String,
    part_size: usize,
}

#[async_trait]
impl WALSink for S3Sink {
    async fn write_path(&self, path: &Path, encoding: &Encoding) -> Result<()> {
        let key = derive_key_from_path(path, &encoding);

        let content_type = Encoding::content_type(&encoding);
        let content_encoding = match path.extension().and_then(|e| e.to_str()) {
            Some("gz") => Some("gzip"),
            Some("zst") => Some("zstd"),
            _ => None,
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
            put.send().await?;
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
            .with_context(|| format!("open {:?}", path))?;
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
                        part_number
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
            bail!("no data read for multipart upload: {:?}", path);
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
    pub async fn new(name: &String, cfg: &tangent_shared::s3::S3Config) -> Result<Self> {
        let aws_cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let client = Client::new(&aws_cfg);

        Ok(Self {
            name: name.clone(),
            client,
            bucket_name: cfg.bucket_name.clone(),
            part_size: 8 * 1024 * 1024,
        })
    }
}

fn derive_key_from_path(path: &Path, enc: &Encoding) -> String {
    let mut stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("tangent");
    if stem.ends_with(".sealed") {
        stem = &stem[..stem.len() - ".sealed".len()];
    }
    let base = if stem.is_empty() {
        Ulid::new().to_string()
    } else {
        stem.to_string()
    };

    let ext = Encoding::extension(enc);
    let comp = path.extension().and_then(|e| e.to_str());
    match comp {
        Some("zst") => format!("{base}.{ext}.zst"),
        Some("gz") => format!("{base}.{ext}.gz"),
        _ => format!("{base}.{ext}"),
    }
}
