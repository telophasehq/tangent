use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client;
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_types::byte_stream::ByteStream;
use chrono::{DateTime, Utc};
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use tangent_shared::sinks::common::{Compression, Encoding, SinkConfig, SinkKind};
use tangent_shared::sinks::s3::S3Config;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

use crate::sinks::rolling_file;
use crate::sinks::wal::{compressed_whole, WALSink};

pub struct S3Sink {
    name: Arc<str>,
    client: Client,
    bucket_name: Arc<str>,
    part_size: usize,
}

#[derive(Clone)]

pub struct S3SinkItem {
    pub bucket_name: Arc<str>,
    pub key_prefix: Option<Arc<str>>,
}

#[async_trait]
impl WALSink for S3Sink {
    async fn write_path_with(
        &self,
        path: &Path,
        name: &str,
        encoding: &Encoding,
        compression: &Compression,
        meta: &S3SinkItem,
    ) -> Result<()> {
        let key = object_key(meta.key_prefix.as_deref(), name);

        let content_type = Encoding::content_type(encoding);
        // Parquet and Avro compress inside the file: the object is not a
        // zstd or gzip stream, and a client told so would try to decode it
        let content_encoding = match compression {
            Compression::None => None,
            Compression::Gzip { .. } => Some("gzip"),
            Compression::Zstd { .. } => Some("zstd"),
            Compression::Snappy { .. } => None,
            Compression::Deflate { .. } => None,
        }
        .filter(|_| compressed_whole(encoding));

        let size = tokio::fs::metadata(path).await?.len();

        if size < 5 * 1024 * 1024 {
            let mut put = self
                .client
                .put_object()
                .bucket(self.bucket_name.as_ref())
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
            .bucket(self.bucket_name.as_ref())
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
                .bucket(self.bucket_name.as_ref())
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
                        .bucket(self.bucket_name.as_ref())
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
                .bucket(self.bucket_name.as_ref())
                .key(&key)
                .upload_id(&upload_id)
                .send()
                .await;
            bail!("no data read for multipart upload: {}", path.display());
        }

        self.client
            .complete_multipart_upload()
            .bucket(self.bucket_name.as_ref())
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
    pub async fn new(name: Arc<str>, bucket_name: Arc<str>) -> Result<Self> {
        let aws_cfg = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let client = Client::new(&aws_cfg);

        Ok(Self {
            name: name,
            client,
            bucket_name: bucket_name,
            part_size: 8 * 1024 * 1024,
        })
    }
}

/// An s3 sink's `key_prefix`: a strftime template, rendered whole in UTC
/// when a write is routed, as a rolling_file sink's `path` is. It is a path
/// in the bucket, so it may not start with `/` or hold a `..` segment: S3
/// keeps both as written, so `/logs` is not under `logs/` for a listing or
/// a table's location, and `..` is a segment of its own, which a tool that
/// copies the bucket to disk resolves (`aws s3 cp --recursive` skips a key
/// that climbs out of the directory, and writes `a/../b` to `b`).
#[derive(Debug)]
pub struct KeyPrefix(String);

impl KeyPrefix {
    pub fn parse(template: &str) -> Result<Self> {
        if !rolling_file::strftime_renders(template) {
            bail!("key_prefix {template:?} is not a strftime template (`%%` is a `%`)");
        }
        if template.starts_with('/') || template.split('/').any(|s| s == "..") {
            bail!("key_prefix {template:?}: a path in the bucket, without a leading `/` or `..`");
        }
        Ok(Self(template.to_string()))
    }

    /// The prefix of the objects a write routed at `now` goes out in.
    pub fn render(&self, now: DateTime<Utc>) -> Arc<str> {
        Arc::from(now.format(&self.0).to_string())
    }
}

/// Each s3 sink's `key_prefix` is one [`KeyPrefix::parse`] takes, or the
/// error names the sink: checked when `tangent run` and `tangent config`
/// load a config.
pub fn check_key_prefixes(sinks: &BTreeMap<Arc<str>, SinkConfig>) -> Result<()> {
    for (name, cfg) in sinks {
        if let SinkKind::S3(S3Config {
            key_prefix: Some(p),
            ..
        }) = &cfg.kind
        {
            KeyPrefix::parse(p).with_context(|| format!("sink '{name}'"))?;
        }
    }
    Ok(())
}

fn object_key(prefix: Option<&str>, name: &str) -> String {
    match prefix {
        Some(p) if !p.is_empty() => format!("{}/{}", p.trim_end_matches('/'), name),
        _ => name.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_key_is_the_prefix_then_the_name() {
        let name = "01JBX4N3V2.ndjson.zst";
        assert_eq!(
            object_key(Some("logs/"), name),
            "logs/01JBX4N3V2.ndjson.zst"
        );
        assert_eq!(object_key(Some(""), name), name);
        assert_eq!(object_key(None, name), name);
    }

    fn at(rfc3339: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(rfc3339)
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn a_key_prefix_renders_in_utc_at_the_time_a_write_is_routed() {
        // 21:04 UTC on the 2nd, whatever zone the clock is read in
        let t = at("2026-11-02T13:04:05-08:00");
        let render = |p: &str| KeyPrefix::parse(p).unwrap().render(t);
        assert_eq!(
            &*render("logs/cloudtrail/%Y/%m/%d/%H"),
            "logs/cloudtrail/2026/11/02/21"
        );
        assert_eq!(&*render("logs/cloudtrail"), "logs/cloudtrail");
        assert_eq!(&*render("date=%F/100%%"), "date=2026-11-02/100%");
    }

    #[test]
    fn a_key_prefix_is_a_strftime_path_in_the_bucket() {
        // `%#z` is one chrono parses but cannot write
        for bad in [
            "logs/%Q",
            "logs/100%",
            "logs/%#z",
            "/logs/%Y",
            "logs/../%Y",
            "..",
        ] {
            assert!(KeyPrefix::parse(bad).is_err(), "{bad}");
        }
        let sinks = |prefix: &str| -> BTreeMap<Arc<str>, SinkConfig> {
            let yaml = format!("type: s3\nbucket_name: b\nkey_prefix: \"{prefix}\"\n");
            BTreeMap::from([(Arc::from("archive"), serde_yaml::from_str(&yaml).unwrap())])
        };
        assert!(check_key_prefixes(&sinks("logs/%Y/%m/%d")).is_ok());
        let e = format!("{:#}", check_key_prefixes(&sinks("logs/%Q")).unwrap_err());
        assert!(
            e.contains("sink 'archive'") && e.contains("not a strftime template"),
            "{e}"
        );
    }

    #[test]
    fn an_objects_key_is_its_rendered_prefix_then_its_name() {
        let prefix = KeyPrefix::parse("logs/cloudtrail/%Y/%m/%d").unwrap();
        let rendered = prefix.render(at("2026-11-02T13:04:05Z"));
        assert_eq!(
            object_key(Some(&rendered), "01JBX4N3V2.parquet"),
            "logs/cloudtrail/2026/11/02/01JBX4N3V2.parquet"
        );
    }
}
