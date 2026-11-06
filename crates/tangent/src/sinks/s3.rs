use anyhow::{bail, Context, Result};
use async_trait::async_trait;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client;
use aws_smithy_runtime_api::client::result::SdkError;
use aws_smithy_types::byte_stream::ByteStream;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;
use tangent_shared::sinks::common::{Compression, Encoding, SinkConfig, SinkKind};
use tangent_shared::sinks::s3::S3Config;
use tokio::fs::File;
use tokio::io::AsyncReadExt;

use crate::sinks::event_time::FieldPath;
use crate::sinks::template::Template;
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
    pub async fn new(name: Arc<str>, cfg: &S3Config) -> Result<Self> {
        let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
        if let Some(region) = crate::sources::poll::opt(&cfg.region) {
            loader = loader.region(aws_config::Region::new(region.to_string()));
        }
        let client = Client::new(&loader.load().await);

        Ok(Self {
            name: name,
            client,
            bucket_name: Arc::from(cfg.bucket_name.as_str()),
            part_size: 8 * 1024 * 1024,
        })
    }
}

/// An s3 sink's `key_prefix`: a strftime template, rendered whole in UTC
/// for each record's own time, read from its `timestamp_field` (the time
/// the record reaches the sink, for one without). It is a path in the bucket, so
/// it may not start with `/` or hold an empty (`//`), `.` or `..` segment:
/// S3 keeps each as written, so `/logs` objects are not under `logs/` and
/// `logs//2026` ones not under `logs/2026/` for a listing or a table's
/// location, and a tool that copies the bucket to disk resolves `.` and
/// `..` (`aws s3 cp --recursive` skips a key that climbs out of the
/// directory, and writes `a/../b` to `b`). A trailing `/` is fine: the key
/// joins the prefix and the name with one `/`.
pub fn key_prefix(template: &str) -> Result<Template> {
    // a `{{ field }}` template is not rendered: its braces would go into
    // every key
    if template.contains("{{") {
        bail!(
            "key_prefix {template:?}: strftime only; a `{{{{ field }}}}` template is not rendered"
        );
    }
    let Some(parsed) = Template::parse(template) else {
        bail!("key_prefix {template:?} is not a strftime template (`%%` is a `%`)");
    };
    // empty, the prefix is the bucket's root
    let mut segments = template.trim_end_matches('/').split('/');
    if !template.is_empty() && segments.any(|s| matches!(s, "" | "." | "..")) {
        bail!(
            "key_prefix {template:?}: a path in the bucket, without a leading `/`, `//`, or a `.` or `..` segment"
        );
    }
    Ok(parsed)
}

/// Each s3 sink's `key_prefix` is one [`key_prefix`] takes, and each s3 and
/// rolling_file sink's `timestamp_field` one [`FieldPath::parse`] takes, or
/// the error names the sink: checked when `tangent run` and `tangent config`
/// load a config.
pub fn check_sinks(sinks: &BTreeMap<Arc<str>, SinkConfig>) -> Result<()> {
    for (name, cfg) in sinks {
        let field = match &cfg.kind {
            SinkKind::S3(S3Config {
                key_prefix: prefix,
                timestamp_field: field,
                ..
            }) => {
                if let Some(p) = prefix {
                    key_prefix(p).with_context(|| format!("sink '{name}'"))?;
                }
                field
            }
            SinkKind::RollingFile(c) => &c.timestamp_field,
            _ => continue,
        };
        FieldPath::parse(field).with_context(|| format!("sink '{name}'"))?;
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
    use chrono::{DateTime, Utc};

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
    fn a_key_prefix_renders_a_time_in_utc() {
        // 21:04 UTC on the 2nd, whatever zone the time is written in
        let t = at("2026-11-02T13:04:05-08:00");
        let render = |p: &str| key_prefix(p).unwrap().render(t);
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
            "/",
            "logs/../%Y",
            "..",
            "logs//%Y",
            "%Y//%m",
            "logs/./%Y",
            ".",
        ] {
            assert!(key_prefix(bad).is_err(), "{bad}");
        }
        for good in ["", "logs/%Y/", "logs/%Y//", "logs/.%Y/x..y"] {
            assert!(key_prefix(good).is_ok(), "{good}");
        }
        let braces = key_prefix("logs/{{ source_type }}/%F/").unwrap_err();
        assert!(braces.to_string().contains("strftime only"), "{braces}");
        let sinks = |prefix: &str| -> BTreeMap<Arc<str>, SinkConfig> {
            let yaml = format!("type: s3\nbucket_name: b\nkey_prefix: \"{prefix}\"\n");
            BTreeMap::from([(Arc::from("archive"), serde_yaml::from_str(&yaml).unwrap())])
        };
        assert!(check_sinks(&sinks("logs/%Y/%m/%d")).is_ok());
        let e = format!("{:#}", check_sinks(&sinks("logs/%Q")).unwrap_err());
        assert!(
            e.contains("sink 'archive'") && e.contains("not a strftime template"),
            "{e}"
        );
    }

    #[test]
    fn a_timestamp_field_names_a_field() {
        let sinks = |extra: &str| -> BTreeMap<Arc<str>, SinkConfig> {
            let yaml = format!("type: s3\nbucket_name: b\n{extra}");
            BTreeMap::from([(Arc::from("archive"), serde_yaml::from_str(&yaml).unwrap())])
        };
        for good in [
            "",
            "timestamp_field: event.time\n",
            "timestamp_field: events[0].time\n",
        ] {
            assert!(check_sinks(&sinks(good)).is_ok(), "{good}");
        }
        for bad in ["\"\"", "event..time", ".ts", "ts.", "\"events[x].time\""] {
            let e = check_sinks(&sinks(&format!("timestamp_field: {bad}\n"))).unwrap_err();
            assert!(e.to_string().contains("sink 'archive'"), "{e}");
        }
        // a rolling_file sink's too
        let lake = |field: &str| -> BTreeMap<Arc<str>, SinkConfig> {
            let yaml = format!(
                "type: rolling_file\npath: lake/%Y\nwal_path: wal\ntimestamp_field: {field}\n"
            );
            BTreeMap::from([(Arc::from("lake"), serde_yaml::from_str(&yaml).unwrap())])
        };
        assert!(check_sinks(&lake("event.time")).is_ok());
        let e = check_sinks(&lake("event..time")).unwrap_err();
        assert!(e.to_string().contains("sink 'lake'"), "{e}");
    }

    #[test]
    fn an_objects_key_is_its_rendered_prefix_then_its_name() {
        let prefix = key_prefix("logs/cloudtrail/%Y/%m/%d").unwrap();
        let rendered = prefix.render(at("2026-11-02T13:04:05Z"));
        assert_eq!(
            object_key(Some(&rendered), "01JBX4N3V2.parquet"),
            "logs/cloudtrail/2026/11/02/01JBX4N3V2.parquet"
        );
    }

    #[tokio::test]
    async fn the_client_uses_the_configured_region() {
        let cfg = S3Config {
            bucket_name: "lake".into(),
            region: Some("eu-west-3".into()),
            key_prefix: None,
            timestamp_field: "ts".into(),
            wal_path: "/unused".into(),
            max_file_age_seconds: 60,
        };
        let sink = S3Sink::new(Arc::from("lake"), &cfg).await.unwrap();
        assert_eq!(
            sink.client.config().region().map(|r| r.as_ref()),
            Some("eu-west-3")
        );
    }

    #[tokio::test]
    async fn an_empty_region_leaves_the_default_chain() {
        let cfg = S3Config {
            bucket_name: "lake".into(),
            region: Some("".into()),
            key_prefix: None,
            timestamp_field: "ts".into(),
            wal_path: "/unused".into(),
            max_file_age_seconds: 60,
        };
        let sink = S3Sink::new(Arc::from("lake"), &cfg).await.unwrap();
        assert_ne!(sink.client.config().region().map(|r| r.as_ref()), Some(""));
    }
}
