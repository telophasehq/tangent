//! A rolling_file sink drops a line its encoding cannot take, keeps the
//! rest, and counts it in `tangent_sink_records_unfit_total`. The counter is
//! the process's: this file runs as a process of its own, so only its tests
//! move it, and each holds [`COUNTER`] while it does.

use bytes::BytesMut;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tangent_runtime::sinks::manager::{Sink, SinkWrite};
use tangent_runtime::sinks::rolling_file::RollingFileSink;
use tangent_runtime::sinks::s3::S3SinkItem;
use tangent_runtime::sinks::wal::{DurableFileSink, WALSink};
use tangent_runtime::SINK_RECORDS_UNFIT_TOTAL;
use tangent_shared::sinks::common::{Compression, Encoding};
use tokio::sync::Mutex;

static COUNTER: Mutex<()> = Mutex::const_new(());

fn dropped() -> u64 {
    SINK_RECORDS_UNFIT_TOTAL
        .with_label_values(&["dropped"])
        .get()
}

/// A rolling_file directory whose first `fails` sends fail, as they do
/// while a disk is full or a bucket out of reach.
struct Flaky {
    fails: AtomicUsize,
    dir: RollingFileSink,
}

#[async_trait::async_trait]
impl WALSink for Flaky {
    async fn write_path_with(
        &self,
        path: &Path,
        name: &str,
        encoding: &Encoding,
        compression: &Compression,
        meta: &S3SinkItem,
    ) -> anyhow::Result<()> {
        let left = self.fails.load(Ordering::SeqCst);
        if left > 0 {
            self.fails.store(left - 1, Ordering::SeqCst);
            anyhow::bail!("the disk is full");
        }
        self.dir
            .write_path_with(path, name, encoding, compression, meta)
            .await
    }
}

/// The files a WAL in `encoding` puts in a rolling_file directory for
/// `payload`, its first `fails` sends failing, and how many lines
/// `tangent_sink_records_unfit_total` counted dropped meanwhile.
async fn sealed(encoding: Encoding, payload: &str, fails: usize) -> (Vec<Vec<u8>>, u64) {
    let _counting = COUNTER.lock().await;
    let root = tempfile::tempdir().unwrap();
    let out = root.path().join("out");
    let wal = DurableFileSink::new(
        Arc::new(Flaky {
            fails: AtomicUsize::new(fails),
            dir: RollingFileSink::new(&out),
        }),
        root.path().join("wal"),
        4,
        1 << 20,
        Duration::from_secs(3600),
        Compression::None,
        encoding,
    )
    .await
    .unwrap();
    let before = dropped();
    wal.write(SinkWrite {
        sink_name: Arc::from("logs"),
        payload: BytesMut::from(payload),
        s3: Some(S3SinkItem {
            bucket_name: Arc::from(""),
            key_prefix: None,
        }),
    })
    .await
    .unwrap();
    // a flush tries a sealed file whose send failed again
    for _ in 0..=fails {
        wal.flush().await.unwrap();
    }
    (files(&out), dropped() - before)
}

/// Each file's bytes; none if the sink never made the directory.
fn files(dir: &Path) -> Vec<Vec<u8>> {
    std::fs::read_dir(dir).map_or(Vec::new(), |d| {
        d.map(|e| std::fs::read(e.unwrap().path()).unwrap())
            .collect()
    })
}

#[tokio::test]
async fn a_json_file_drops_a_line_that_is_not_json_and_keeps_the_rest() {
    let payload = "{\"n\":1}\nnot json\n{\"n\":2}\n";
    let (files, dropped) = sealed(Encoding::JSON, payload, 0).await;
    assert_eq!(
        files,
        [br#"[{"n":1},{"n":2}]"#],
        "the sealed file is in place"
    );
    assert_eq!(dropped, 1);
}

#[tokio::test]
async fn an_avro_file_drops_a_line_its_schema_does_not_take_and_keeps_the_rest() {
    let avro = Encoding::Avro {
        schema: r#"{"type":"record","name":"r","fields":[{"name":"n","type":"string"}]}"#.into(),
    };
    // not JSON, then a number where the schema has a string
    let payload = "{\"n\":\"1\"}\nnot json\n{\"n\":2}\n{\"n\":\"3\"}\n";
    let (files, dropped) = sealed(avro, payload, 0).await;
    assert_eq!(files.len(), 1, "the sealed file is in place");
    #[derive(serde::Deserialize)]
    struct R {
        n: String,
    }
    let got: Vec<String> = apache_avro::Reader::new(&files[0][..])
        .unwrap()
        .map(|v| apache_avro::from_value::<R>(&v.unwrap()).unwrap().n)
        .collect();
    assert_eq!(got, ["1", "3"]);
    assert_eq!(dropped, 2);
}

#[tokio::test]
async fn a_dropped_line_is_counted_once_its_file_is_sent_not_at_each_try() {
    // each try encodes the sealed file again, dropping the line again
    let payload = "{\"n\":1}\nnot json\n{\"n\":2}\n";
    let (files, dropped) = sealed(Encoding::JSON, payload, 2).await;
    assert_eq!(files, [br#"[{"n":1},{"n":2}]"#], "sent on the third try");
    assert_eq!(dropped, 1);
}
