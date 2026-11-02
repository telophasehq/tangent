use anyhow::{bail, Result};
use apache_avro::Codec;
use arrow_json::ReaderBuilder;
use arrow_schema::Schema;
use bytes::{BufMut, Bytes, BytesMut};
use memchr::{memchr, memchr_iter};
use parquet::basic::{Compression as PqCompression, GzipLevel, ZstdLevel};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::io::{BufRead, Write};
use std::sync::Arc;
use tangent_shared::sinks::common::{Compression, Encoding};

#[must_use]
pub fn ndjson_ensure_newline(mut raw: BytesMut) -> BytesMut {
    if !raw.ends_with(b"\n") {
        raw.put_u8(b'\n');
    }
    raw
}

pub fn ndjson_to_json_array(raw: &[u8]) -> Result<BytesMut> {
    let mut values = Vec::<serde_json::Value>::new();
    for line in raw.split(|&b| b == b'\n') {
        if line.is_empty() {
            continue;
        }
        let v: serde_json::Value = serde_json::from_slice(line)?;
        values.push(v);
    }

    let arr = serde_json::Value::Array(values);
    let mut out = BytesMut::new();
    serde_json::to_writer((&mut out).writer(), &arr)?;
    Ok(out)
}

pub fn normalize_from_ndjson(
    enc: &Encoding,
    comp: &Compression,
    raw: BytesMut,
) -> Result<BytesMut> {
    match enc {
        Encoding::NDJSON => Ok(ndjson_ensure_newline(raw)),
        Encoding::JSON => ndjson_to_json_array(&raw),
        Encoding::Avro { schema: s } => ndjson_to_avro(&raw, s, comp),
        Encoding::Parquet { schema: s } => ndjson_to_parquet(&raw, s, comp),
    }
}

fn ndjson_iter_lines(raw: &[u8]) -> impl Iterator<Item = &[u8]> {
    raw.split(|&b| b == b'\n').filter(|line| !line.is_empty())
}

pub fn ndjson_to_avro(raw: &[u8], avro_schema_json: &str, comp: &Compression) -> Result<BytesMut> {
    let codec = avro_codec_from(comp);
    let schema = apache_avro::Schema::parse_str(avro_schema_json)?;
    let mut writer = apache_avro::Writer::with_codec(&schema, Vec::<u8>::new(), codec);

    for line in ndjson_iter_lines(raw) {
        let value: serde_json::Value = serde_json::from_slice(line)?;
        writer.append_ser(value)?;
    }

    let bytes = writer.into_inner()?;
    Ok(BytesMut::from(bytes.as_slice()))
}

/// Lines in one Arrow batch, and the bytes they may come to: past either,
/// the batch is written and the next begun.
const PARQUET_BATCH_LINES: usize = 8192;
const PARQUET_BATCH_BYTES: usize = 64 << 20;

/// NDJSON as one Parquet file: the columns the Arrow schema names, read out
/// of each line (a field it does not name is skipped; a nested object it
/// names is a struct, its fields leaf columns of their own).
pub fn ndjson_to_parquet(
    raw: &[u8],
    arrow_schema_json: &str,
    comp: &Compression,
) -> Result<BytesMut> {
    let out = write_parquet(raw, Vec::new(), arrow_schema_json, comp)?;
    Ok(BytesMut::from(out.as_slice()))
}

/// [`ndjson_to_parquet`] from a reader to a writer, a batch of lines in
/// memory at a time. Each line must be one JSON object, as NDJSON has it.
pub fn write_parquet<R: BufRead, W: Write + Send>(
    mut input: R,
    out: W,
    arrow_schema_json: &str,
    comp: &Compression,
) -> Result<W> {
    let schema = Arc::new(serde_json::from_str::<Schema>(arrow_schema_json)?);
    let mut decoder = ReaderBuilder::new(Arc::clone(&schema))
        .with_batch_size(PARQUET_BATCH_LINES)
        .build_decoder()?;
    let mut writer = ArrowWriter::try_new(out, schema, Some(parquet_props_from(comp)?))?;

    // the batch's lines back to back, without their line breaks, and where each ends
    let mut chunk = Vec::<u8>::new();
    let mut ends = Vec::<usize>::with_capacity(PARQUET_BATCH_LINES);
    loop {
        let start = chunk.len();
        let eof = input.read_until(b'\n', &mut chunk)? == 0;
        let mut end = chunk.len();
        while end > start && matches!(chunk[end - 1], b'\n' | b'\r') {
            end -= 1;
        }
        chunk.truncate(end);
        if chunk[start..].iter().all(u8::is_ascii_whitespace) {
            chunk.truncate(start);
        } else {
            let line = &chunk[start..];
            if decoder.decode(line)? != line.len() {
                bail!("a line holds more than one JSON value");
            }
            ends.push(chunk.len());
        }
        let full = ends.len() == PARQUET_BATCH_LINES || chunk.len() >= PARQUET_BATCH_BYTES;
        if !ends.is_empty() && (full || eof) {
            let Some(batch) = decoder.flush()? else {
                bail!("{} lines made no rows", ends.len());
            };
            if batch.num_rows() != ends.len() {
                bail!(
                    "{} lines made {} rows: each line must be one JSON object",
                    ends.len(),
                    batch.num_rows()
                );
            }
            writer.write(&batch)?;
            chunk.clear();
            ends.clear();
        }
        if eof {
            break;
        }
    }
    Ok(writer.into_inner()?)
}

fn parquet_props_from(comp: &Compression) -> Result<WriterProperties> {
    let mut b = WriterProperties::builder();
    let pq = match comp {
        Compression::None => PqCompression::UNCOMPRESSED,
        Compression::Gzip { level } => {
            let lvl = GzipLevel::try_new(*level)?;
            PqCompression::GZIP(lvl)
        }
        Compression::Zstd { level } => {
            let lvl = ZstdLevel::try_new(*level)?;
            PqCompression::ZSTD(lvl)
        }
        _ => {
            anyhow::bail!("unsupported compression for parquet: {:?}", comp)
        }
    };
    b = b.set_compression(pq);
    Ok(b.build())
}

fn avro_codec_from(comp: &Compression) -> Codec {
    match *comp {
        Compression::None => Codec::Null,
        Compression::Gzip { .. } => Codec::Null,
        Compression::Deflate { .. } => Codec::Deflate,
        Compression::Zstd { .. } => Codec::Zstandard,
        Compression::Snappy { .. } => Codec::Snappy,
    }
}

pub fn chunk_ndjson(buf: &mut BytesMut, chunks: usize) -> Vec<BytesMut> {
    let mut out = Vec::with_capacity(chunks);
    loop {
        match memchr(b'\n', &buf[..]) {
            Some(nl) => {
                out.push(buf.split_to(nl + 1));
            }
            None => break,
        }
    }
    out
}

pub fn ndjson_chunk_slices(buf: Bytes, max_chunk: usize) -> Vec<Bytes> {
    let mut line_ends: Vec<usize> = memchr_iter(b'\n', &buf).collect();
    if line_ends.last().is_none_or(|&i| i + 1 != buf.len()) {
        line_ends.push(buf.len());
    }

    let mut chunks = Vec::<Bytes>::new();
    let mut chunk_start = 0usize;
    let mut chunk_size = 0usize;

    let mut prev_end = 0usize;
    for &end in &line_ends {
        let has_nl = end != buf.len();
        let line_len = end - prev_end + if has_nl { 1 } else { 0 };

        if line_len > max_chunk && chunk_start == prev_end {
            let hard = end + if has_nl { 1 } else { 0 };
            chunks.push(buf.slice(prev_end..hard));
            chunk_start = hard;
            chunk_size = 0;
        } else {
            if chunk_size > 0 && chunk_size + line_len > max_chunk {
                let hard = prev_end;
                chunks.push(buf.slice(chunk_start..hard));
                chunk_start = prev_end;
                chunk_size = 0;
            }
            chunk_size += line_len;
        }

        prev_end = end + 1;
    }

    if chunk_start < buf.len() {
        chunks.push(buf.slice(chunk_start..buf.len()));
    }
    chunks
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::cast::AsArray;
    use arrow_array::types::{Int64Type, TimestampMillisecondType};
    use arrow_array::RecordBatch;
    use arrow_schema::{DataType, Field, TimeUnit};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    /// ts as a time, `_norm`'s org and actor, and n; nothing else.
    fn schema() -> String {
        let norm = vec![
            Field::new("org", DataType::Utf8, true),
            Field::new("actor", DataType::Utf8, true),
        ];
        let schema = Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, Some("+00:00".into())),
                true,
            ),
            Field::new("_norm", DataType::Struct(norm.into()), true),
            Field::new("n", DataType::Int64, true),
        ]);
        serde_json::to_string(&schema).unwrap()
    }

    const LINES: [&str; 3] = [
        r#"{"ts":"2026-10-20T13:00:00Z","_norm":{"org":"acme","actor":"ann","ip":"192.0.2.1"},"n":1,"eventName":"GetObject"}"#,
        r#"{"ts":"2026-10-20T15:00:00.250+02:00","_norm":{"org":"globex"},"userAgent":"aws-cli/2.17.0"}"#,
        r#"{"_norm":{"actor":"bob"},"n":3,"extra":{"deep":[1,2]}}"#,
    ];

    fn ndjson() -> String {
        // a blank line and a CRLF line break on the way
        format!("{}\n\n{}\r\n{}", LINES[0], LINES[1], LINES[2])
    }

    fn read(bytes: BytesMut) -> (Vec<String>, RecordBatch) {
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes.freeze()).unwrap();
        let leaves = builder
            .metadata()
            .file_metadata()
            .schema_descr()
            .columns()
            .iter()
            .map(|c| c.path().string())
            .collect();
        // one row group, so one batch
        let mut batches = builder.with_batch_size(1 << 20).build().unwrap();
        let batch = batches.next().unwrap().unwrap();
        assert!(batches.next().is_none());
        (leaves, batch)
    }

    fn strings(batch: &RecordBatch, path: &[&str]) -> Vec<Option<String>> {
        let mut col = batch.column_by_name(path[0]).unwrap().clone();
        for name in &path[1..] {
            col = col.as_struct().column_by_name(name).unwrap().clone();
        }
        col.as_string::<i32>()
            .iter()
            .map(|s| s.map(str::to_string))
            .collect()
    }

    #[test]
    fn parquet_has_the_schemas_columns_a_struct_as_leaves() {
        let enc = Encoding::Parquet { schema: schema() };
        let out =
            normalize_from_ndjson(&enc, &Compression::default(), ndjson().as_str().into()).unwrap();
        let (leaves, batch) = read(out);
        assert_eq!(leaves, ["ts", "_norm.org", "_norm.actor", "n"]);
        assert_eq!(batch.num_rows(), 3);
        let ts: Vec<Option<i64>> = batch
            .column_by_name("ts")
            .unwrap()
            .as_primitive::<TimestampMillisecondType>()
            .iter()
            .collect();
        assert_eq!(ts, [Some(1_792_501_200_000), Some(1_792_501_200_250), None]);
        assert_eq!(
            strings(&batch, &["_norm", "actor"]),
            [Some("ann".into()), None, Some("bob".into())]
        );
        assert_eq!(
            strings(&batch, &["_norm", "org"]),
            [Some("acme".into()), Some("globex".into()), None]
        );
        let n: Vec<Option<i64>> = batch
            .column_by_name("n")
            .unwrap()
            .as_primitive::<Int64Type>()
            .iter()
            .collect();
        assert_eq!(n, [Some(1), None, Some(3)]);
    }

    #[test]
    fn parquet_rows_keep_their_order_across_batches() {
        let lines: Vec<String> = (0..PARQUET_BATCH_LINES * 2 + 5)
            .map(|i| format!(r#"{{"n":{i},"_norm":{{"actor":"a{}"}}}}"#, i % 7))
            .collect();
        let out =
            ndjson_to_parquet(lines.join("\n").as_bytes(), &schema(), &Compression::None).unwrap();
        let (_, batch) = read(out);
        assert_eq!(batch.num_rows(), lines.len());
        let n = batch
            .column_by_name("n")
            .unwrap()
            .as_primitive::<Int64Type>();
        let actors = strings(&batch, &["_norm", "actor"]);
        for i in 0..lines.len() {
            assert_eq!(n.value(i), i as i64);
            assert_eq!(actors[i], Some(format!("a{}", i % 7)));
        }
    }

    #[test]
    fn parquet_wants_one_object_a_line() {
        let comp = Compression::None;
        let two = "{\"n\":1}{\"n\":2}\n{\"n\":3}\n";
        assert!(ndjson_to_parquet(two.as_bytes(), &schema(), &comp).is_err());
        let split = "{\"n\":\n1}\n";
        assert!(ndjson_to_parquet(split.as_bytes(), &schema(), &comp).is_err());
    }
}
