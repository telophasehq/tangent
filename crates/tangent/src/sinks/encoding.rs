use anyhow::{bail, Context, Result};
use apache_avro::Codec;
use arrow_array::RecordBatch;
use arrow_json::ReaderBuilder;
use arrow_schema::{Schema, SchemaRef};
use bytes::{BufMut, Bytes, BytesMut};
use memchr::{memchr, memchr_iter};
use parquet::basic::{Compression as PqCompression, GzipLevel, ZstdLevel};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::io::{BufRead, Write};
use std::sync::Arc;
use tangent_shared::sinks::common::{Compression, Encoding};

use crate::SINK_RECORDS_UNFIT_TOTAL;

#[must_use]
pub fn ndjson_ensure_newline(mut raw: BytesMut) -> BytesMut {
    if !raw.ends_with(b"\n") {
        raw.put_u8(b'\n');
    }
    raw
}

pub fn ndjson_to_json_array(raw: &[u8]) -> Result<BytesMut> {
    let out = write_json_array(raw, BytesMut::new().writer())?;
    Ok(out.into_inner())
}

/// [`ndjson_to_json_array`] from a reader to a writer, a line in memory at
/// a time.
pub fn write_json_array<R: BufRead, W: Write>(input: R, mut out: W) -> Result<W> {
    out.write_all(b"[")?;
    let mut first = true;
    for line in input.split(b'\n') {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        let v: serde_json::Value = serde_json::from_slice(&line)?;
        if !first {
            out.write_all(b",")?;
        }
        serde_json::to_writer(&mut out, &v)?;
        first = false;
    }
    out.write_all(b"]")?;
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

pub fn ndjson_to_avro(raw: &[u8], avro_schema_json: &str, comp: &Compression) -> Result<BytesMut> {
    let out = write_avro(raw, BytesMut::new().writer(), avro_schema_json, comp)?;
    Ok(out.into_inner())
}

/// [`ndjson_to_avro`] from a reader to a writer, a line in memory at a time
/// (and a block, in the Avro writer).
pub fn write_avro<R: BufRead, W: Write>(
    input: R,
    out: W,
    avro_schema_json: &str,
    comp: &Compression,
) -> Result<W> {
    let schema = apache_avro::Schema::parse_str(avro_schema_json)?;
    let mut writer = apache_avro::Writer::with_codec(&schema, out, avro_codec_from(comp));
    for line in input.split(b'\n') {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        let value: serde_json::Value = serde_json::from_slice(&line)?;
        writer.append_ser(value)?;
    }
    Ok(writer.into_inner()?)
}

/// Lines in one Arrow batch, and the bytes they may come to: past either,
/// the batch is written and the next begun.
const PARQUET_BATCH_LINES: usize = 8192;
const PARQUET_BATCH_BYTES: usize = 64 << 20;

/// NDJSON as one Parquet file: the columns the Arrow schema names, read out
/// of each line (a field it does not name is skipped; a nested object it
/// names is a struct, its fields leaf columns of their own). A line the
/// schema cannot read is dropped as [`write_parquet`] says.
pub fn ndjson_to_parquet(
    raw: &[u8],
    arrow_schema_json: &str,
    comp: &Compression,
) -> Result<BytesMut> {
    let (out, _) = write_parquet(raw, Vec::new(), arrow_schema_json, comp)?;
    Ok(BytesMut::from(out.as_slice()))
}

/// Lines the schema could not read, dropped.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Unfit {
    pub dropped: usize,
}

/// [`ndjson_to_parquet`] from a reader to a writer, a batch of lines in
/// memory at a time. A line the schema cannot read (not one JSON value, a
/// type the schema does not have for a field, a time arrow cannot parse)
/// does not fail the file: it is dropped, counted in
/// `tangent_sink_records_unfit_total` and returned.
pub fn write_parquet<R: BufRead, W: Write + Send>(
    mut input: R,
    out: W,
    arrow_schema_json: &str,
    comp: &Compression,
) -> Result<(W, Unfit)> {
    let (schema, props) = parquet_setup(arrow_schema_json, comp)?;
    let mut writer = ArrowWriter::try_new(out, Arc::clone(&schema), Some(props))?;
    let mut unfit = Unfit::default();

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
            ends.push(chunk.len());
        }
        let full = ends.len() == PARQUET_BATCH_LINES || chunk.len() >= PARQUET_BATCH_BYTES;
        if !ends.is_empty() && (full || eof) {
            let mut from = 0;
            let lines: Vec<&[u8]> = ends
                .iter()
                .map(|&to| {
                    let line = &chunk[from..to];
                    from = to;
                    line
                })
                .collect();
            if let Ok(batch) = rows(&schema, &lines) {
                writer.write(&batch)?;
            } else {
                // a line in the batch the schema cannot read: line by line
                for line in &lines {
                    if let Ok(batch) = rows(&schema, &[line]) {
                        writer.write(&batch)?;
                    } else {
                        unfit.dropped += 1;
                    }
                }
            }
            chunk.clear();
            ends.clear();
        }
        if eof {
            break;
        }
    }
    if unfit != Unfit::default() {
        SINK_RECORDS_UNFIT_TOTAL
            .with_label_values(&["dropped"])
            .inc_by(unfit.dropped as u64);
        tracing::warn!(
            dropped = unfit.dropped,
            "parquet: lines the schema could not read"
        );
    }
    Ok((writer.into_inner()?, unfit))
}

/// The lines' rows, one a line, or an error if a line is not one JSON value
/// the schema can read.
fn rows(schema: &SchemaRef, lines: &[&[u8]]) -> Result<RecordBatch> {
    // room for a row more than the lines: a line of two values shows as one
    let mut decoder = ReaderBuilder::new(Arc::clone(schema))
        .with_batch_size(lines.len() + 1)
        .build_decoder()?;
    for line in lines {
        if decoder.decode(line)? != line.len() {
            bail!("a line holds more than one JSON value");
        }
    }
    let Some(batch) = decoder.flush()? else {
        bail!("{} lines made no rows", lines.len());
    };
    if batch.num_rows() != lines.len() {
        bail!(
            "{} lines made {} rows: each line must be one JSON value",
            lines.len(),
            batch.num_rows()
        );
    }
    Ok(batch)
}

/// The schema read out of each line and the writer's properties: what a
/// file's encoding fails on before its first line, if it fails at all.
fn parquet_setup(
    arrow_schema_json: &str,
    comp: &Compression,
) -> Result<(SchemaRef, WriterProperties)> {
    let schema: Schema = serde_json::from_str(arrow_schema_json)
        .context("the parquet schema is not an Arrow schema")?;
    Ok((Arc::new(schema), parquet_props_from(comp)?))
}

/// What a sink's encoding would fail on at every file, found when the sink
/// is made rather than after writes are acked into its WAL: a Parquet
/// schema that does not parse, or that arrow-json or the writer cannot take,
/// a compression Parquet does not have; an Avro schema that does not parse.
pub fn check(enc: &Encoding, comp: &Compression) -> Result<()> {
    match enc {
        Encoding::Parquet { schema } => {
            let (schema, props) = parquet_setup(schema, comp)?;
            ReaderBuilder::new(Arc::clone(&schema)).build_decoder()?;
            ArrowWriter::try_new(std::io::sink(), schema, Some(props))?;
        }
        Encoding::Avro { schema } => {
            apache_avro::Schema::parse_str(schema)?;
        }
        Encoding::NDJSON | Encoding::JSON => {}
    }
    Ok(())
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
    fn a_line_the_schema_cannot_read_is_dropped() {
        let good = [r#"{"n":1,"_norm":{"actor":"ann"}}"#, r#"{"n":5}"#];
        let unread = [
            r#"{"n":"two","_norm":{"actor":"bob"}}"#, // a string for an Int64
            r#"{"ts":"yesterday","n":3}"#,            // a time arrow cannot parse
            r#"[1,2]"#,                               // JSON, not an object
        ];
        let not_json = ["{\"n\":", "1}", "{\"n\":4}{\"n\":4}", "not json"];
        let ndjson = [good[0], unread[0], not_json[0], not_json[1], unread[1]]
            .iter()
            .chain(&[not_json[2], unread[2], not_json[3], good[1]])
            .map(|l| format!("{l}\n"))
            .collect::<String>();

        let (out, unfit) =
            write_parquet(ndjson.as_bytes(), Vec::new(), &schema(), &Compression::None).unwrap();
        assert_eq!(unfit, Unfit { dropped: 7 });
        let (_, batch) = read(BytesMut::from(out.as_slice()));
        let n: Vec<Option<i64>> = batch
            .column_by_name("n")
            .unwrap()
            .as_primitive::<Int64Type>()
            .iter()
            .collect();
        assert_eq!(n, [Some(1), Some(5)], "the good lines, in order");
    }

    #[test]
    fn json_and_avro_stream_what_they_made_whole() {
        let ndjson = "{\"n\":1,\"m\":\"x\"}\n\n{\"n\":2,\"m\":\"y\"}\n";
        let json = ndjson_to_json_array(ndjson.as_bytes()).unwrap();
        assert_eq!(&json[..], br#"[{"n":1,"m":"x"},{"n":2,"m":"y"}]"#);

        // append_ser makes a JSON object an Avro map, so a map schema
        let schema = r#"{"type":"map","values":"string"}"#;
        let ndjson = "{\"n\":\"1\"}\n\n{\"n\":\"2\",\"m\":\"y\"}\n";
        let avro =
            ndjson_to_avro(ndjson.as_bytes(), schema, &Compression::Zstd { level: 3 }).unwrap();
        let read: Vec<apache_avro::types::Value> = apache_avro::Reader::new(&avro[..])
            .unwrap()
            .map(Result::unwrap)
            .collect();
        assert_eq!(read.len(), 2);
    }

    #[test]
    fn check_finds_what_every_file_would_fail_on() {
        let zstd = Compression::Zstd { level: 3 };
        let parquet = |schema: &str| Encoding::Parquet {
            schema: schema.to_string(),
        };
        assert!(check(&parquet(&schema()), &zstd).is_ok());
        assert!(check(&Encoding::NDJSON, &zstd).is_ok());
        assert!(check(&parquet("{\"fields\":"), &zstd).is_err());
        let snappy = Compression::Snappy { level: 3 };
        assert!(check(&parquet(&schema()), &snappy).is_err());
        let avro = |schema: &str| Encoding::Avro {
            schema: schema.to_string(),
        };
        assert!(check(&avro(r#"{"type":"record","name":"r","fields":[]}"#), &zstd).is_ok());
        assert!(check(&avro("{\"type\":"), &zstd).is_err());
    }
}
