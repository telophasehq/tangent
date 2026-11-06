use anyhow::{bail, Result};
use apache_avro::Codec;
use arrow_array::{builder::StringBuilder, new_null_array, ArrayRef, RecordBatch};
use arrow_json::ReaderBuilder;
use arrow_schema::{DataType, Field, FieldRef, Schema, SchemaRef, TimeUnit};
use bytes::{BufMut, Bytes, BytesMut};
use memchr::{memchr, memchr_iter};
use parquet::basic::{Compression as PqCompression, GzipLevel, ZstdLevel};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::io::{BufRead, Write};
use std::sync::Arc;
use tangent_shared::sinks::common::{Column, ColumnType, Columns, Compression, Encoding};

use crate::SINK_RECORDS_UNFIT_TOTAL;

#[must_use]
pub fn ndjson_ensure_newline(mut raw: BytesMut) -> BytesMut {
    if !raw.ends_with(b"\n") {
        raw.put_u8(b'\n');
    }
    raw
}

pub fn ndjson_to_json_array(raw: &[u8]) -> Result<BytesMut> {
    let (out, _) = write_json_array(raw, BytesMut::new().writer())?;
    Ok(out.into_inner())
}

/// [`ndjson_to_json_array`] from a reader to a writer, a line in memory at
/// a time. A line that is not one JSON value does not fail the file: it is
/// dropped, counted in `tangent_sink_records_unfit_total{fate="dropped"}`,
/// and the lines dropped are returned.
pub fn write_json_array<R: BufRead, W: Write>(input: R, mut out: W) -> Result<(W, usize)> {
    out.write_all(b"[")?;
    let mut first = true;
    let mut dropped = 0;
    for line in input.split(b'\n') {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        let Ok(v) = serde_json::from_slice::<serde_json::Value>(&line) else {
            dropped += 1;
            continue;
        };
        if !first {
            out.write_all(b",")?;
        }
        serde_json::to_writer(&mut out, &v)?;
        first = false;
    }
    out.write_all(b"]")?;
    if dropped > 0 {
        SINK_RECORDS_UNFIT_TOTAL
            .with_label_values(&["dropped"])
            .inc_by(dropped as u64);
        tracing::warn!(dropped, "json: lines that are not JSON");
    }
    Ok((out, dropped))
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
        Encoding::Parquet {
            schema,
            record_column,
        } => ndjson_to_parquet(&raw, schema, record_column.as_deref(), comp),
    }
}

pub fn ndjson_to_avro(raw: &[u8], avro_schema_json: &str, comp: &Compression) -> Result<BytesMut> {
    let (out, _) = write_avro(raw, BytesMut::new().writer(), avro_schema_json, comp)?;
    Ok(out.into_inner())
}

/// [`ndjson_to_avro`] from a reader to a writer, a line in memory at a time
/// (and a block, in the Avro writer). Each line is resolved against the
/// schema, an object to a record and a value to its union branch, before it
/// is written. A line the schema does not take (not JSON, a value Avro has
/// no type for, a value not of the schema) does not fail the file: it is
/// dropped, counted in `tangent_sink_records_unfit_total{fate="dropped"}`,
/// and the lines dropped are returned.
pub fn write_avro<R: BufRead, W: Write>(
    input: R,
    out: W,
    avro_schema_json: &str,
    comp: &Compression,
) -> Result<(W, usize)> {
    let schema = apache_avro::Schema::parse_str(avro_schema_json)?;
    let mut writer = apache_avro::Writer::with_codec(&schema, out, avro_codec_from(comp));
    let mut dropped = 0;
    for line in input.split(b'\n') {
        let line = line?;
        if line.is_empty() {
            continue;
        }
        // the line's errors come before the writer has any of it: the
        // writer's are the file's, and fail it
        let Some(value) = serde_json::from_slice::<serde_json::Value>(&line)
            .ok()
            .and_then(|v| apache_avro::to_value(v).ok())
            .and_then(|v| v.resolve(&schema).ok())
        else {
            dropped += 1;
            continue;
        };
        writer.append(value)?;
    }
    if dropped > 0 {
        SINK_RECORDS_UNFIT_TOTAL
            .with_label_values(&["dropped"])
            .inc_by(dropped as u64);
        tracing::warn!(dropped, "avro: lines the schema does not take");
    }
    Ok((writer.into_inner()?, dropped))
}

/// Lines in one Arrow batch, and the bytes they may come to: past either,
/// the batch is written and the next begun.
const PARQUET_BATCH_LINES: usize = 8192;
const PARQUET_BATCH_BYTES: usize = 64 << 20;

/// NDJSON as one Parquet file: the columns the schema names, read out
/// of each line (a field it does not name is skipped; a nested object it
/// names is a struct, its fields leaf columns of their own), and, with
/// `record_column`, each line, in a Utf8 column of that name. A line the
/// schema cannot read is kept or dropped as [`write_parquet`] says.
pub fn ndjson_to_parquet(
    raw: &[u8],
    columns: &Columns,
    record_column: Option<&str>,
    comp: &Compression,
) -> Result<BytesMut> {
    let (out, _) = write_parquet(raw, Vec::new(), columns, record_column, comp)?;
    Ok(BytesMut::from(out.as_slice()))
}

/// Lines the schema could not read: kept whole in the record column with
/// the typed columns null, or dropped.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Unfit {
    pub kept: usize,
    pub dropped: usize,
}

/// [`ndjson_to_parquet`] from a reader to a writer, a batch of lines in
/// memory at a time. The record column holds each line without its line
/// break (a trailing CR or LF goes), invalid UTF-8 replaced. A line the
/// schema cannot read (not one JSON value, a type the schema does not have
/// for a field, a time arrow cannot parse) does not fail the file: with a
/// record column it is kept there, its typed columns null, if it is JSON at
/// all and every typed column may be null; otherwise it is dropped. Both are
/// counted in `tangent_sink_records_unfit_total` and returned.
pub fn write_parquet<R: BufRead, W: Write + Send>(
    mut input: R,
    out: W,
    columns: &Columns,
    record_column: Option<&str>,
    comp: &Compression,
) -> Result<(W, Unfit)> {
    let (schema, written, props) = parquet_setup(columns, record_column, comp)?;
    let mut writer = ArrowWriter::try_new(out, Arc::clone(&written), Some(props))?;
    let with_records = |batch: RecordBatch, lines: &[&[u8]]| -> Result<RecordBatch> {
        if record_column.is_none() {
            return Ok(batch);
        }
        let mut columns = batch.columns().to_vec();
        columns.push(record_array(lines));
        Ok(RecordBatch::try_new(Arc::clone(&written), columns)?)
    };
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
                writer.write(&with_records(batch, &lines)?)?;
            } else {
                // a line in the batch the schema cannot read: line by line
                for line in &lines {
                    if let Ok(batch) = rows(&schema, &[line]) {
                        writer.write(&with_records(batch, &[line])?)?;
                    } else if let Some(row) = unfit_row(&schema, &written, record_column, line) {
                        writer.write(&row)?;
                        unfit.kept += 1;
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
            .with_label_values(&["kept"])
            .inc_by(unfit.kept as u64);
        SINK_RECORDS_UNFIT_TOTAL
            .with_label_values(&["dropped"])
            .inc_by(unfit.dropped as u64);
        tracing::warn!(
            kept = unfit.kept,
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

/// The lines as the record column: as SQLite's sink keeps them, invalid
/// UTF-8 replaced.
fn record_array(lines: &[&[u8]]) -> ArrayRef {
    let mut records =
        StringBuilder::with_capacity(lines.len(), lines.iter().map(|l| l.len()).sum());
    for line in lines {
        records.append_value(String::from_utf8_lossy(line));
    }
    Arc::new(records.finish())
}

/// A line the schema cannot read, as a row: in the record column, the
/// typed columns null. None without a record column, for a line that is not
/// JSON, or with a typed column that may not be null.
fn unfit_row(
    schema: &SchemaRef,
    written: &SchemaRef,
    record_column: Option<&str>,
    line: &[u8],
) -> Option<RecordBatch> {
    record_column?;
    serde_json::from_slice::<serde::de::IgnoredAny>(line).ok()?;
    let mut columns: Vec<ArrayRef> = schema
        .fields()
        .iter()
        .map(|f| new_null_array(f.data_type(), 1))
        .collect();
    columns.push(record_array(&[line]));
    RecordBatch::try_new(Arc::clone(written), columns).ok()
}

/// The schema read out of each line, the schema written (with the record
/// column), and the writer's properties: what a file's encoding fails on
/// before its first line, if it fails at all.
fn parquet_setup(
    columns: &Columns,
    record_column: Option<&str>,
    comp: &Compression,
) -> Result<(SchemaRef, SchemaRef, WriterProperties)> {
    let schema = Schema::new(fields(columns));
    let written = match record_column {
        None => Arc::new(schema.clone()),
        Some(column) => {
            if schema.field_with_name(column).is_ok() {
                bail!("record_column {column:?} is a column of the schema already");
            }
            let mut fields: Vec<FieldRef> = schema.fields().iter().cloned().collect();
            fields.push(Arc::new(Field::new(column, DataType::Utf8, false)));
            Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
        }
    };
    Ok((Arc::new(schema), written, parquet_props_from(comp)?))
}

/// The Arrow fields of a schema's columns, each nullable.
fn fields(columns: &Columns) -> Vec<Field> {
    columns
        .0
        .iter()
        .map(|(name, col)| {
            let ty = match col {
                Column::Type(ColumnType::String) => DataType::Utf8,
                Column::Type(ColumnType::Int) => DataType::Int64,
                Column::Type(ColumnType::Float) => DataType::Float64,
                Column::Type(ColumnType::Bool) => DataType::Boolean,
                Column::Type(ColumnType::Timestamp) => {
                    DataType::Timestamp(TimeUnit::Millisecond, Some("+00:00".into()))
                }
                Column::Struct(inner) => DataType::Struct(fields(inner).into()),
            };
            Field::new(name, ty, true)
        })
        .collect()
}

/// What a sink's encoding would fail on at every file, found when the sink
/// is made rather than after writes are acked into its WAL: a Parquet
/// schema arrow-json or the writer cannot take (a struct with no columns),
/// a record column the schema already has, a compression Parquet does not
/// have; an Avro schema that does not parse.
pub fn check(enc: &Encoding, comp: &Compression) -> Result<()> {
    match enc {
        Encoding::Parquet {
            schema,
            record_column,
        } => {
            let (schema, written, props) = parquet_setup(schema, record_column.as_deref(), comp)?;
            ReaderBuilder::new(schema).build_decoder()?;
            ArrowWriter::try_new(std::io::sink(), written, Some(props))?;
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
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    /// ts as a time, `_norm`'s org and actor, and n; nothing else.
    fn schema() -> Columns {
        serde_yaml::from_str("{ ts: timestamp, _norm: { org: string, actor: string }, n: int }")
            .unwrap()
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
        let enc = Encoding::Parquet {
            schema: schema(),
            record_column: None,
        };
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
    fn parquet_keeps_each_line_whole_in_the_record_column() {
        let out = ndjson_to_parquet(
            ndjson().as_bytes(),
            &schema(),
            Some("record"),
            &Compression::Zstd { level: 3 },
        )
        .unwrap();
        let (leaves, batch) = read(out);
        assert_eq!(leaves, ["ts", "_norm.org", "_norm.actor", "n", "record"]);
        let records: Vec<Option<String>> = LINES.iter().map(|l| Some(l.to_string())).collect();
        assert_eq!(strings(&batch, &["record"]), records);
        assert_eq!(
            strings(&batch, &["_norm", "actor"]),
            [Some("ann".into()), None, Some("bob".into())]
        );
    }

    #[test]
    fn parquet_record_column_stays_paired_across_batches() {
        let lines: Vec<String> = (0..PARQUET_BATCH_LINES * 2 + 5)
            .map(|i| format!(r#"{{"n":{i},"_norm":{{"actor":"a{}"}}}}"#, i % 7))
            .collect();
        let out = ndjson_to_parquet(
            lines.join("\n").as_bytes(),
            &schema(),
            Some("record"),
            &Compression::None,
        )
        .unwrap();
        let (_, batch) = read(out);
        assert_eq!(batch.num_rows(), lines.len());
        let n = batch
            .column_by_name("n")
            .unwrap()
            .as_primitive::<Int64Type>();
        for (i, record) in strings(&batch, &["record"]).into_iter().enumerate() {
            assert_eq!(record.as_deref(), Some(lines[i].as_str()));
            assert_eq!(n.value(i), i as i64);
        }
    }

    #[test]
    fn a_line_the_schema_cannot_read_is_kept_whole_or_dropped() {
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
        let comp = Compression::None;

        let (out, unfit) = write_parquet(
            ndjson.as_bytes(),
            Vec::new(),
            &schema(),
            Some("record"),
            &comp,
        )
        .unwrap();
        assert_eq!(
            unfit,
            Unfit {
                kept: 3,
                dropped: 4
            }
        );
        let (_, batch) = read(BytesMut::from(out.as_slice()));
        let want: Vec<Option<String>> = [good[0], unread[0], unread[1], unread[2], good[1]]
            .iter()
            .map(|l| Some(l.to_string()))
            .collect();
        assert_eq!(strings(&batch, &["record"]), want, "in order");
        let n: Vec<Option<i64>> = batch
            .column_by_name("n")
            .unwrap()
            .as_primitive::<Int64Type>()
            .iter()
            .collect();
        assert_eq!(
            n,
            [Some(1), None, None, None, Some(5)],
            "unread: typed null"
        );
        assert_eq!(
            strings(&batch, &["_norm", "actor"]),
            [Some("ann".into()), None, None, None, None]
        );

        // without a record column there is nowhere to keep them
        let (out, unfit) =
            write_parquet(ndjson.as_bytes(), Vec::new(), &schema(), None, &comp).unwrap();
        assert_eq!(
            unfit,
            Unfit {
                kept: 0,
                dropped: 7
            }
        );
        let (_, batch) = read(BytesMut::from(out.as_slice()));
        assert_eq!(batch.num_rows(), 2);

        let taken = ndjson_to_parquet(b"{\"n\":1}\n", &schema(), Some("n"), &comp);
        assert!(taken
            .unwrap_err()
            .to_string()
            .contains("column of the schema"));
    }

    #[test]
    fn json_and_avro_stream_what_they_made_whole() {
        let ndjson = "{\"n\":1,\"m\":\"x\"}\n\n{\"n\":2,\"m\":\"y\"}\n";
        let json = ndjson_to_json_array(ndjson.as_bytes()).unwrap();
        assert_eq!(&json[..], br#"[{"n":1,"m":"x"},{"n":2,"m":"y"}]"#);

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
    fn json_drops_a_line_that_is_not_one_json_value() {
        let ndjson = "{\"n\":1}\nnot json\n{\"n\":\n{\"n\":4}{\"n\":4}\n\n{\"n\":2}\n";
        let (out, dropped) = write_json_array(ndjson.as_bytes(), Vec::new()).unwrap();
        assert_eq!(out, br#"[{"n":1},{"n":2}]"#);
        assert_eq!(dropped, 3, "a blank line is no line");
    }

    #[test]
    fn avro_drops_a_line_its_schema_does_not_take() {
        let schema = r#"{"type":"map","values":"string"}"#;
        let ndjson = [
            r#"{"n":"1"}"#,
            "not json",
            r#"{"n":2}"#,                    // not of the schema
            r#"{"n":18446744073709551615}"#, // a u64 Avro has no type for
            "",
            r#"{"n":"3","m":"y"}"#,
        ]
        .map(|l| format!("{l}\n"))
        .concat();
        let comp = Compression::Zstd { level: 3 };
        let (out, dropped) = write_avro(ndjson.as_bytes(), Vec::new(), schema, &comp).unwrap();
        assert_eq!(dropped, 3);
        let read: Vec<std::collections::BTreeMap<String, String>> =
            apache_avro::Reader::new(&out[..])
                .unwrap()
                .map(|v| apache_avro::from_value(&v.unwrap()).unwrap())
                .collect();
        assert_eq!(read.len(), 2);
        assert_eq!(read[0]["n"], "1");
        assert_eq!((read[1]["n"].as_str(), read[1]["m"].as_str()), ("3", "y"));
    }

    #[test]
    fn avro_takes_an_object_as_a_record_and_a_value_as_its_union_branch() {
        use apache_avro::types::Value as A;
        let record = r#"{"type":"record","name":"r","fields":[
            {"name":"n","type":"string"},
            {"name":"m","type":["null","long"],"default":null}]}"#;
        let ndjson = [
            r#"{"n":"good"}"#,
            r#"{"n":2}"#, // not of the schema
            r#"{"n":"x","m":3,"extra":true}"#,
        ]
        .map(|l| format!("{l}\n"))
        .concat();
        let comp = Compression::None;
        let (out, dropped) = write_avro(ndjson.as_bytes(), Vec::new(), record, &comp).unwrap();
        assert_eq!(dropped, 1);
        let read: Vec<A> = apache_avro::Reader::new(&out[..])
            .unwrap()
            .map(Result::unwrap)
            .collect();
        let rec =
            |n: &str, m: A| A::Record(vec![("n".into(), A::String(n.into())), ("m".into(), m)]);
        assert_eq!(
            read,
            [
                rec("good", A::Union(0, Box::new(A::Null))),
                rec("x", A::Union(1, Box::new(A::Long(3))))
            ]
        );

        let union = r#"{"type":"map","values":["null","string"]}"#;
        let ndjson = "{\"a\":\"x\",\"b\":null}\n";
        let (out, dropped) = write_avro(ndjson.as_bytes(), Vec::new(), union, &comp).unwrap();
        assert_eq!(dropped, 0);
        assert_eq!(apache_avro::Reader::new(&out[..]).unwrap().count(), 1);
    }

    #[test]
    fn check_finds_what_every_file_would_fail_on() {
        let zstd = Compression::Zstd { level: 3 };
        let parquet = |schema: Columns, record: Option<&str>| Encoding::Parquet {
            schema,
            record_column: record.map(str::to_string),
        };
        assert!(check(&parquet(schema(), Some("record")), &zstd).is_ok());
        assert!(check(&Encoding::NDJSON, &zstd).is_ok());
        let empty = serde_yaml::from_str("{ _norm: {} }").unwrap();
        assert!(check(&parquet(empty, None), &zstd).is_err());
        assert!(check(&parquet(schema(), Some("n")), &zstd).is_err());
        let snappy = Compression::Snappy { level: 3 };
        assert!(check(&parquet(schema(), None), &snappy).is_err());
        let avro = |schema: &str| Encoding::Avro {
            schema: schema.to_string(),
        };
        assert!(check(&avro(r#"{"type":"record","name":"r","fields":[]}"#), &zstd).is_ok());
        assert!(check(&avro("{\"type\":"), &zstd).is_err());
    }
}
