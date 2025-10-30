use anyhow::Result;
use apache_avro::Codec;
use arrow_json::ReaderBuilder;
use arrow_schema::Schema;
use bytes::{BufMut, Bytes, BytesMut};
use memchr::{memchr, memchr_iter};
use parquet::basic::{Compression as PqCompression, GzipLevel, ZstdLevel};
use parquet::{arrow::ArrowWriter, file::properties::WriterProperties};
use std::io::Cursor;
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

pub fn ndjson_to_parquet(
    raw: &[u8],
    arrow_schema_json: &str,
    comp: &Compression,
) -> Result<BytesMut> {
    let reader = Cursor::new(raw);
    let arrow_schema: Schema = serde_json::from_str(arrow_schema_json)?;
    let json_reader = ReaderBuilder::new(Arc::new(arrow_schema.clone())).build(reader);

    let props = parquet_props_from(comp)?;
    let mut out = Cursor::new(Vec::<u8>::new());
    let mut writer = ArrowWriter::try_new(&mut out, Arc::new(arrow_schema), Some(props))?;

    for maybe_batch in json_reader? {
        let batch = maybe_batch?;
        writer.write(&batch)?;
    }
    writer.close()?;

    Ok(BytesMut::from(out.into_inner().as_slice()))
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
