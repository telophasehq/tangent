use std::io::{self, Write};

use anyhow::Result;
use bytes::{BufMut, Bytes, BytesMut};
use memchr::memchr_iter;
use serde::Deserialize;
use tangent_shared::sources::common::{DecodeCompression, DecodeFormat};

pub fn decompress_bytes(comp: &DecodeCompression, data: BytesMut) -> Result<BytesMut> {
    Ok(match comp {
        DecodeCompression::None | DecodeCompression::Auto => data,
        DecodeCompression::Gzip => {
            use flate2::read::GzDecoder;
            let mut dec = GzDecoder::new(&data[..]);
            let mut out = BytesMut::new();
            let mut w = BytesMutWriter(&mut out);
            std::io::copy(&mut dec, &mut w)?;
            out
        }
        DecodeCompression::Zstd => {
            let mut dec = zstd::stream::read::Decoder::new(&data[..])?;
            let mut out = BytesMut::new();
            let mut w = BytesMutWriter(&mut out);
            std::io::copy(&mut dec, &mut w)?;
            out
        }
    })
}

pub fn decompress_vec(comp: &DecodeCompression, data: &[u8]) -> Result<BytesMut> {
    Ok(match comp {
        DecodeCompression::None | DecodeCompression::Auto => BytesMut::from(data),
        DecodeCompression::Gzip => {
            use flate2::read::GzDecoder;
            let mut dec = GzDecoder::new(&data[..]);
            let mut out = BytesMut::new();
            let mut w = BytesMutWriter(&mut out);
            std::io::copy(&mut dec, &mut w)?;
            out
        }
        DecodeCompression::Zstd => {
            let mut dec = zstd::stream::read::Decoder::new(&data[..])?;
            let mut out = BytesMut::new();
            let mut w = BytesMutWriter(&mut out);
            std::io::copy(&mut dec, &mut w)?;
            out
        }
    })
}

struct BytesMutWriter<'a>(&'a mut BytesMut);

impl<'a> Write for BytesMutWriter<'a> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[must_use]
pub fn json_to_ndjson(v: &serde_json::Value) -> BytesMut {
    match v {
        serde_json::Value::Array(a) => {
            let mut buf = BytesMut::new();
            for el in a {
                serde_json::to_writer((&mut buf).writer(), el).ok();
                buf.put_u8(b'\n');
            }
            buf
        }
        _ => {
            let mut buf = BytesMut::new();
            serde_json::to_writer((&mut buf).writer(), v).ok();
            buf.put_u8(b'\n');
            buf
        }
    }
}

pub fn msgpack_to_ndjson(data: &[u8]) -> Result<BytesMut> {
    use rmp_serde::Deserializer;
    let mut de = Deserializer::from_read_ref(data);
    let val = serde_json::Value::deserialize(&mut de)?;
    Ok(json_to_ndjson(&val))
}

pub fn normalize_to_ndjson(fmt: &DecodeFormat, mut raw: BytesMut) -> BytesMut {
    match fmt {
        DecodeFormat::Ndjson | DecodeFormat::Text => {
            if !raw.ends_with(b"\n") {
                raw.put_u8(b'\n');
            }
            raw
        }
        DecodeFormat::Json | DecodeFormat::JsonArray => {
            match serde_json::from_slice::<serde_json::Value>(&raw) {
                Ok(v) => json_to_ndjson(&v),
                Err(e) => {
                    tracing::warn!(error=?e, "failed JSON parse; fallback to text");
                    if !raw.ends_with(b"\n") {
                        raw.put_u8(b'\n');
                    }
                    raw
                }
            }
        }
        DecodeFormat::Msgpack => match msgpack_to_ndjson(&raw) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error=?e, "failed MsgPack decode; fallback to text");
                if !raw.ends_with(b"\n") {
                    raw.put_u8(b'\n');
                }
                raw
            }
        },
        DecodeFormat::Auto => unreachable!(),
    }
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
