use anyhow::Result;
use bytes::{BufMut, BytesMut};
use serde::Deserialize;
use tangent_shared::sources::common::{DecodeCompression, DecodeFormat};

pub fn decompress_bytes(comp: DecodeCompression, data: &[u8]) -> Result<Vec<u8>> {
    Ok(match comp {
        DecodeCompression::None | DecodeCompression::Auto => data.to_vec(),
        DecodeCompression::Gzip => {
            use flate2::read::GzDecoder;
            let mut dec = GzDecoder::new(data);
            let mut out = Vec::new();
            std::io::copy(&mut dec, &mut out)?;
            out
        }
        DecodeCompression::Zstd => {
            let mut dec = zstd::stream::read::Decoder::new(data)?;
            let mut out = Vec::new();
            std::io::copy(&mut dec, &mut out)?;
            out
        }
    })
}

pub fn json_to_ndjson(v: &serde_json::Value) -> Vec<u8> {
    match v {
        serde_json::Value::Array(a) => {
            let mut buf = BytesMut::new();
            for el in a {
                serde_json::to_writer((&mut buf).writer(), el).ok();
                buf.put_u8(b'\n');
            }
            buf.to_vec()
        }
        _ => {
            let mut buf = BytesMut::new();
            serde_json::to_writer((&mut buf).writer(), v).ok();
            buf.put_u8(b'\n');
            buf.to_vec()
        }
    }
}

pub fn msgpack_to_ndjson(data: &[u8]) -> Result<Vec<u8>> {
    use rmp_serde::Deserializer;
    let mut de = Deserializer::from_read_ref(data);
    let val = serde_json::Value::deserialize(&mut de)?;
    Ok(json_to_ndjson(&val))
}

pub fn normalize_to_ndjson(fmt: DecodeFormat, raw: &[u8]) -> Vec<u8> {
    match fmt {
        DecodeFormat::Ndjson | DecodeFormat::Text => {
            let mut b = raw.to_vec();
            if b.last().copied() != Some(b'\n') {
                b.push(b'\n');
            }
            b
        }
        DecodeFormat::Json | DecodeFormat::JsonArray => {
            match serde_json::from_slice::<serde_json::Value>(raw) {
                Ok(v) => json_to_ndjson(&v),
                Err(e) => {
                    tracing::warn!(error=?e, "failed JSON parse; fallback to text");
                    let mut b = raw.to_vec();
                    if b.last().copied() != Some(b'\n') {
                        b.push(b'\n');
                    }
                    b
                }
            }
        }
        DecodeFormat::Msgpack => match msgpack_to_ndjson(raw) {
            Ok(v) => v,
            Err(e) => {
                tracing::warn!(error=?e, "failed MsgPack decode; fallback to text");
                let mut b = raw.to_vec();
                if b.last().copied() != Some(b'\n') {
                    b.push(b'\n');
                }
                b
            }
        },
        DecodeFormat::Auto => unreachable!(),
    }
}
