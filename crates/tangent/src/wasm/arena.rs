use anyhow::Result;
use base64::engine::general_purpose::STANDARD as B64;
use base64::Engine;
use bytes::{BufMut, BytesMut};
use serde::ser::{Serialize, SerializeMap, SerializeSeq, Serializer};
use std::io;

// Adjust paths to your generated bindings:
use crate::wasm::host::exports::tangent::logs::mapper::{Batchout, Field, Idx, Node};

struct NodeView<'a> {
    arena: &'a [Node],
    strings: &'a [String],
    idx: Idx,
}

impl<'a> Serialize for NodeView<'a> {
    fn serialize<S: Serializer>(&self, ser: S) -> Result<S::Ok, S::Error> {
        match &self.arena[self.idx as usize] {
            Node::Null => ser.serialize_none(), // JSON null
            Node::Boolean(b) => ser.serialize_bool(*b),
            Node::Integer(n) => ser.serialize_i64(*n),
            Node::Float(f) => {
                if !f.is_finite() {
                    return ser.serialize_none();
                } // NaN/Inf → null
                ser.serialize_f64(*f)
            }
            Node::String(s) => ser.serialize_str(s),
            Node::Bytes(b) => {
                // base64 without creating a String
                let mut tmp = vec![0u8; b.len()];
                let n = B64
                    .encode_slice(b, &mut tmp)
                    .map_err(serde::ser::Error::custom)?;
                let s = std::str::from_utf8(&tmp[..n]).map_err(serde::ser::Error::custom)?;
                ser.serialize_str(s)
            }
            Node::Array(children) => {
                let mut seq = ser.serialize_seq(Some(children.len()))?;
                for &child in children {
                    seq.serialize_element(&NodeView {
                        arena: self.arena,
                        strings: self.strings,
                        idx: child,
                    })?;
                }
                seq.end()
            }
            Node::Object(fields) => {
                let mut map = ser.serialize_map(Some(fields.len()))?;
                for Field { keyid, val } in fields {
                    // key lookup via string table
                    let key = self
                        .strings
                        .get(*keyid as usize)
                        .ok_or_else(|| serde::ser::Error::custom("key_id OOB"))?;
                    map.serialize_entry(
                        key,
                        &NodeView {
                            arena: self.arena,
                            strings: self.strings,
                            idx: *val,
                        },
                    )?;
                }
                map.end()
            }
        }
    }
}

/// Write the batch as NDJSON (one JSON per root, newline-terminated).
pub fn write_json_from_batch(bo: &Batchout) -> Result<Vec<BytesMut>> {
    let arena = &bo.arena;
    let strings = &bo.strings;

    let out = Vec::<BytesMut>::with_capacity(bo.roots.len());
    for &root in &bo.roots {
        let buf = BytesMut::new();
        let mut w = buf.writer();
        let view = NodeView {
            arena,
            strings,
            idx: root,
        };
        serde_json::to_writer(&mut w, &view)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
    }
    Ok(out)
}
