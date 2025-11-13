use std::sync::Arc;

use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use base64::Engine;
use bytes::{Bytes, BytesMut};
use serde_json::{Map as JsonMap, Number as JsonNumber, Value as JsonValue};
use simd_json::base::ValueAsScalar;
use simd_json::derived::{TypedArrayValue, TypedScalarValue};
use simd_json::prelude::{ValueAsArray, ValueAsObject, ValueObjectAccess};
use simd_json::{BorrowedValue, StaticNode};
use wasmtime::component::{bindgen, HasData, Resource, ResourceTable};
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

use crate::wasm::host::tangent::logs::log;
use log::Scalar;

bindgen!({
    world: "processor",
    path: "../../assets/wit",
    exports: {default: async},
    with: {
        "tangent:logs/log.logview": JsonLogView,
    }
});

pub struct HostEngine {
    pub ctx: WasiCtx,
    pub table: ResourceTable,
}

impl HostEngine {
    pub fn new(ctx: WasiCtx) -> Self {
        Self {
            ctx,
            table: ResourceTable::new(),
        }
    }
}

impl WasiView for HostEngine {
    fn ctx(&mut self) -> WasiCtxView<'_> {
        WasiCtxView {
            ctx: &mut self.ctx,
            table: &mut self.table,
        }
    }
}

struct JsonDoc {
    _raw: Bytes,
    doc: BorrowedValue<'static>,
}

#[derive(Clone)]
pub struct JsonLogView(Arc<JsonDoc>);

impl JsonLogView {
    pub fn from_bytes(mut line: BytesMut) -> anyhow::Result<Self> {
        let v: simd_json::BorrowedValue<'_> = simd_json::to_borrowed_value(line.as_mut())?;
        let v_static: simd_json::BorrowedValue<'static> = unsafe { std::mem::transmute(v) };

        let raw = line.freeze();

        Ok(Self(Arc::new(JsonDoc {
            _raw: raw,
            doc: v_static,
        })))
    }

    pub fn lookup<'a>(&'a self, path: &str) -> Option<&'a BorrowedValue<'a>> {
        let mut v = &self.0.doc;

        if let Some(val) = v.get(path) {
            return Some(val);
        }
        for seg in path.split('.') {
            if let Some((key, bracket)) = seg.split_once('[') {
                v = v.get(key)?;
                let mut rest = bracket;
                loop {
                    let start = 0;
                    let close = rest.find(']')?;
                    let idx: usize = rest[start..close].parse().ok()?;
                    v = v.as_array()?.get(idx)?;

                    if let Some(next_open) = rest[close + 1..].find('[') {
                        rest = &rest[close + 2 + next_open..];
                    } else {
                        break;
                    }
                }
            } else {
                v = v.get(seg)?;
            }
        }
        Some(v)
    }

    pub fn to_scalar(v: &BorrowedValue) -> Option<Scalar> {
        match v {
            BorrowedValue::String(s) => Some(Scalar::Str(s.to_string())),
            BorrowedValue::Static(StaticNode::I64(i)) => Some(Scalar::Int(*i)),
            BorrowedValue::Static(StaticNode::U64(i)) => Some(Scalar::Int(*i as i64)),
            BorrowedValue::Static(StaticNode::F64(f)) => Some(Scalar::Float(*f)),
            BorrowedValue::Static(StaticNode::Bool(b)) => Some(Scalar::Boolean(*b)),
            _ => None,
        }
    }
}

impl log::HostLogview for HostEngine {
    fn has(&mut self, h: Resource<JsonLogView>, path: String) -> bool {
        let present = {
            let v: &JsonLogView = match self.table.get(&h) {
                Ok(v) => v,
                Err(_) => return false,
            };
            v.lookup(&path).is_some()
        };
        present
    }

    fn get(&mut self, h: Resource<JsonLogView>, path: String) -> Option<log::Scalar> {
        let v: &JsonLogView = self.table.get(&h).ok()?;
        v.lookup(&path).and_then(JsonLogView::to_scalar)
    }

    fn len(&mut self, h: Resource<JsonLogView>, path: String) -> Option<u32> {
        let v: &JsonLogView = self.table.get(&h).ok()?;
        let item = v.lookup(&path)?;

        if item.is_array() {
            return Some(item.as_array().unwrap().len() as u32);
        }

        if item.is_str() {
            return Some(item.as_str().unwrap().len() as u32);
        }

        return None;
    }

    fn get_list(&mut self, h: Resource<JsonLogView>, path: String) -> Option<Vec<log::Scalar>> {
        let v: &JsonLogView = self.table.get(&h).ok()?;
        v.lookup(&path)?
            .as_array()
            .map(|arr| arr.iter().filter_map(JsonLogView::to_scalar).collect())
    }

    fn get_map(
        &mut self,
        h: Resource<JsonLogView>,
        path: String,
    ) -> Option<Vec<(String, log::Scalar)>> {
        let v: &JsonLogView = self.table.get(&h).ok()?;
        v.lookup(&path)?.as_object().map(|obj| {
            obj.iter()
                .filter_map(|(k, v)| JsonLogView::to_scalar(v).map(|s| (k.to_string(), s)))
                .collect::<Vec<(String, log::Scalar)>>()
        })
    }

    fn keys(&mut self, h: Resource<JsonLogView>, path: String) -> Vec<String> {
        let out = {
            let v: &JsonLogView = match self.table.get(&h) {
                Ok(v) => v,
                Err(_) => return vec![],
            };
            v.lookup(&path)
                .and_then(|vv| {
                    vv.as_object()
                        .map(|m| m.keys().map(|k| k.to_string()).collect())
                })
                .unwrap_or_default()
        };
        out
    }

    fn drop(&mut self, h: Resource<JsonLogView>) -> wasmtime::Result<()> {
        tracing::warn!("dropped");
        let _ = self.table.delete(h)?;
        Ok(())
    }
}

impl HasData for HostEngine {
    type Data<'a> = &'a mut Self;
}

impl log::Host for HostEngine {}

const MAX_MAPPER_VALUE_DEPTH: usize = 256;

fn mapper_value_from_index(index: u32, arena: &[log::Value], depth: usize) -> JsonValue {
    if depth >= MAX_MAPPER_VALUE_DEPTH {
        tracing::warn!(index, "mapper value exceeded maximum nesting depth");
        return JsonValue::Null;
    }

    let value = match arena.get(index as usize) {
        Some(value) => value,
        None => {
            tracing::warn!(
                index,
                total = arena.len(),
                "mapper value referenced invalid index"
            );
            return JsonValue::Null;
        }
    };

    match value {
        log::Value::NullValue => JsonValue::Null,
        log::Value::BoolValue(b) => JsonValue::Bool(*b),
        log::Value::StringValue(s) => JsonValue::String(s.clone()),
        log::Value::BlobValue(bytes) => {
            let encoded = BASE64_STANDARD.encode(bytes);
            JsonValue::String(encoded)
        }
        log::Value::S64Value(i) => JsonValue::Number(JsonNumber::from(*i)),
        log::Value::F64Value(f) => match JsonNumber::from_f64(*f) {
            Some(num) => JsonValue::Number(num),
            None => {
                tracing::warn!(value=%f, "guest produced non-finite float; encoding as null");
                JsonValue::Null
            }
        },
        log::Value::ListValue(children) => {
            let items = children
                .iter()
                .map(|child| mapper_value_from_index(*child, arena, depth + 1))
                .collect::<Vec<_>>();
            JsonValue::Array(items)
        }
        log::Value::MapValue(entries) => {
            let mut map = JsonMap::with_capacity(entries.len());
            for (key, child) in entries {
                map.insert(
                    key.clone(),
                    mapper_value_from_index(*child, arena, depth + 1),
                );
            }
            JsonValue::Object(map)
        }
    }
}

pub fn mapper_frame_to_json(frame: log::Frame) -> JsonValue {
    let log::Frame { values, fields } = frame;
    let mut obj = JsonMap::with_capacity(fields.len());
    for (key, index) in fields {
        obj.insert(key, mapper_value_from_index(index, &values, 0));
    }
    JsonValue::Object(obj)
}
