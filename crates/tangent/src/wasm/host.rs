use std::sync::Arc;

use serde_json::Value;
use wasmtime::component::{bindgen, HasData, Resource, ResourceTable};
use wasmtime_wasi::p2::{IoView, WasiCtx, WasiView};

use crate::wasm::host::tangent::logs::log;
use log::Scalar;

bindgen!({
    world: "processor",
    path: "../../assets/wit",
    async: true,
    with: {
        "tangent:logs/log/logview": JsonLogView,
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

    pub fn debug_has_logview(&mut self, h: &Resource<JsonLogView>) -> bool {
        self.table.get(h).is_ok()
    }
}

impl IoView for HostEngine {
    fn table(&mut self) -> &mut ResourceTable {
        &mut self.table
    }
}
impl WasiView for HostEngine {
    fn ctx(&mut self) -> &mut WasiCtx {
        &mut self.ctx
    }
}

#[derive(Clone)]
pub struct JsonLogView {
    doc: Arc<Value>,
}

impl JsonLogView {
    pub const fn new(doc: Arc<Value>) -> Self {
        Self { doc }
    }

    pub fn lookup(&self, path: &str) -> Option<&Value> {
        let mut v = &*self.doc;
        for seg in path.split('.') {
            if let Some((key, _)) = seg.split_once('[') {
                let start = seg.find('[')? + 1;
                let end = seg.len().checked_sub(1)?;
                let idx: usize = seg[start..end].parse().ok()?;
                v = v.get(key)?.get(idx)?;
            } else {
                v = v.get(seg)?;
            }
        }
        Some(v)
    }

    pub fn to_scalar(v: &Value) -> Option<log::Scalar> {
        match v {
            Value::String(s) => Some(Scalar::Str(s.to_string())),
            Value::Number(n) => n
                .as_i64()
                .map_or_else(|| n.as_f64().map(Scalar::Float), |i| Some(Scalar::Int(i))),
            Value::Bool(b) => Some(Scalar::Boolean(*b)),
            Value::Array(_) | Value::Object(_) | Value::Null => None,
        }
    }
}

impl log::HostLogview for HostEngine {
    async fn has(&mut self, h: Resource<JsonLogView>, path: String) -> bool {
        let present = {
            let v: &JsonLogView = match self.table.get(&h) {
                Ok(v) => v,
                Err(_) => return false,
            };
            v.lookup(&path).is_some()
        };
        present
    }

    async fn get(&mut self, h: Resource<JsonLogView>, path: String) -> Option<log::Scalar> {
        let out = {
            let v: &JsonLogView = self.table.get(&h).ok()?;
            v.lookup(&path).and_then(JsonLogView::to_scalar)
        };
        out
    }

    async fn get_list(
        &mut self,
        h: Resource<JsonLogView>,
        path: String,
    ) -> Option<Vec<log::Scalar>> {
        let out = {
            let v: &JsonLogView = self.table.get(&h).ok()?;
            v.lookup(&path)?
                .as_array()
                .map(|arr| arr.iter().filter_map(JsonLogView::to_scalar).collect())
        };
        out
    }

    async fn get_map(
        &mut self,
        h: Resource<JsonLogView>,
        path: String,
    ) -> Option<Vec<(String, log::Scalar)>> {
        let out = {
            let v: &JsonLogView = self.table.get(&h).ok()?;
            v.lookup(&path)?.as_object().map(|obj| {
                obj.iter()
                    .filter_map(|(k, v)| JsonLogView::to_scalar(v).map(|s| (k.clone(), s)))
                    .collect()
            })
        };
        out
    }

    async fn keys(&mut self, h: Resource<JsonLogView>, path: String) -> Vec<String> {
        let out = {
            let v: &JsonLogView = match self.table.get(&h) {
                Ok(v) => v,
                Err(_) => return vec![],
            };
            v.lookup(&path)
                .and_then(|vv| vv.as_object().map(|m| m.keys().cloned().collect()))
                .unwrap_or_default()
        };
        out
    }

    async fn drop(&mut self, h: Resource<JsonLogView>) -> wasmtime::Result<()> {
        let _ = self.table.delete(h)?;
        Ok(())
    }
}

impl HasData for HostEngine {
    type Data<'a> = &'a mut Self;
}

impl log::Host for HostEngine {}
