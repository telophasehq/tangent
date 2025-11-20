use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use reqwest::Client;
use simd_json::base::ValueAsScalar;
use simd_json::derived::{TypedArrayValue, TypedScalarValue};
use simd_json::prelude::{ValueAsArray, ValueAsObject, ValueObjectAccess};
use simd_json::{BorrowedValue, StaticNode};
use wasmtime::component::{bindgen, HasData, Resource, ResourceTable};
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

use crate::wasm::host::tangent::logs::log;
use crate::wasm::host::tangent::logs::remote;
use log::Scalar;

bindgen!({
    world: "processor",
    path: "../../assets/wit",
    exports: {default: async},
    imports: {
        "tangent:logs/remote.call-batch": async,
    },
    with: {
        "tangent:logs/log.logview": JsonLogView,
    }
});

pub struct HostEngine {
    pub ctx: WasiCtx,
    pub table: ResourceTable,
    http_client: Client,
}

impl HostEngine {
    pub fn new(ctx: WasiCtx) -> Self {
        Self {
            ctx,
            table: ResourceTable::new(),
            http_client: Client::new(),
        }
    }

    async fn execute_single(client: Client, r: remote::Request) -> remote::Response {
        use remote::Method;

        let method = match r.method {
            Method::Get => reqwest::Method::GET,
            Method::Post => reqwest::Method::POST,
            Method::Put => reqwest::Method::PUT,
            Method::Delete => reqwest::Method::DELETE,
            Method::Patch => reqwest::Method::PATCH,
        };

        let mut req_builder = client.request(method, &r.url);

        for (name, value) in &r.headers {
            req_builder = req_builder.header(name.as_str(), value.as_str());
        }

        if let Some(ms) = r.timeout_ms {
            req_builder = req_builder.timeout(std::time::Duration::from_millis(ms as u64));
        }

        if !r.body.is_empty() {
            req_builder = req_builder.body(r.body.clone());
        }

        match req_builder.send().await {
            Ok(res) => {
                let status = res.status().as_u16();
                let headers = res
                    .headers()
                    .iter()
                    .map(|(k, v)| (k.to_string(), v.to_str().unwrap_or_default().to_string()))
                    .collect::<Vec<(String, String)>>();

                let body_bytes = match res.bytes().await {
                    Ok(b) => b.to_vec(),
                    Err(e) => {
                        return remote::Response {
                            id: r.id,
                            status,
                            headers,
                            body: Vec::new(),
                            error: Some(format!("failed to read body: {e}")),
                        }
                    }
                };

                remote::Response {
                    id: r.id,
                    status,
                    headers,
                    body: body_bytes,
                    error: None,
                }
            }
            Err(e) => remote::Response {
                id: r.id,
                status: 0,
                headers: Vec::new(),
                body: Vec::new(),
                error: Some(e.to_string()),
            },
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

impl remote::Host for HostEngine {
    async fn call_batch(
        &mut self,
        reqs: Vec<remote::Request>,
    ) -> Result<Vec<remote::Response>, String> {
        let mut out = Vec::with_capacity(reqs.len());
        let client = self.http_client.clone();

        for r in reqs {
            let resp = Self::execute_single(client.clone(), r);
            out.push(resp.await);
        }

        Ok(out)
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
        let _ = self.table.delete(h)?;
        Ok(())
    }
}

impl HasData for HostEngine {
    type Data<'a> = &'a mut Self;
}

impl log::Host for HostEngine {}
