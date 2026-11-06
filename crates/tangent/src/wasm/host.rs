use std::sync::Arc;

use ahash::HashMap;
use anyhow::Result;
use bytes::{Bytes, BytesMut};
use reqwest::Client;
use serde_json::Value as JSONValue;
use simd_json::base::ValueAsScalar;
use simd_json::derived::{TypedArrayValue, TypedScalarValue};
use simd_json::prelude::Writable;
use simd_json::prelude::{ValueAsArray, ValueAsObject, ValueObjectAccess};
use simd_json::{BorrowedValue, StaticNode};
use wasmtime::component::{bindgen, HasData, Resource, ResourceTable};
use wasmtime_wasi::{WasiCtx, WasiCtxView, WasiView};

use crate::enrichment::Tables;
use crate::state::{StateStore, StateValue, Txn};
use crate::wasm::host::tangent::logs::log;
use crate::wasm::host::tangent::logs::remote;
use log::Scalar;

bindgen!({
    world: "processor",
    path: "../../assets/wit",
    exports: {default: async},
    imports: {
        "tangent:logs/remote.call-batch": async,
        "tangent:logs/state.get": async,
        "tangent:logs/state.set": async,
        "tangent:logs/state.del": async,
        "tangent:logs/state.incr": async,
        "tangent:logs/state.scan": async,
        "tangent:logs/state.begin": async,
        "tangent:logs/state.commit": async,
    },
    with: {
        "tangent:logs/log.logview": JsonLogView,
    }
});

pub struct HostEngine {
    pub ctx: WasiCtx,
    pub table: ResourceTable,
    http_client: Client,
    state: Arc<dyn StateStore>,
    /// Namespace of this plugin's state: the one every plugin shares, unless
    /// the plugin was given a `state_namespace` of its own.
    ns: Arc<str>,
    /// The transaction the guest began and has not ended.
    txn: Option<Box<dyn Txn>>,
    plugin_cfg: Arc<HashMap<String, JSONValue>>,
    /// The process's enrichment tables, shared with every other instance.
    tables: Arc<Tables>,
    /// If true, short-circuit remote calls with successful empty responses.
    pub disable_remote_calls: bool,
}

impl HostEngine {
    pub fn new(
        ctx: WasiCtx,
        state: Arc<dyn StateStore>,
        ns: Arc<str>,
        config: Arc<HashMap<String, JSONValue>>,
        tables: Arc<Tables>,
        disable_remote_calls: bool,
    ) -> Self {
        Self {
            ctx,
            table: ResourceTable::new(),
            http_client: Client::new(),
            state,
            ns,
            txn: None,
            plugin_cfg: config,
            tables,
            disable_remote_calls,
        }
    }

    /// Rolls back a transaction the guest left open when its call ended: it
    /// trapped, failed or never committed. True when there was one. Until
    /// then no other state call runs, so the worker ends every call.
    pub fn end_call(&mut self) -> bool {
        self.txn.take().is_some()
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

impl From<Scalar> for StateValue {
    fn from(s: Scalar) -> Self {
        match s {
            Scalar::Str(s) => StateValue::Str(s),
            Scalar::Int(i) => StateValue::Int(i),
            Scalar::Float(f) => StateValue::Float(f),
            Scalar::Boolean(b) => StateValue::Bool(b),
            Scalar::Bytes(b) => StateValue::Bytes(b),
        }
    }
}

impl From<StateValue> for Scalar {
    fn from(v: StateValue) -> Self {
        match v {
            StateValue::Str(s) => Scalar::Str(s),
            StateValue::Int(i) => Scalar::Int(i),
            StateValue::Float(f) => Scalar::Float(f),
            StateValue::Bool(b) => Scalar::Boolean(b),
            StateValue::Bytes(b) => Scalar::Bytes(b),
            // The guest has no JSON scalar; it gets the text.
            StateValue::Json(j) => Scalar::Str(j.to_string()),
        }
    }
}

impl remote::Host for HostEngine {
    async fn call_batch(
        &mut self,
        reqs: Vec<remote::Request>,
    ) -> Result<Vec<remote::Response>, String> {
        if self.txn.is_some() {
            return Err("commit or roll back the state transaction first: it would hold every worker's state for the round trip".into());
        }
        if self.disable_remote_calls {
            // Short-circuit with successful empty responses.
            let out = reqs
                .into_iter()
                .map(|r| remote::Response {
                    id: r.id,
                    status: 204,
                    headers: Vec::new(),
                    body: Vec::new(),
                    error: None,
                })
                .collect();
            return Ok(out);
        }

        let mut out = Vec::with_capacity(reqs.len());
        let client = self.http_client.clone();

        for r in reqs {
            let resp = Self::execute_single(client.clone(), r);
            out.push(resp.await);
        }

        Ok(out)
    }
}

impl tangent::logs::config::Host for HostEngine {
    fn get(&mut self, key: String) -> Option<String> {
        self.plugin_cfg.get(&key).map(|v| {
            match v {
                // Plain string config (e.g. slack_channel)
                serde_json::Value::String(s) => s.clone(),
                // Anything else (objects, arrays, numbers, bools, null)
                // – serialize to JSON for the guest
                other => other.to_string(),
            }
        })
    }
}

/// A lookup is an index search in memory the tables already hold: no file,
/// no lock, nothing async.
impl tangent::logs::enrichment::Host for HostEngine {
    fn lookup(&mut self, table: String, key: String) -> Option<String> {
        self.tables.lookup(&table, &key)
    }
}

/// The guest's state: the plugins' namespace of the state store, shared by
/// every record (a plugin keys per tenant itself). Each call stands alone,
/// unless the guest began a transaction: then they go through it until it
/// commits or rolls back.
impl tangent::logs::state::Host for HostEngine {
    async fn get(&mut self, key: String) -> Result<Option<Scalar>, String> {
        let got = match self.txn.as_mut() {
            Some(t) => t.get(&self.ns, &key).await,
            None => self.state.get(&self.ns, &key).await,
        };
        got.map(|v| v.map(Scalar::from)).map_err(|e| e.to_string())
    }

    async fn set(&mut self, key: String, value: Scalar, ttl_ms: Option<u64>) -> Result<(), String> {
        let value = value.into();
        match self.txn.as_mut() {
            Some(t) => t.set(&self.ns, &key, &value, ttl_ms).await,
            None => self.state.set(&self.ns, &key, &value, ttl_ms).await,
        }
        .map_err(|e| e.to_string())
    }

    async fn del(&mut self, key: String) -> Result<bool, String> {
        match self.txn.as_mut() {
            Some(t) => t.del(&self.ns, &key).await,
            None => self.state.del(&self.ns, &key).await,
        }
        .map_err(|e| e.to_string())
    }

    async fn incr(&mut self, key: String, by: i64, ttl_ms: Option<u64>) -> Result<i64, String> {
        match self.txn.as_mut() {
            Some(t) => t.incr(&self.ns, &key, by, ttl_ms).await,
            None => self.state.incr(&self.ns, &key, by, ttl_ms).await,
        }
        .map_err(|e| e.to_string())
    }

    async fn scan(
        &mut self,
        from: String,
        to: String,
        limit: u32,
    ) -> Result<Vec<(String, Scalar)>, String> {
        let limit = limit as usize;
        match self.txn.as_mut() {
            Some(t) => t.range(&self.ns, &from, &to, limit).await,
            None => self.state.range(&self.ns, &from, &to, limit).await,
        }
        .map(|entries| {
            entries
                .into_iter()
                .map(|e| (e.key, Scalar::from(e.value)))
                .collect()
        })
        .map_err(|e| e.to_string())
    }

    async fn begin(&mut self) -> Result<(), String> {
        if self.txn.is_some() {
            return Err("a transaction is open already".into());
        }
        self.txn = Some(self.state.begin().await.map_err(|e| e.to_string())?);
        Ok(())
    }

    async fn commit(&mut self) -> Result<(), String> {
        match self.txn.take() {
            Some(t) => t.commit().await.map_err(|e| e.to_string()),
            None => Err("no transaction is open".into()),
        }
    }

    fn rollback(&mut self) {
        self.txn = None;
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

    /// Size of the record this view was parsed from.
    pub fn byte_len(&self) -> usize {
        self.0._raw.len()
    }

    /// The record as JSON text again.
    pub fn to_json(&self) -> String {
        self.0.doc.encode()
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
    fn log(&mut self, h: Resource<JsonLogView>) -> String {
        // Serialized again rather than the input bytes: parsing de-escaped
        // strings in place, so the buffer is not JSON any more once a value
        // held a quote or a backslash.
        let v: &JsonLogView = self.table.get(&h).unwrap();
        v.to_json()
    }

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

#[cfg(test)]
mod tests {
    use super::*;

    /// simd-json parses in place: a de-escaped string is written over the
    /// input, so the bytes a view was parsed from are not JSON any more once
    /// a value held a quote or a backslash. `log()` encodes the tree again.
    #[test]
    fn a_record_with_an_escaped_quote_round_trips_through_the_view() {
        let input = br#"{"why":"he said \"hi\"","flag":true}"#;
        let view = JsonLogView::from_bytes(BytesMut::from(&input[..])).unwrap();
        let again: serde_json::Value = serde_json::from_str(&view.to_json()).unwrap();
        assert_eq!(again["why"], "he said \"hi\"");
        assert_eq!(again["flag"], true);
    }
    use tangent::logs::{remote::Host as _, state::Host as _};

    fn guest(store: &Arc<dyn StateStore>) -> HostEngine {
        HostEngine::new(
            wasmtime_wasi::WasiCtxBuilder::new().build(),
            Arc::clone(store),
            Arc::from("baseline"),
            Arc::new(HashMap::default()),
            Arc::new(Tables::default()),
            true,
        )
    }

    fn sqlite(dir: &std::path::Path) -> Arc<dyn StateStore> {
        Arc::new(crate::state::sqlite::SqliteStore::open(&Default::default(), dir).unwrap())
    }

    /// Whether a call on `store` from elsewhere gets through within 50 ms.
    async fn free(store: &Arc<dyn StateStore>) -> bool {
        let store = Arc::clone(store);
        tokio::time::timeout(
            std::time::Duration::from_millis(50),
            tokio::spawn(async move { store.get("baseline", "x").await.unwrap() }),
        )
        .await
        .is_ok()
    }

    /// Eight instances of a plugin, as on eight workers, each call reading a
    /// count and writing it back one higher inside a transaction: none of
    /// the 200 is lost.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_transaction_holds_a_read_and_its_write_across_workers() {
        let dir = tempfile::tempdir().unwrap();
        let store = sqlite(dir.path());
        let workers: Vec<_> = (0..8)
            .map(|_| {
                let store = Arc::clone(&store);
                tokio::spawn(async move {
                    let mut g = guest(&store);
                    for _ in 0..25 {
                        g.begin().await.unwrap();
                        let n = match g.get("count".into()).await.unwrap() {
                            Some(Scalar::Int(n)) => n,
                            _ => 0,
                        };
                        tokio::task::yield_now().await;
                        g.set("count".into(), Scalar::Int(n + 1), None)
                            .await
                            .unwrap();
                        g.commit().await.unwrap();
                        assert!(!g.end_call(), "nothing left open");
                    }
                })
            })
            .collect();
        for w in workers {
            w.await.unwrap();
        }
        assert_eq!(
            store.get("baseline", "count").await.unwrap(),
            Some(StateValue::Int(200))
        );
    }

    /// Without `begin`, each state call lands at once and holds nothing.
    #[tokio::test(flavor = "multi_thread")]
    async fn without_a_transaction_each_call_stands_alone() {
        let dir = tempfile::tempdir().unwrap();
        let store = sqlite(dir.path());
        let mut g = guest(&store);
        g.set("a".into(), Scalar::Int(1), None).await.unwrap();
        assert!(free(&store).await);
        assert_eq!(
            store.get("baseline", "a").await.unwrap(),
            Some(StateValue::Int(1))
        );
        assert!(!g.end_call());
    }

    /// Only a commit keeps a transaction's writes: a rollback, or a call
    /// that returns with one open, undoes them and frees the store.
    #[tokio::test(flavor = "multi_thread")]
    async fn only_a_commit_keeps_a_transactions_writes() {
        let dir = tempfile::tempdir().unwrap();
        let store = sqlite(dir.path());
        let mut g = guest(&store);
        g.begin().await.unwrap();
        g.set("a".into(), Scalar::Int(1), None).await.unwrap();
        g.rollback();
        assert_eq!(store.get("baseline", "a").await.unwrap(), None);

        g.begin().await.unwrap();
        g.set("b".into(), Scalar::Int(2), None).await.unwrap();
        assert!(!free(&store).await, "an open transaction holds the store");
        assert!(g.end_call(), "left open");
        assert!(free(&store).await);
        assert_eq!(store.get("baseline", "b").await.unwrap(), None);
    }

    /// Inside a transaction a second `begin` and a remote call fail, and a
    /// commit needs one to commit.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_transaction_refuses_a_second_begin_and_a_remote_call() {
        let dir = tempfile::tempdir().unwrap();
        let store = sqlite(dir.path());
        let mut g = guest(&store);
        assert!(g.commit().await.is_err(), "nothing to commit");
        g.begin().await.unwrap();
        assert!(g.begin().await.is_err());
        assert!(g.call_batch(Vec::new()).await.is_err());
        g.commit().await.unwrap();
        assert!(g.call_batch(Vec::new()).await.is_ok());
    }
}
