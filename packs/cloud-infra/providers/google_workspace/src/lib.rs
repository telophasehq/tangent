//! Google Workspace Reports API activity: `_norm` (the user, the first
//! event's name, the address) and the record trimmed to what it means, each
//! event's parameters flattened from a list of `{name, value}` into a map.
//! The kind, etag, qualifiers and customer ids go.
wit_bindgen::generate!({
    path: ".tangent/wit",
    world: "processor",
    generate_all,
});

use std::sync::OnceLock;

use common::{carry_attached, emit, get, obj, org_from_profile, pick, Norm};
use exports::tangent::logs::mapper::{Guest, Meta, Pred, Selector};
use serde_json::{Map, Value};
use tangent::logs::config;
use tangent::logs::log::{Logview, Scalar};

struct Component;

export!(Component);

const SOURCE: &str = "google_workspace";

/// The company: `org` in the profile, else `own_org` in the plugin's config.
fn org() -> &'static Option<String> {
    static O: OnceLock<Option<String>> = OnceLock::new();
    O.get_or_init(|| {
        org_from_profile().or_else(|| {
            config::get("own_org")
                .map(|s| s.trim().trim_matches('"').to_string())
                .filter(|s| !s.is_empty())
        })
    })
}


fn norm_of(r: &Value) -> Norm {
    Norm {
        actor: get(r, "actor.email"),
        org: None,
        kind: get(r, "events[0].name"),
        ip: get(r, "ipAddress"),
        country: None,
        dest: None,
        target: None,
    }
}

fn trim(m: &Map<String, Value>) -> Map<String, Value> {
    let mut out = pick(m, &["ts", "source", "ipAddress", "ownerDomain"]);
    if let Some(id) = obj(m.get("id")) {
        out.insert("id".into(), Value::Object(pick(id, &["time", "applicationName"])));
    }
    if let Some(a) = obj(m.get("actor")) {
        out.insert("actor".into(), Value::Object(pick(a, &["email", "callerType"])));
    }
    if let Some(events) = m.get("events").and_then(Value::as_array) {
        let evs: Vec<Value> = events
            .iter()
            .filter_map(Value::as_object)
            .map(|e| {
                let mut ev = pick(e, &["type", "name"]);
                if let Some(params) = e.get("parameters").and_then(Value::as_array) {
                    let mut pm = Map::new();
                    for p in params.iter().filter_map(Value::as_object) {
                        let Some(name) = p.get("name").and_then(Value::as_str) else { continue };
                        let v = ["value", "boolValue", "intValue", "multiValue", "multiIntValue", "messageValue"]
                            .iter()
                            .find_map(|k| p.get(*k))
                            .cloned()
                            .unwrap_or(Value::Null);
                        pm.insert(name.to_string(), v);
                    }
                    if !pm.is_empty() {
                        ev.insert("parameters".into(), Value::Object(pm));
                    }
                }
                Value::Object(ev)
            })
            .collect();
        out.insert("events".into(), Value::Array(evs));
    }
    out
}

impl Guest for Component {
    fn metadata() -> Meta {
        Meta {
            name: SOURCE.to_string(),
            version: "0.1.0".to_string(),
        }
    }

    /// Only this provider's records; the router may hand the plugin a batch
    /// of everything, and the probe is the filter.
    fn probe() -> Vec<Selector> {
        vec![Selector {
            any: Vec::new(),
            all: vec![Pred::Eq(("source".to_string(), Scalar::Str(SOURCE.to_string())))],
            none: Vec::new(),
        }]
    }

    fn process_logs(input: Vec<Logview>) -> Result<Vec<u8>, String> {
        let mut out = Vec::new();
        for lv in input {
            let rec: Value = serde_json::from_str(&lv.log()).map_err(|e| e.to_string())?;
            let Value::Object(m) = rec else {
                return Err("record is not a JSON object".to_string());
            };
            let mut n = norm_of(&Value::Object(m.clone()));
            if let Some(o) = org() {
                n.org = Some(o.clone());
            }
            let mut trimmed = trim(&m);
            carry_attached(&m, &mut trimmed);
            emit(&mut out, trimmed, &n)?;
        }
        Ok(out)
    }
}
