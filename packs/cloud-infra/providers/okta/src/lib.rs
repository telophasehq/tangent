//! Okta System Log records: `_norm` (the user, the event type, the client's
//! address and country, the first target) and the record trimmed to what it
//! means, Okta's own risk signals from `debugData` included. The uuid,
//! version, request chain, trace ids and hashes go.
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

const SOURCE: &str = "okta";

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
        actor: get(r, "actor.alternateId"),
        org: None,
        kind: get(r, "eventType"),
        ip: get(r, "client.ipAddress"),
        country: get(r, "client.geographicalContext.country"),
        dest: None,
        target: get(r, "target[0].displayName"),
    }
}

fn trim(m: &Map<String, Value>) -> Map<String, Value> {
    let mut out = pick(m, &["ts", "source", "published", "eventType", "displayMessage", "severity", "outcome"]);
    if let Some(a) = obj(m.get("actor")) {
        out.insert("actor".into(), Value::Object(pick(a, &["type", "alternateId", "displayName"])));
    }
    if let Some(c) = obj(m.get("client")) {
        let mut client = pick(c, &["ipAddress", "zone", "device"]);
        if let Some(ua) = obj(c.get("userAgent")) {
            client.insert("userAgent".into(), Value::Object(pick(ua, &["os", "browser"])));
        }
        if let Some(g) = obj(c.get("geographicalContext")) {
            client.insert("geographicalContext".into(), Value::Object(pick(g, &["city", "state", "country"])));
        }
        out.insert("client".into(), Value::Object(client));
    }
    if let Some(sc) = obj(m.get("securityContext")) {
        out.insert("securityContext".into(), Value::Object(pick(sc, &["asNumber", "asOrg", "isp", "isProxy"])));
    }
    if let Some(targets) = m.get("target").and_then(Value::as_array) {
        let t: Vec<Value> = targets
            .iter()
            .filter_map(Value::as_object)
            .map(|t| Value::Object(pick(t, &["type", "alternateId", "displayName"])))
            .collect();
        if !t.is_empty() {
            out.insert("target".into(), Value::Array(t));
        }
    }
    if let Some(ac) = obj(m.get("authenticationContext")) {
        let keep = pick(ac, &["authenticationProvider", "credentialType", "credentialProvider"]);
        if !keep.is_empty() {
            out.insert("authenticationContext".into(), Value::Object(keep));
        }
    }
    // Okta's own risk signals live in debugData, beside request urls and
    // trace ids that do not travel.
    if let Some(dd) = m.get("debugContext").and_then(|d| d.get("debugData")).and_then(Value::as_object) {
        let keep = pick(dd, &["threatSuspected", "risk", "behaviors", "logOnlySecurityData", "factor"]);
        if !keep.is_empty() {
            out.insert("debugData".into(), Value::Object(keep));
        }
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
