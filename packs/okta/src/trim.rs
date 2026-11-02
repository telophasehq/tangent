use normkit::{obj, pick, prune};
use serde_json::{Map, Value};

pub fn okta(m: &Map<String, Value>) -> Map<String, Value> {
    let mut out = pick(
        m,
        &[
            "ts",
            "source",
            "published",
            "eventType",
            "displayMessage",
            "severity",
            "outcome",
            "device",
        ],
    );
    if let Some(a) = obj(m.get("actor")) {
        out.insert(
            "actor".into(),
            Value::Object(pick(a, &["type", "alternateId", "displayName"])),
        );
    }
    if let Some(c) = obj(m.get("client")) {
        let mut client = pick(c, &["ipAddress", "zone", "device"]);
        if let Some(ua) = obj(c.get("userAgent")) {
            client.insert(
                "userAgent".into(),
                Value::Object(pick(ua, &["os", "browser"])),
            );
        }
        if let Some(g) = obj(c.get("geographicalContext")) {
            client.insert(
                "geographicalContext".into(),
                Value::Object(pick(g, &["city", "state", "country"])),
            );
        }
        out.insert("client".into(), Value::Object(client));
    }
    if let Some(sc) = obj(m.get("securityContext")) {
        out.insert(
            "securityContext".into(),
            Value::Object(pick(sc, &["asNumber", "asOrg", "isp", "isProxy"])),
        );
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
        let keep = pick(
            ac,
            &[
                "authenticationProvider",
                "credentialType",
                "credentialProvider",
            ],
        );
        if !keep.is_empty() {
            out.insert("authenticationContext".into(), Value::Object(keep));
        }
    }
    // okta's own risk signals
    if let Some(dd) = m
        .get("debugContext")
        .and_then(|d| d.get("debugData"))
        .and_then(Value::as_object)
    {
        let keep = pick(
            dd,
            &[
                "threatSuspected",
                "risk",
                "behaviors",
                "logOnlySecurityData",
                "factor",
            ],
        );
        if !keep.is_empty() {
            out.insert("debugData".into(), Value::Object(keep));
        }
    }
    prune(&mut out, &|k, x| {
        x.is_null() || (k == "zone" && x == "null")
    });
    out
}
