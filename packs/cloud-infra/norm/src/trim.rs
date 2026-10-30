use serde_json::{json, Map, Value};

pub fn of(source: &str) -> Option<fn(&Map<String, Value>) -> Map<String, Value>> {
    match source {
        "cloudtrail" => Some(cloudtrail),
        "github_audit" => Some(github_audit),
        "okta" => Some(okta),
        "google_workspace" => Some(google_workspace),
        _ => None,
    }
}

fn cloudtrail(m: &Map<String, Value>) -> Map<String, Value> {
    let mut out = pick(
        m,
        &[
            "ts",
            "source",
            "eventTime",
            "eventSource",
            "eventName",
            "awsRegion",
            "recipientAccountId",
            "sourceIPAddress",
            "userAgent",
            "errorCode",
            "errorMessage",
            "readOnly",
            "resources",
        ],
    );
    if let Some(u) = obj(m.get("userIdentity")) {
        let mut id = pick(u, &["type", "userName", "arn", "accountId", "invokedBy"]);
        let uv = Value::Object(u.clone());
        if let Some(role) = uv.pointer("/sessionContext/sessionIssuer/userName") {
            id.insert("role".into(), role.clone());
        }
        if let Some(mfa) = uv.pointer("/sessionContext/attributes/mfaAuthenticated") {
            id.insert("mfaAuthenticated".into(), mfa.clone());
        }
        out.insert("userIdentity".into(), Value::Object(id));
    }
    if let Some(rp) = m.get("requestParameters").filter(|v| !v.is_null()) {
        out.insert("requestParameters".into(), shrink(rp, 1500));
    }
    let name = m.get("eventName").and_then(Value::as_str).unwrap_or("");
    let granting = name == "ConsoleLogin"
        || name.starts_with("AssumeRole")
        || name.starts_with("Create")
        || name == "GetSessionToken"
        || name == "GetFederationToken";
    if let Some(re) = m.get("responseElements").filter(|v| !v.is_null()) {
        if granting || re.to_string().len() <= 300 {
            out.insert("responseElements".into(), shrink(re, 1000));
        }
    }
    if let Some(a) = obj(m.get("additionalEventData")) {
        let keep = pick(
            a,
            &["MFAUsed", "LoginTo", "SamlProviderArn", "CredentialType"],
        );
        if !keep.is_empty() {
            out.insert("additionalEventData".into(), Value::Object(keep));
        }
    }
    out
}

fn github_audit(m: &Map<String, Value>) -> Map<String, Value> {
    pick(
        m,
        &[
            "ts",
            "source",
            "action",
            "actor",
            "actor_is_bot",
            "actor_ip",
            "actor_location",
            "org",
            "repo",
            "user",
            "team",
            "visibility",
            "public_repo",
            "programmatic_access_type",
            "token_scopes",
            "integration",
            "operation_type",
            "permission",
            "old_permission",
            "name",
            "head_branch",
            "event",
            "conclusion",
            "runner_name",
            "runner_type",
            "pull_request_title",
            "pull_request_url",
            "topic",
            "ref",
            "key",
            "fingerprint",
            "read_only",
            "hook_id",
            "config",
            "target_login",
            "reason",
            "explanation",
        ],
    )
}

fn okta(m: &Map<String, Value>) -> Map<String, Value> {
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
    out
}

fn google_workspace(m: &Map<String, Value>) -> Map<String, Value> {
    let mut out = pick(m, &["ts", "source", "ipAddress", "ownerDomain"]);
    if let Some(id) = obj(m.get("id")) {
        out.insert(
            "id".into(),
            Value::Object(pick(id, &["time", "applicationName"])),
        );
    }
    if let Some(a) = obj(m.get("actor")) {
        out.insert(
            "actor".into(),
            Value::Object(pick(a, &["email", "callerType"])),
        );
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
                        let Some(name) = p.get("name").and_then(Value::as_str) else {
                            continue;
                        };
                        let v = [
                            "value",
                            "boolValue",
                            "intValue",
                            "multiValue",
                            "multiIntValue",
                            "messageValue",
                        ]
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

fn pick(m: &Map<String, Value>, keys: &[&str]) -> Map<String, Value> {
    let mut out = Map::new();
    for k in keys {
        if let Some(v) = m.get(*k) {
            out.insert((*k).to_string(), v.clone());
        }
    }
    out
}

fn obj(v: Option<&Value>) -> Option<&Map<String, Value>> {
    v.and_then(Value::as_object)
}

/// past `max` bytes, the top-level scalars and the names of the rest
fn shrink(v: &Value, max: usize) -> Value {
    if v.to_string().len() <= max {
        return v.clone();
    }
    match v {
        Value::Object(m) => {
            let mut out = Map::new();
            let mut dropped = Vec::new();
            for (k, x) in m {
                if x.is_object() || x.is_array() {
                    dropped.push(k.clone());
                } else {
                    out.insert(k.clone(), x.clone());
                }
            }
            if !dropped.is_empty() {
                out.insert("_omitted".into(), json!(dropped));
            }
            Value::Object(out)
        }
        Value::Array(a) => json!({"_omitted": format!("{} items", a.len())}),
        other => other.clone(),
    }
}
