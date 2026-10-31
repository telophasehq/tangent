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

const PER_EVENT: &[&str] = &[
    "dd.trace_id",
    "dd.span_id",
    "trace_id",
    "traceId",
    "span_id",
    "spanId",
    "request_id",
    "requestId",
];

pub fn scrubbed_line(source: &str, line: &str) -> Option<String> {
    let mut m: Map<String, Value> = serde_json::from_str(line).ok()?;
    let n = m.len();
    m.retain(|k, _| !PER_EVENT.contains(&k.as_str()));
    if source == "hiro" {
        datadog(&mut m);
    }
    let signed = m.values_mut().fold(false, |any, v| unsign(v) | any);
    (signed || m.len() < n).then(|| serde_json::to_string(&m).ok())?
}

fn datadog(m: &mut Map<String, Value>) {
    m.remove("dd.version");
    m.remove("ddsource");
    for (dd, k) in [("dd.service", "service"), ("dd.env", "env")] {
        if m.get(dd) == m.get(k) {
            m.remove(dd);
        }
    }
}

const SIGNED: &[&str] = &[
    "sig",
    "Signature",
    "X-Amz-Signature",
    "X-Amz-Security-Token",
    "X-Goog-Signature",
    "token",
    "access_token",
];

fn unsign(v: &mut Value) -> bool {
    match v {
        Value::String(s) => unsigned(s).map(|u| *s = u).is_some(),
        Value::Array(a) => a.iter_mut().fold(false, |any, x| unsign(x) | any),
        Value::Object(m) => m.values_mut().fold(false, |any, x| unsign(x) | any),
        _ => false,
    }
}

/// `a?sv=1&sig=x b` -> `a?sv=1 b`; None when nothing is signed
pub fn unsigned(s: &str) -> Option<String> {
    if !s.contains(['?', '&', '#']) {
        return None;
    }
    let end = |c: char| c.is_whitespace() || "&\"'<>#,;)]}".contains(c);
    let (mut s, mut d, mut any) = (s.to_string(), 0, false);
    while let Some(i) = s[d..].find(['?', '&', '#']) {
        d += i;
        let pair = &s[d + 1..];
        let k = pair
            .find(|c| c == '=' || c == '?' || end(c))
            .unwrap_or(pair.len());
        let signed = SIGNED.iter().any(|n| n.eq_ignore_ascii_case(&pair[..k]));
        let Some(value) = pair[k..].strip_prefix('=').filter(|_| signed) else {
            d += 1;
            continue;
        };
        let v = s.len() - value.len() + value.find(end).unwrap_or(value.len());
        if s[v..].starts_with('&') {
            s.replace_range(d + 1..=v, "");
        } else {
            s.replace_range(d..v, "");
        }
        any = true;
    }
    any.then_some(s)
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
    // scrubbed after the size gates, so a scrub only ever takes away
    let rp = m.get("requestParameters").filter(|rp| !rp.is_null());
    if let Some(rp) = rp.and_then(|rp| scrubbed(shrink(rp, 1500), |_| {})) {
        out.insert("requestParameters".into(), rp);
    }
    let name = m.get("eventName").and_then(Value::as_str).unwrap_or("");
    let granting = name == "ConsoleLogin"
        || name.starts_with("AssumeRole")
        || name.starts_with("Create")
        || name == "GetSessionToken"
        || name == "GetFederationToken";
    let re = m
        .get("responseElements")
        .filter(|re| !re.is_null() && (granting || re.to_string().len() <= 300));
    if let Some(re) = re.and_then(|re| scrubbed(shrink(re, 1000), |re| unissued(re, name))) {
        out.insert("responseElements".into(), re);
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

/// Keys no verdict turns on, at any depth of a call's parameters or response.
const UNSEEN: &[&str] = &[
    "nextToken",
    "NextToken",
    "marker",
    "Marker",
    "nextMarker",
    "NextMarker",
    "paginationToken",
    "PaginationToken",
    "continuationToken",
    "ContinuationToken",
    "clientToken",
    "ClientToken",
    "idempotencyToken",
    "IdempotencyToken",
    "callerReference",
    "CallerReference",
    "requestId",
    "RequestId",
    "uploadId",
    "UploadId",
    "streamUrl",
    "sessionToken",
    "SessionToken",
    "tokenValue",
    "accessToken",
    "AccessToken",
    "idToken",
    "IdToken",
    "refreshToken",
    "RefreshToken",
    "secretAccessKey",
    "SecretAccessKey",
    "secretKey",
    "challengeParameters",
    "challengeResponses",
    "authParameters",
    "X-Amz-Signature",
    "X-Amz-Security-Token",
    "itemContentHash",
    "contentHash",
    "ifMatch",
    "IfMatch",
    "eTag",
    "ETag",
    "layerDigest",
    "layerDigests",
    "imageManifest",
    "aws-crypto-public-key",
    "sourceAAD",
    "destinationAAD",
    "packedPolicySize",
    "sessionTokenUtilization",
    "sessionTokenSize",
    "messageSchemaVersion",
    "schemaVersion",
    "x-id",
];

fn unissued(re: &mut Map<String, Value>, name: &str) {
    let issued = |k: &str| matches!(k, "credentials" | "Credentials" | "roleCredentials");
    re.retain(|k, _| !issued(k));
    if let Some(u) = re.get_mut("assumedRoleUser").and_then(Value::as_object_mut) {
        u.remove("assumedRoleId");
    }
    // shrink's names, less those cut anyway and assumedRoleUser (roleArn and roleSessionName)
    if let Some(o) = re.get_mut("_omitted").and_then(Value::as_array_mut) {
        let cut = |n: &str| issued(n) || n == "assumedRoleUser" || UNSEEN.contains(&n);
        o.retain(|n| !n.as_str().is_some_and(cut));
        if o.is_empty() {
            re.remove("_omitted");
        }
    }
    if name == "CreateInvalidation" {
        re.remove("location");
        if let Some(i) = re.get_mut("invalidation").and_then(Value::as_object_mut) {
            i.remove("id");
        }
    }
}

fn scrubbed(mut v: Value, cut: impl FnOnce(&mut Map<String, Value>)) -> Option<Value> {
    let was = empty(&v);
    if let Some(m) = v.as_object_mut() {
        cut(m);
    }
    kept(&mut v, &|k, _| UNSEEN.contains(&k));
    (was || !empty(&v)).then_some(v)
}

fn github_audit(m: &Map<String, Value>) -> Map<String, Value> {
    let mut out = pick(
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
    );
    // a hosted runner is named for its job's VM: `GitHub Actions 12`
    if let Some(n) = out.get("runner_name").and_then(Value::as_str) {
        let n = n.strip_prefix("GitHub Actions ").unwrap_or("");
        if !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()) {
            out.remove("runner_name");
        }
    }
    let action = m.get("action").and_then(Value::as_str).unwrap_or("");
    if action.starts_with("workflows.") {
        out.remove("topic");
    }
    out
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
    prune(&mut out, &|k, x| {
        x.is_null() || (k == "zone" && x == "null")
    });
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
                        let Some(name) = p
                            .get("name")
                            .and_then(Value::as_str)
                            .filter(|n| *n != "billable")
                        else {
                            continue;
                        };
                        let Some(v) = [
                            "value",
                            "boolValue",
                            "intValue",
                            "multiValue",
                            "multiIntValue",
                            "messageValue",
                        ]
                        .iter()
                        .find_map(|k| p.get(*k)) else {
                            continue;
                        };
                        pm.insert(name.to_string(), v.clone());
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

/// The entries `gone` picks, at any depth, and whatever their going empties.
fn prune(m: &mut Map<String, Value>, gone: &dyn Fn(&str, &Value) -> bool) {
    m.retain(|k, x| !gone(k, x) && kept(x, gone));
}

fn kept(x: &mut Value, gone: &dyn Fn(&str, &Value) -> bool) -> bool {
    let was = empty(x);
    match x {
        Value::Object(m) => prune(m, gone),
        Value::Array(a) => a.retain_mut(|x| kept(x, gone)),
        _ => {}
    }
    was || !empty(x)
}

fn empty(v: &Value) -> bool {
    match v {
        Value::Object(m) => m.is_empty(),
        Value::Array(a) => a.is_empty(),
        _ => false,
    }
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
