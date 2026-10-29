//! CloudTrail records: `_norm` (who, which account, what, from where, on
//! what) and the record trimmed to what it means. A role session is the
//! role, an SSO session the person, AWS acting for you nobody, another
//! account `account:<id>`. The response is kept when it granted something
//! (a sign-in, a key, a session); a Describe call's response is bulk and
//! goes. The ids, versions, categories and TLS details go.
wit_bindgen::generate!({
    path: ".tangent/wit",
    world: "processor",
    generate_all,
});

use std::sync::OnceLock;

use common::{carry_attached, emit, get, obj, org_from_profile, pick, shrink, Norm};
use exports::tangent::logs::mapper::{Guest, Meta, Pred, Selector};
use serde_json::{Map, Value};
use tangent::logs::config;
use tangent::logs::log::{Logview, Scalar};

struct Component;

export!(Component);

const SOURCE: &str = "cloudtrail";

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


/// Who acted. An IAM user by name. A role session by the role, since the
/// session name is new every time, except an SSO role, where the session
/// name is the person. A service-linked role, or a service calling as itself,
/// is nobody: there is no principal to know. Another account assuming a role
/// here is `account:<id>`.
fn actor_of(r: &Value) -> Option<String> {
    let kind = get(r, "userIdentity.type").unwrap_or_default();
    match kind.as_str() {
        "AssumedRole" => {
            let Some(role) = get(r, "userIdentity.sessionContext.sessionIssuer.userName") else {
                return get(r, "userIdentity.userName").or_else(|| get(r, "userIdentity.arn"));
            };
            let issuer = get(r, "userIdentity.sessionContext.sessionIssuer.arn").unwrap_or_default();
            if issuer.contains("/aws-service-role/") {
                return None;
            }
            if role.starts_with("AWSReservedSSO_") {
                let arn = get(r, "userIdentity.arn").unwrap_or_default();
                if let Some(session) = arn.rsplit('/').next().filter(|s| !s.is_empty()) {
                    return Some(session.to_string());
                }
            }
            Some(role)
        }
        "AWSService" => None,
        "AWSAccount" => get(r, "userIdentity.accountId").map(|a| format!("account:{a}")),
        "Root" => get(r, "userIdentity.userName").or_else(|| Some("root".to_string())),
        "" if get(r, "userIdentity.invokedBy").is_some() => None,
        _ => get(r, "userIdentity.userName").or_else(|| get(r, "userIdentity.arn")),
    }
}

fn norm_of(r: &Value) -> Norm {
    let kind = get(r, "eventName");
    // The address is the caller's, except when the caller is an AWS service
    // (not an address) or the identity broker: a federated sign-in is
    // recorded from IAM Identity Center's address, not the person's.
    let ip = get(r, "sourceIPAddress").filter(|ip| {
        !ip.ends_with(".amazonaws.com")
            && ip != "AWS Internal"
            && !matches!(kind.as_deref(), Some("AssumeRoleWithSAML" | "AssumeRoleWithWebIdentity"))
    });
    Norm {
        actor: actor_of(r),
        org: get(r, "userIdentity.accountId"),
        kind,
        ip,
        country: None,
        dest: None,
        // The object acted on, when it has a stable name.
        target: get(r, "requestParameters.bucketName")
            .or_else(|| get(r, "requestParameters.roleArn"))
            .or_else(|| get(r, "requestParameters.userName"))
            .or_else(|| get(r, "requestParameters.groupId")),
    }
}

fn trim(m: &Map<String, Value>) -> Map<String, Value> {
    // `recipientAccountId` is the account the event happened in; the
    // caller's (`userIdentity.accountId`) differs when it reaches across, and
    // an organization trail holds every account.
    let mut out = pick(m, &["ts", "source", "eventTime", "eventSource", "eventName", "awsRegion", "recipientAccountId", "sourceIPAddress", "userAgent", "errorCode", "errorMessage", "readOnly", "resources"]);
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
        let keep = pick(a, &["MFAUsed", "LoginTo", "SamlProviderArn", "CredentialType"]);
        if !keep.is_empty() {
            out.insert("additionalEventData".into(), Value::Object(keep));
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
