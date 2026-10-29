//! CloudTrail records: `_norm` (who, which account, what, from where, on
//! what) and the record trimmed to what it means. A role session is the
//! role, an SSO session the person, AWS acting on its own `aws:<service>`
//! with `_norm.aws: true`, another account `account:<id>`. The response is
//! kept when it granted something (a sign-in, a key, a session); a Describe
//! call's response is bulk and goes. The ids, versions, categories and TLS
//! details go.
wit_bindgen::generate!({
    path: ".tangent/wit",
    world: "processor",
    generate_all,
});

use std::sync::OnceLock;

use common::{carry_attached, get, obj, org_from_profile, pick, shrink, Norm};
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
/// name is the person. AWS acting on its own is `aws:<service>`. Another
/// account assuming a role here is `account:<id>`.
fn actor_of(r: &Value) -> Option<String> {
    if let Some(service) = aws_acting(r) {
        return Some(format!("aws:{service}"));
    }
    let kind = get(r, "userIdentity.type").unwrap_or_default();
    match kind.as_str() {
        "AssumedRole" => {
            let Some(role) = get(r, "userIdentity.sessionContext.sessionIssuer.userName") else {
                return get(r, "userIdentity.userName").or_else(|| get(r, "userIdentity.arn"));
            };
            if role.starts_with("AWSReservedSSO_") {
                let arn = get(r, "userIdentity.arn").unwrap_or_default();
                if let Some(session) = arn.rsplit('/').next().filter(|s| !s.is_empty()) {
                    return Some(session.to_string());
                }
            }
            Some(role)
        }
        "AWSAccount" => get(r, "userIdentity.accountId").map(|a| format!("account:{a}")),
        "Root" => get(r, "userIdentity.userName").or_else(|| Some("root".to_string())),
        _ => get(r, "userIdentity.userName").or_else(|| get(r, "userIdentity.arn")),
    }
}

/// The service, when AWS acted on its own: a service-linked role's session,
/// a service calling as itself (`AWSService`), or a call only `invokedBy`
/// names. Every field read here is one AWS writes, so no name a customer
/// chooses becomes an `aws:` actor.
fn aws_acting(r: &Value) -> Option<String> {
    match get(r, "userIdentity.type").unwrap_or_default().as_str() {
        // By the service in the role's path, whatever invoked the call: the
        // same service also calls from plain addresses.
        "AssumedRole" => linked_service(&get(r, "userIdentity.sessionContext.sessionIssuer.arn")?),
        "AWSService" => Some(invoker(r)),
        "" if get(r, "userIdentity.invokedBy").is_some_and(|by| !by.is_empty()) => Some(invoker(r)),
        _ => None,
    }
}

/// The service a service-linked role belongs to: its path starts with
/// `aws-service-role/<service>.amazonaws.com/`. IAM reserves that prefix for
/// AWS, but only as a prefix; `role/x/aws-service-role/foo` is anyone's.
fn linked_service(arn: &str) -> Option<String> {
    let (_, path) = arn.split_once(":role/")?;
    let service = path.strip_prefix("aws-service-role/")?.split('/').next();
    Some(named(service.map(str::to_string)).unwrap_or_else(|| "service".to_string()))
}

/// Which service made a call: `invokedBy`, else an `*.amazonaws.com` address,
/// else the service-linked role it assumed, else the API's own service (an
/// ECR lifecycle event is `aws:ecr`).
fn invoker(r: &Value) -> String {
    named(get(r, "userIdentity.invokedBy"))
        .or_else(|| named(get(r, "sourceIPAddress").filter(|ip| ip.ends_with(".amazonaws.com"))))
        .or_else(|| get(r, "requestParameters.roleArn").and_then(|arn| linked_service(&arn)))
        .or_else(|| named(get(r, "eventSource")))
        .unwrap_or_else(|| "service".to_string())
}

/// A service's name without `.amazonaws.com`. `AWS Internal` names nothing:
/// unrelated services all write it.
fn named(s: Option<String>) -> Option<String> {
    s.map(|s| s.strip_suffix(".amazonaws.com").unwrap_or(&s).to_string())
        .filter(|s| !s.is_empty() && s != "AWS Internal")
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
            let Some(m) = rec.as_object() else {
                return Err("record is not a JSON object".to_string());
            };
            let mut n = norm_of(&rec);
            if let Some(o) = org() {
                n.org = Some(o.clone());
            }
            let mut trimmed = trim(m);
            carry_attached(m, &mut trimmed);
            // AWS acting says so in `_norm.aws`: facts reads the marker, since
            // an `aws:` name alone could be any source's user.
            let mut norm = serde_json::to_value(&n).map_err(|e| e.to_string())?;
            if aws_acting(&rec).is_some() {
                norm["aws"] = Value::Bool(true);
            }
            trimmed.insert("_norm".into(), norm);
            out.extend(serde_json::to_vec(&Value::Object(trimmed)).map_err(|e| e.to_string())?);
            out.push(b'\n');
        }
        Ok(out)
    }
}
