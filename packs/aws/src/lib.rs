//! CloudTrail: each event into `_norm`, trimmed to what a verdict turns on,
//! its address described from the runtime's tables, and what the company
//! profile says about it in `_facts`, with what only CloudTrail says (root
//! credentials used, ingress opened to the world) among them.

wit_bindgen::generate!({
    path: ".tangent/wit",
    world: "processor",
    generate_all,
});

mod trim;

use exports::tangent::logs::mapper::{Guest, Meta, Pred, Selector};
use normkit::Norm;
use serde_json::{Map, Value};
use tangent::logs::enrichment::lookup;
use tangent::logs::log::{Logview, Scalar};

struct Component;

export!(Component);

fn text(s: Scalar) -> Option<String> {
    match s {
        Scalar::Str(v) => Some(v),
        Scalar::Int(i) => Some(i.to_string()),
        Scalar::Float(f) => Some(f.to_string()),
        Scalar::Boolean(b) => Some(b.to_string()),
        Scalar::Bytes(_) => None,
    }
}

/// its URLs unsigned, as an untrimmed record's are
fn get(lv: &Logview, path: &str) -> Option<String> {
    let s = lv.get(path).and_then(text)?;
    Some(normkit::unsigned(&s).unwrap_or(s))
}

/// a role session is its role, an SSO session its person
fn cloudtrail_actor(lv: &Logview) -> Option<String> {
    let kind = get(lv, "userIdentity.type").unwrap_or_default();
    match kind.as_str() {
        "AssumedRole" => {
            let Some(role) = get(lv, "userIdentity.sessionContext.sessionIssuer.userName") else {
                return get(lv, "userIdentity.userName").or_else(|| get(lv, "userIdentity.arn"));
            };
            if role.starts_with("AWSReservedSSO_") {
                let arn = get(lv, "userIdentity.arn").unwrap_or_default();
                if let Some(session) = arn.rsplit('/').next().filter(|s| !s.is_empty()) {
                    return Some(session.to_string());
                }
            }
            Some(role)
        }
        "AWSAccount" => get(lv, "userIdentity.accountId").map(|a| format!("account:{a}")),
        "Root" => get(lv, "userIdentity.userName").or_else(|| Some("root".to_string())),
        _ => get(lv, "userIdentity.userName").or_else(|| get(lv, "userIdentity.arn")),
    }
}

/// The service, when AWS acted on its own; only fields AWS writes are read.
fn aws_acting(lv: &Logview) -> Option<String> {
    match get(lv, "userIdentity.type").unwrap_or_default().as_str() {
        "AssumedRole" => linked_service(&get(lv, "userIdentity.sessionContext.sessionIssuer.arn")?),
        "AWSService" => invoker(lv),
        "" if get(lv, "userIdentity.invokedBy").is_some_and(|by| !by.is_empty()) => invoker(lv),
        _ => None,
    }
}

/// `:role/aws-service-role/<service>.amazonaws.com/...`, as the path's prefix only
fn linked_service(arn: &str) -> Option<String> {
    let (_, path) = arn.split_once(":role/")?;
    let service = path.strip_prefix("aws-service-role/")?.split('/').next();
    named(service.map(str::to_string))
}

fn invoker(lv: &Logview) -> Option<String> {
    named(get(lv, "userIdentity.invokedBy"))
        .or_else(|| named(get(lv, "sourceIPAddress").filter(|ip| ip.ends_with(".amazonaws.com"))))
        .or_else(|| get(lv, "requestParameters.roleArn").and_then(|arn| linked_service(&arn)))
        .or_else(|| named(get(lv, "eventSource")))
}

/// without `.amazonaws.com`; `AWS Internal` names nothing
fn named(s: Option<String>) -> Option<String> {
    s.map(|s| s.strip_suffix(".amazonaws.com").unwrap_or(&s).to_string())
        .filter(|s| !s.is_empty() && s != "AWS Internal")
}

fn norm(lv: &Logview) -> Norm {
    let mut n = Norm::default();
    let aws = aws_acting(lv);
    if aws.is_some() {
        n.principal = Some("aws");
    }
    n.actor = aws
        .map(|s| format!("aws:{s}"))
        .or_else(|| cloudtrail_actor(lv));
    n.org = get(lv, "userIdentity.accountId");
    n.kind = get(lv, "eventName");
    // an AWS service or the SSO broker is not the caller's address
    n.ip = get(lv, "sourceIPAddress").filter(|ip| {
        !ip.ends_with(".amazonaws.com")
            && ip != "AWS Internal"
            && !matches!(
                n.kind.as_deref(),
                Some("AssumeRoleWithSAML" | "AssumeRoleWithWebIdentity")
            )
    });
    n.target = get(lv, "requestParameters.bucketName")
        .or_else(|| get(lv, "requestParameters.roleArn"))
        .or_else(|| get(lv, "requestParameters.userName"))
        .or_else(|| get(lv, "requestParameters.groupId"));
    n
}

/// What only CloudTrail says, read off the trimmed record.
fn facts(rec: &Map<String, Value>, n: &mut Norm) {
    if rec
        .get("userIdentity")
        .and_then(|u| u.get("type"))
        .and_then(Value::as_str)
        == Some("Root")
    {
        n.fact("root", "AWS root credentials used");
    }
    if rec
        .get("requestParameters")
        .is_some_and(|rp| rp.to_string().contains("0.0.0.0/0"))
    {
        n.fact("world_open", "opened to 0.0.0.0/0");
    }
}

impl Guest for Component {
    fn metadata() -> Meta {
        Meta {
            name: "aws".to_string(),
            version: "0.1.0".to_string(),
        }
    }

    fn probe() -> Vec<Selector> {
        vec![Selector {
            any: Vec::new(),
            all: vec![Pred::In((
                "source".to_string(),
                vec![Scalar::Str("cloudtrail".to_string())],
            ))],
            none: Vec::new(),
        }]
    }

    fn process_logs(input: Vec<Logview>) -> Result<Vec<u8>, String> {
        let kit = factskit::kit(lookup);
        let mut out = Vec::new();
        for lv in input {
            let mut n = norm(&lv);
            let rec = trim::cloudtrail(&normkit::record(&lv.log())?);
            facts(&rec, &mut n);
            kit.write(&mut out, rec, n)?;
        }
        Ok(out)
    }
}
