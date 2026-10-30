//! Each source's shape into `_norm`; the four API sources' records are trimmed too.

wit_bindgen::generate!({
    path: ".tangent/wit",
    world: "processor",
    generate_all,
});

mod trim;

use std::collections::BTreeSet;
use std::sync::OnceLock;

use exports::tangent::logs::mapper::{Guest, Meta, Pred, Selector};
use serde::Serialize;
use tangent::logs::config;
use tangent::logs::log::{Logview, Scalar};

/// The company's own name and the sources that are its own infrastructure.
fn own() -> &'static (Option<String>, BTreeSet<String>) {
    static O: OnceLock<(Option<String>, BTreeSet<String>)> = OnceLock::new();
    O.get_or_init(|| {
        if let Some(f) = std::fs::read_to_string("/data/profile.yaml")
            .ok()
            .and_then(|t| serde_yaml::from_str::<serde_json::Value>(&t).ok())
        {
            let org = f["org"].as_str().map(str::to_string);
            let sources = f["own_sources"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|v| v.as_str().map(str::to_string))
                        .collect()
                })
                .unwrap_or_default();
            return (org, sources);
        }
        let org = config::get("own_org")
            .map(|s| s.trim().trim_matches('"').to_string())
            .filter(|s| !s.is_empty());
        let sources = config::get("own_sources")
            .and_then(|raw| serde_json::from_str::<Vec<String>>(&raw).ok())
            .unwrap_or_default()
            .into_iter()
            .collect();
        (org, sources)
    })
}

struct Component;

export!(Component);

#[derive(Default, Serialize)]
struct Norm {
    #[serde(skip_serializing_if = "Option::is_none")]
    actor: Option<String>,
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    aws: bool, // AWS acting on its own
    #[serde(skip_serializing_if = "Option::is_none")]
    org: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    ip: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    country: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    dest: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    target: Option<String>,
}

fn text(s: Scalar) -> Option<String> {
    match s {
        Scalar::Str(v) => Some(v),
        Scalar::Int(i) => Some(i.to_string()),
        Scalar::Float(f) => Some(f.to_string()),
        Scalar::Boolean(b) => Some(b.to_string()),
        Scalar::Bytes(_) => None,
    }
}

fn get(lv: &Logview, path: &str) -> Option<String> {
    lv.get(path).and_then(text)
}

/// `https://api.github.com/repos/x` -> `api.github.com`
fn host_of(url: &str) -> String {
    let rest = url.split("://").nth(1).unwrap_or(url);
    rest.split(['/', '?', ':'])
        .next()
        .unwrap_or(rest)
        .to_string()
}

/// `acme/web` -> `acme`
fn org_of_repo(repo: &str) -> Option<String> {
    repo.split('/').next().map(str::to_string)
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

fn norm(lv: &Logview, source: &str) -> Norm {
    let mut n = Norm::default();
    match source {
        "okta" => {
            n.actor = get(lv, "actor.alternateId");
            n.kind = get(lv, "eventType");
            n.ip = get(lv, "client.ipAddress");
            n.country = get(lv, "client.geographicalContext.country");
            n.target = get(lv, "target[0].displayName");
        }
        "github_audit" => {
            n.actor = get(lv, "actor");
            n.org = get(lv, "org");
            n.kind = get(lv, "action");
            n.ip = get(lv, "actor_ip");
            n.country = get(lv, "actor_location.country_code");
            n.target = get(lv, "repo");
        }
        "cloudtrail" => {
            let aws = aws_acting(lv);
            n.aws = aws.is_some();
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
        }
        "google_workspace" => {
            n.actor = get(lv, "actor.email");
            n.kind = get(lv, "events[0].name");
            n.ip = get(lv, "ipAddress");
        }
        "app" => {
            n.actor = get(lv, "actor");
            n.org = get(lv, "org");
            n.kind = get(lv, "event");
            n.ip = get(lv, "ip");
            n.target = get(lv, "target").or_else(|| get(lv, "key_id"));
        }
        // A CI run's story is told per repository: a run id is new every time.
        "gha_ci" => {
            n.actor = get(lv, "repo");
            n.org = get(lv, "repo").and_then(|r| org_of_repo(&r));
            n.kind = get(lv, "step");
            n.target = get(lv, "repo");
        }
        "runner_egress" => {
            n.actor = get(lv, "repo");
            n.org = get(lv, "repo").and_then(|r| org_of_repo(&r));
            n.kind = Some("egress".to_string());
            n.dest = get(lv, "dst_host");
            n.target = get(lv, "repo");
        }
        // Hiro lines are judged per organization; a plan id is new every time.
        "hiro" => {
            n.org = get(lv, "organization_id");
            n.kind = get(lv, "event");
            n.ip = get(lv, "client_ip");
            n.dest = get(lv, "destination").map(|d| host_of(&d));
            n.target = match n.kind.as_deref() {
                Some("secret_read") => get(lv, "path"),
                Some("tool_result") => get(lv, "tool"),
                Some("aws_api_call") => get(lv, "api"),
                _ => None,
            };
        }
        _ => {
            n.actor = get(lv, "actor");
            n.org = get(lv, "org");
            n.kind = get(lv, "event");
            n.ip = get(lv, "ip");
        }
    }
    // The company's own infrastructure is one tenant: the company.
    let (own_org, own_sources) = own();
    if let Some(org) = own_org {
        if own_sources.contains(source) {
            n.org = Some(org.clone());
        }
    }
    n
}

impl Guest for Component {
    fn metadata() -> Meta {
        Meta {
            name: "norm".to_string(),
            version: "0.1.0".to_string(),
        }
    }

    fn probe() -> Vec<Selector> {
        vec![Selector {
            any: Vec::new(),
            all: vec![Pred::Has("source".to_string())],
            none: Vec::new(),
        }]
    }

    fn process_logs(input: Vec<Logview>) -> Result<Vec<u8>, String> {
        let mut out = Vec::new();
        for lv in input {
            let source = get(&lv, "source").unwrap_or_default();
            let n = norm(&lv, &source);
            let added = serde_json::to_string(&n).map_err(|e| e.to_string())?;
            let mut line = match trim::of(&source) {
                Some(trim) => {
                    let rec = serde_json::from_str(&lv.log()).map_err(|e| e.to_string())?;
                    serde_json::to_string(&trim(&rec)).map_err(|e| e.to_string())?
                }
                None => lv.log().trim_end().to_string(),
            };
            if !line.ends_with('}') {
                return Err("record is not a JSON object".to_string());
            }
            line.pop();
            if line.trim_end() != "{" {
                line.push(',');
            }
            line.push_str("\"_norm\":");
            line.push_str(&added);
            line.push('}');
            line.push('\n');
            out.extend(line.into_bytes());
        }
        Ok(out)
    }
}
