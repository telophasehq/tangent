//! Normalizes the corpus' eight source shapes into `_norm`: the fields the
//! window nodes key on (`actor`, `org`), count by (`kind`) and track for
//! first sightings (`ip`, `country`, `dest`, `target`), for the shapes that
//! are not a provider's API: a product's audit log, an agent's logs, CI
//! lines. The sources read straight from their APIs (CloudTrail, the GitHub
//! audit log, Okta, Google Workspace) have a plugin each under `providers/`,
//! which also trims the record; this one leaves the record as it came and
//! adds `_norm` beside it. Which field means what for each source is code
//! here. One thing is config: which sources are the
//! company's own infrastructure. A product's logs carry a customer's tenant
//! id, and that is their partition: one per customer. The company's own
//! logs (its cloud accounts, its GitHub org, its identity provider, its CI)
//! have no customer in them; they are one tenant, the company, and all land
//! in the one partition named for it, whatever ids each source carries.
//!
//! From `profile.yaml` in the plugin's data directory (`org`, `own_sources`),
//! or `plugins.norm.config` (`own_org`, `own_sources`):
//!   org: acme
//!   own_sources: [cloudtrail, github_audit, okta, google_workspace, gha_ci, runner_egress]

wit_bindgen::generate!({
    path: ".tangent/wit",
    world: "processor",
    generate_all,
});

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
                .map(|a| a.iter().filter_map(|v| v.as_str().map(str::to_string)).collect())
                .unwrap_or_default();
            return (org, sources);
        }
        let org = config::get("own_org").map(|s| s.trim().trim_matches('"').to_string()).filter(|s| !s.is_empty());
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
    rest.split(['/', '?', ':']).next().unwrap_or(rest).to_string()
}

/// `acme/web` -> `acme`
fn org_of_repo(repo: &str) -> Option<String> {
    repo.split('/').next().map(str::to_string)
}

fn norm(lv: &Logview) -> Norm {
    let source = get(lv, "source").unwrap_or_default();
    let mut n = Norm::default();
    match source.as_str() {
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
        if own_sources.contains(source.as_str()) {
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
        // Every record with a `source`, except the providers' own plugins'.
        let providers = ["cloudtrail", "github_audit", "okta", "google_workspace"];
        vec![Selector {
            any: Vec::new(),
            all: vec![Pred::Has("source".to_string())],
            none: providers
                .iter()
                .map(|p| Pred::Eq(("source".to_string(), Scalar::Str((*p).to_string()))))
                .collect(),
        }]
    }

    fn process_logs(input: Vec<Logview>) -> Result<Vec<u8>, String> {
        let mut out = Vec::new();
        for lv in input {
            let n = norm(&lv);
            let added = serde_json::to_string(&n).map_err(|e| e.to_string())?;
            let mut line = lv.log().trim_end().to_string();
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
