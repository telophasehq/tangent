//! Your product's NDJSON: its audit log, one org per customer, and an AI
//! agent's run log; each record into `_norm`, less its per-event ids and URL
//! signatures, its address described from the runtime's tables, and what the
//! company profile says about it in `_facts`. `config:` names the sources of
//! each shape: `sources` (default `[app]`) plus `extra_sources`, and
//! `agent_sources` (default `[agent]`).

wit_bindgen::generate!({
    path: ".tangent/wit",
    world: "processor",
    generate_all,
});

use std::sync::OnceLock;

use exports::tangent::logs::mapper::{Guest, Meta, Pred, Selector};
use normkit::Norm;
use serde_json::{Map, Value};
use tangent::logs::config;
use tangent::logs::enrichment::lookup;
use tangent::logs::log::{Logview, Scalar};

struct Component;

export!(Component);

struct Sources {
    app: Vec<String>,
    agent: Vec<String>,
}

fn sources() -> &'static Sources {
    static S: OnceLock<Sources> = OnceLock::new();
    S.get_or_init(|| {
        let listed = |key: &str| config::get(key).map(|raw| normkit::list(&raw));
        let mut app = listed("sources").unwrap_or_else(|| vec!["app".to_string()]);
        app.extend(listed("extra_sources").unwrap_or_default());
        let agent = listed("agent_sources").unwrap_or_else(|| vec!["agent".to_string()]);
        Sources { app, agent }
    })
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

/// its URLs unsigned, as an untrimmed record's are
fn get(lv: &Logview, path: &str) -> Option<String> {
    let s = lv.get(path).and_then(text)?;
    Some(normkit::unsigned(&s).unwrap_or(s))
}

fn norm(lv: &Logview, agent: bool) -> Norm {
    let mut n = Norm::default();
    if agent {
        // An agent's lines are judged per organization; a plan id is new every time.
        n.principal = Some("none");
        n.org = get(lv, "organization_id");
        n.kind = get(lv, "event");
        n.ip = get(lv, "client_ip");
        n.dest = get(lv, "destination").map(|d| normkit::host_of(&d));
        n.target = match n.kind.as_deref() {
            Some("secret_read") => get(lv, "path"),
            Some("tool_result") => get(lv, "tool"),
            Some("aws_api_call") => get(lv, "api"),
            _ => None,
        };
    } else {
        n.actor = get(lv, "actor");
        n.org = get(lv, "org");
        n.kind = get(lv, "event");
        n.ip = get(lv, "ip");
        n.target = get(lv, "target").or_else(|| get(lv, "key_id"));
    }
    n
}

/// Datadog's own bookkeeping, and its copies of the service and env.
fn datadog(m: &mut Map<String, Value>) {
    m.remove("dd.version");
    m.remove("ddsource");
    for (dd, k) in [("dd.service", "service"), ("dd.env", "env")] {
        if m.get(dd) == m.get(k) {
            m.remove(dd);
        }
    }
}

impl Guest for Component {
    fn metadata() -> Meta {
        Meta {
            name: "app".to_string(),
            version: "0.1.0".to_string(),
        }
    }

    fn probe() -> Vec<Selector> {
        let s = sources();
        let names = s.app.iter().chain(&s.agent);
        vec![Selector {
            any: Vec::new(),
            all: vec![Pred::In((
                "source".to_string(),
                names.map(|n| Scalar::Str(n.clone())).collect(),
            ))],
            none: Vec::new(),
        }]
    }

    fn process_logs(input: Vec<Logview>) -> Result<Vec<u8>, String> {
        let kit = factskit::kit(lookup);
        let mut out = Vec::new();
        for lv in input {
            let source = get(&lv, "source").unwrap_or_default();
            let agent = sources().agent.contains(&source);
            let n = norm(&lv, agent);
            let rec = normkit::scrubbed(&lv.log(), |m| {
                if agent {
                    datadog(m);
                }
            })?;
            kit.write(&mut out, rec, n)?;
        }
        Ok(out)
    }
}
