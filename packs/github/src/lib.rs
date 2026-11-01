//! GitHub: the audit log, trimmed, and a CI run's job logs and runner egress
//! (NDJSON), told per repository; each record into `_norm`, its address
//! described from the runtime's tables, and what the company profile says
//! about it in `_facts`.

wit_bindgen::generate!({
    path: ".tangent/wit",
    world: "processor",
    generate_all,
});

mod trim;

use exports::tangent::logs::mapper::{Guest, Meta, Pred, Selector};
use normkit::Norm;
use tangent::logs::enrichment::lookup;
use tangent::logs::log::{Logview, Scalar};

struct Component;

export!(Component);

const SOURCES: &[&str] = &["github_audit", "gha_ci", "runner_egress"];

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

/// `acme/web` -> `acme`
fn org_of_repo(repo: &str) -> Option<String> {
    repo.split('/').next().map(str::to_string)
}

fn norm(lv: &Logview, source: &str) -> Norm {
    let mut n = Norm::default();
    match source {
        "github_audit" => {
            n.actor = get(lv, "actor");
            n.org = get(lv, "org");
            n.kind = get(lv, "action");
            n.ip = get(lv, "actor_ip");
            n.country = get(lv, "actor_location.country_code");
            n.target = get(lv, "repo");
        }
        // A CI run's story is told per repository: a run id is new every time.
        "gha_ci" => {
            n.actor = get(lv, "repo");
            n.principal = Some("none");
            n.org = get(lv, "repo").and_then(|r| org_of_repo(&r));
            n.kind = get(lv, "step");
            n.target = get(lv, "repo");
        }
        "runner_egress" => {
            n.actor = get(lv, "repo");
            n.principal = Some("none");
            n.org = get(lv, "repo").and_then(|r| org_of_repo(&r));
            n.kind = Some("egress".to_string());
            n.dest = get(lv, "dst_host");
            n.target = get(lv, "repo");
        }
        _ => {}
    }
    n
}

impl Guest for Component {
    fn metadata() -> Meta {
        Meta {
            name: "github".to_string(),
            version: "0.1.0".to_string(),
        }
    }

    fn probe() -> Vec<Selector> {
        vec![Selector {
            any: Vec::new(),
            all: vec![Pred::In((
                "source".to_string(),
                SOURCES.iter().map(|s| Scalar::Str(s.to_string())).collect(),
            ))],
            none: Vec::new(),
        }]
    }

    fn process_logs(input: Vec<Logview>) -> Result<Vec<u8>, String> {
        let kit = factskit::kit(lookup);
        let mut out = Vec::new();
        for lv in input {
            let source = get(&lv, "source").unwrap_or_default();
            let n = norm(&lv, &source);
            let rec = if source == "github_audit" {
                trim::github_audit(&normkit::record(&lv.log())?)
            } else {
                normkit::scrubbed(&lv.log(), |_| {})?
            };
            kit.write(&mut out, rec, n)?;
        }
        Ok(out)
    }
}
