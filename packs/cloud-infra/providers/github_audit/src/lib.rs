//! GitHub organization audit log records: `_norm` (the actor, the org, the
//! action, the address and country when the enterprise discloses them, the
//! repository) and the record trimmed to what it means. Every `*_id`, the
//! hashed token, transport details and run ids go.
wit_bindgen::generate!({
    path: ".tangent/wit",
    world: "processor",
    generate_all,
});

use std::sync::OnceLock;

use common::{carry_attached, emit, get, org_from_profile, pick, Norm};
use exports::tangent::logs::mapper::{Guest, Meta, Pred, Selector};
use serde_json::{Map, Value};
use tangent::logs::config;
use tangent::logs::log::{Logview, Scalar};

struct Component;

export!(Component);

const SOURCE: &str = "github_audit";

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


fn norm_of(r: &Value) -> Norm {
    Norm {
        actor: get(r, "actor"),
        org: get(r, "org"),
        kind: get(r, "action"),
        ip: get(r, "actor_ip"),
        country: get(r, "actor_location.country_code"),
        dest: None,
        target: get(r, "repo"),
    }
}

fn trim(m: &Map<String, Value>) -> Map<String, Value> {
    pick(m, &["ts", "source", "action", "actor", "actor_is_bot", "actor_ip", "actor_location", "org", "repo", "user", "team", "visibility", "public_repo", "programmatic_access_type", "token_scopes", "integration", "operation_type", "permission", "old_permission", "name", "head_branch", "event", "conclusion", "runner_name", "runner_type", "pull_request_title", "pull_request_url", "topic", "ref", "key", "fingerprint", "read_only", "hook_id", "config", "target_login", "reason", "explanation"])
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
