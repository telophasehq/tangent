//! Okta's System Log: each event into `_norm`, trimmed to what a verdict turns
//! on (Okta's own risk signals kept), its address described from the
//! runtime's tables, and what the company profile says about it in `_facts`.

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

fn norm(lv: &Logview) -> Norm {
    Norm {
        actor: get(lv, "actor.alternateId"),
        kind: get(lv, "eventType"),
        ip: get(lv, "client.ipAddress"),
        country: get(lv, "client.geographicalContext.country"),
        target: get(lv, "target[0].displayName"),
        ..Norm::default()
    }
}

impl Guest for Component {
    fn metadata() -> Meta {
        Meta {
            name: "okta".to_string(),
            version: "0.1.0".to_string(),
        }
    }

    fn probe() -> Vec<Selector> {
        vec![Selector {
            any: Vec::new(),
            all: vec![Pred::In((
                "source".to_string(),
                vec![Scalar::Str("okta".to_string())],
            ))],
            none: Vec::new(),
        }]
    }

    fn process_logs(input: Vec<Logview>) -> Result<Vec<u8>, String> {
        let kit = factskit::kit(lookup);
        let mut out = Vec::new();
        for lv in input {
            let n = norm(&lv);
            let rec = trim::okta(&normkit::record(&lv.log())?);
            kit.write(&mut out, rec, n)?;
        }
        Ok(out)
    }
}
