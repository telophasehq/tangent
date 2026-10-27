//! Per-tenant counters with no tenant handling in the plugin.
//!
//! The config partitions the stream by `org_id`, so the runtime hands this
//! plugin one org's records at a time and scopes every `state` call to that
//! org. `count` is therefore "events from this org" and `seen:<event>` is
//! "has this org done this before", with no org anywhere in the keys.

use serde::Serialize;

wit_bindgen::generate!({
    path: ".tangent/wit",
    world: "processor",
    generate_all,
});

use exports::tangent::logs::mapper::{Guest, Meta, Pred, Selector};
use tangent::logs::log::{Logview, Scalar};
use tangent::logs::state;

struct Component;

export!(Component);

#[derive(Serialize)]
struct Out {
    org_id: String,
    event: String,
    /// Events seen from this org, including this one.
    n: i64,
    /// First time this org produced this event.
    first: bool,
}

fn string_from_scalar(s: Scalar) -> Option<String> {
    match s {
        Scalar::Str(v) => Some(v),
        _ => None,
    }
}

impl Guest for Component {
    fn metadata() -> Meta {
        Meta {
            name: "counter".to_string(),
            version: "0.1.0".to_string(),
        }
    }

    fn probe() -> Vec<Selector> {
        vec![Selector {
            any: Vec::new(),
            all: vec![Pred::Has("org_id".to_string())],
            none: Vec::new(),
        }]
    }

    fn process_logs(input: Vec<Logview>) -> Result<Vec<u8>, String> {
        let mut buf = Vec::new();

        for lv in input {
            let org_id = lv.get("org_id").and_then(string_from_scalar).unwrap_or_default();
            let event = lv.get("event").and_then(string_from_scalar).unwrap_or_default();

            let n = state::incr("count", 1, None)?;

            let seen_key = format!("seen:{event}");
            let first = state::get(&seen_key)?.is_none();
            if first {
                state::set(&seen_key, &Scalar::Boolean(true), None)?;
            }

            let out = Out {
                org_id,
                event,
                n,
                first,
            };
            let json_line = serde_json::to_vec(&out).map_err(|e| e.to_string())?;
            buf.extend(json_line);
            buf.push(b'\n');
        }

        Ok(buf)
    }
}
