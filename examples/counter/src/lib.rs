//! Per-org counters in plugin state, right however many workers run it.
//!
//! The org is part of every key: `count:<org>` is "events from this org" and
//! `seen:<org>:<event>` is "has this org done this before". The count is one
//! `incr`, atomic on its own; "seen before?" reads a key and writes it back,
//! so the two go in one transaction, or two workers could both see "first".

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

/// Whether `key` is new, marking it seen: a read and the write that depends
/// on it, in one transaction.
fn first_time(key: &str) -> Result<bool, String> {
    state::begin()?;
    let first = match state::get(key) {
        Ok(v) => v.is_none(),
        Err(e) => {
            state::rollback();
            return Err(e);
        }
    };
    if first {
        if let Err(e) = state::set(key, &Scalar::Boolean(true), None) {
            state::rollback();
            return Err(e);
        }
    }
    state::commit()?;
    Ok(first)
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
            let org_id = lv
                .get("org_id")
                .and_then(string_from_scalar)
                .unwrap_or_default();
            let event = lv
                .get("event")
                .and_then(string_from_scalar)
                .unwrap_or_default();

            let n = state::incr(&format!("count:{org_id}"), 1, None)?;
            let first = first_time(&format!("seen:{org_id}:{event}"))?;

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
