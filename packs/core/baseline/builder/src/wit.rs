//! The plugin: the runtime's state as a [`Kv`], and the profile's `people`.

wit_bindgen::generate!({
    path: ".tangent/wit",
    world: "processor",
    generate_all,
});

use std::sync::OnceLock;

use baselinekit::{Kv, Scalar, R};
use exports::tangent::logs::mapper::{Guest, Meta, Selector};
use identitykit::People;
use serde_json::Value;
use tangent::logs::enrichment::lookup;
use tangent::logs::log::{self, Logview};
use tangent::logs::state;

struct Component;

export!(Component);

impl Guest for Component {
    fn metadata() -> Meta {
        Meta {
            name: "baseline_builder".to_string(),
            version: "0.1.0".to_string(),
        }
    }

    // Everything: a record with no actor is passed over.
    fn probe() -> Vec<Selector> {
        vec![Selector {
            any: Vec::new(),
            all: Vec::new(),
            none: Vec::new(),
        }]
    }

    fn process_logs(input: Vec<Logview>) -> Result<Vec<u8>, String> {
        let lines: Vec<String> = input.iter().map(Logview::log).collect();
        crate::build(&mut Wit, people(), &lines)?;
        Ok(Vec::new())
    }
}

/// Read once per instance from the `profile` table. None there, no one has
/// another name.
fn people() -> &'static People {
    static P: OnceLock<People> = OnceLock::new();
    P.get_or_init(|| {
        People::from_value(
            &lookup("profile", "people")
                .and_then(|json| serde_json::from_str::<Value>(&json).ok())
                .unwrap_or(Value::Null),
        )
    })
}

struct Wit;

fn ours(v: log::Scalar) -> Option<Scalar> {
    match v {
        log::Scalar::Int(i) => Some(Scalar::Int(i)),
        log::Scalar::Str(s) => Some(Scalar::Str(s)),
        _ => None,
    }
}

impl Kv for Wit {
    fn get(&mut self, key: &str) -> R<Option<Scalar>> {
        Ok(state::get(key)?.and_then(ours))
    }
    fn set(&mut self, key: &str, v: Scalar) -> R<()> {
        let v = match v {
            Scalar::Int(i) => log::Scalar::Int(i),
            Scalar::Str(s) => log::Scalar::Str(s),
        };
        state::set(key, &v, Some(0))
    }
    fn incr(&mut self, key: &str, by: i64) -> R<()> {
        state::incr(key, by, Some(0)).map(|_| ())
    }
    fn scan(&mut self, lo: &str, hi: &str) -> R<Vec<(String, Scalar)>> {
        let rows = state::scan(lo, hi, 100_000)?;
        Ok(rows
            .into_iter()
            .filter_map(|(k, v)| Some((k, ours(v)?)))
            .collect())
    }
}
