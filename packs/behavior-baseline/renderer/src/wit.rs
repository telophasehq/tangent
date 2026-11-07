//! The plugin: the runtime's state as a [`Kv`], read only.

wit_bindgen::generate!({
    path: ".tangent/wit",
    world: "processor",
    generate_all,
});

use baselinekit::{Kv, Scalar, R};
use exports::tangent::logs::mapper::{Guest, Meta, Selector};
use tangent::logs::log::{self, Logview};
use tangent::logs::state;

struct Component;

export!(Component);

impl Guest for Component {
    fn metadata() -> Meta {
        Meta {
            name: "baseline_renderer".to_string(),
            version: "0.1.0".to_string(),
        }
    }

    // Everything: records with no actor go on unchanged.
    fn probe() -> Vec<Selector> {
        vec![Selector {
            any: Vec::new(),
            all: Vec::new(),
            none: Vec::new(),
        }]
    }

    fn process_logs(input: Vec<Logview>) -> Result<Vec<u8>, String> {
        let lines: Vec<String> = input.iter().map(Logview::log).collect();
        crate::render(&mut Wit, &lines)
    }
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
    fn set(&mut self, _: &str, _: Scalar) -> R<()> {
        Err("baseline_renderer writes nothing".into())
    }
    fn incr(&mut self, _: &str, _: i64) -> R<()> {
        Err("baseline_renderer writes nothing".into())
    }
    fn scan(&mut self, lo: &str, hi: &str) -> R<Vec<(String, Scalar)>> {
        let rows = state::scan(lo, hi, 100_000)?;
        Ok(rows
            .into_iter()
            .filter_map(|(k, v)| Some((k, ours(v)?)))
            .collect())
    }
}
