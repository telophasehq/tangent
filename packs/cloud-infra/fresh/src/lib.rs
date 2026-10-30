//! Drops the records the sqlite source sent as `_context`, judged when they were new.

wit_bindgen::generate!({
    path: ".tangent/wit",
    world: "processor",
    generate_all,
});

use exports::tangent::logs::mapper::{Guest, Meta, Pred, Selector};
use tangent::logs::log::Logview;

struct Component;

export!(Component);

impl Guest for Component {
    fn metadata() -> Meta {
        Meta {
            name: "fresh".to_string(),
            version: "0.1.0".to_string(),
        }
    }

    fn probe() -> Vec<Selector> {
        vec![Selector {
            any: Vec::new(),
            all: Vec::new(),
            none: vec![Pred::Has("_context".to_string())],
        }]
    }

    fn process_logs(input: Vec<Logview>) -> Result<Vec<u8>, String> {
        let mut out = Vec::new();
        for lv in input {
            out.extend(lv.log().into_bytes());
            out.push(b'\n');
        }
        Ok(out)
    }
}
