//! Keeps what is new: the records the `store` sink marked `_novel`, a value
//! one of their scopes has not had within its retention. Put before an
//! inference node, the model reads only those, with their history; what an
//! actor or the org does every day is kept, not judged.

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
            name: "novel".to_string(),
            version: "0.1.0".to_string(),
        }
    }

    fn probe() -> Vec<Selector> {
        vec![Selector {
            any: Vec::new(),
            all: vec![Pred::Has("_novel".to_string())],
            none: Vec::new(),
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
