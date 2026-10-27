//! Keeps what the `judge` inference node flagged, and what it could not judge
//! (an error where the verdict should be, so a person sees it). The selection
//! is the probe; the body passes records through unchanged. This is the whole
//! filter: the inference node only adds its verdict, a plugin decides.

wit_bindgen::generate!({
    path: ".tangent/wit",
    world: "processor",
    generate_all,
});

use exports::tangent::logs::mapper::{Guest, Meta, Pred, Selector};
use tangent::logs::log::{Logview, Scalar};

struct Component;

export!(Component);

fn only(pred: Pred) -> Selector {
    Selector {
        any: Vec::new(),
        all: vec![pred],
        none: Vec::new(),
    }
}

impl Guest for Component {
    fn metadata() -> Meta {
        Meta {
            name: "flagged".to_string(),
            version: "0.1.0".to_string(),
        }
    }

    // A record passes when any selector matches.
    fn probe() -> Vec<Selector> {
        vec![
            only(Pred::Eq(("judge.flag".to_string(), Scalar::Boolean(true)))),
            only(Pred::Has("judge.error".to_string())),
        ]
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
