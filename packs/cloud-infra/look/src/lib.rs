//! Keeps what the `screen` inference node marked worth a look, and what it
//! could not read (an error where the answer should be: the judge sees it
//! rather than nobody). The selection is the probe; the body passes records
//! through unchanged. The inference node only adds its answer, a plugin
//! decides.

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
            name: "look".to_string(),
            version: "0.1.0".to_string(),
        }
    }

    // A record passes when any selector matches.
    fn probe() -> Vec<Selector> {
        vec![
            only(Pred::Eq(("screen.look".to_string(), Scalar::Boolean(true)))),
            only(Pred::Has("screen.error".to_string())),
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
