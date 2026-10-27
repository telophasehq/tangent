//! Keeps the records the `first_pass` inference node answered "yes" about, and
//! the ones it could not answer (an error in place of the word). The selection
//! is the probe; the body passes records through unchanged.

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
            name: "keep_yes".to_string(),
            version: "0.1.0".to_string(),
        }
    }

    fn probe() -> Vec<Selector> {
        vec![
            only(Pred::Eq((
                "_first_pass.answer".to_string(),
                Scalar::Str("yes".to_string()),
            ))),
            only(Pred::Has("_first_pass.error".to_string())),
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
