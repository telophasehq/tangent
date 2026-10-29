//! Keeps what the judge should read: the records the `store` sink marked
//! `_novel` (a value one of their scopes has not had within its retention),
//! and the ones the profile says to see every time, however usual. AWS
//! acting on its own is left out. Put before an inference node, the model
//! reads only those, with their history; what an actor or the org does every
//! day is kept, not judged.
//!
//! `_novel` only selects. It is taken off what passes, so the model judges a
//! record on what it is and its history, not on being new.

wit_bindgen::generate!({
    path: ".tangent/wit",
    world: "processor",
    generate_all,
});

use exports::tangent::logs::mapper::{Guest, Meta, Pred, Selector};
use serde_json::Value;
use tangent::logs::log::{Logview, Scalar};

struct Component;

export!(Component);

fn is(path: &str, value: Scalar) -> Pred {
    Pred::Eq((path.to_string(), value))
}

/// The record without `_novel`. A line with no mark, or that is not a JSON
/// object, goes on as it came.
fn without_novel(line: String) -> String {
    let Ok(Value::Object(mut rec)) = serde_json::from_str::<Value>(&line) else {
        return line;
    };
    match rec.remove("_novel") {
        Some(_) => Value::Object(rec).to_string(),
        None => line,
    }
}

impl Guest for Component {
    fn metadata() -> Meta {
        Meta {
            name: "novel".to_string(),
            version: "0.1.0".to_string(),
        }
    }

    // A record passes when any of these holds, unless AWS is acting on its own.
    fn probe() -> Vec<Selector> {
        vec![Selector {
            any: vec![
                Pred::Has("_novel".to_string()),
                // What the profile says to see every time. Not `dest_known:
                // false`: a profile that lists no hosts gives it to every
                // runner egress record.
                is("_facts.watch", Scalar::Boolean(true)),
                is("_facts.root", Scalar::Boolean(true)),
                is("_facts.world_open", Scalar::Boolean(true)),
                is("_facts.sensitive_target", Scalar::Boolean(true)),
                is("_facts.key_known_network", Scalar::Boolean(false)),
            ],
            all: Vec::new(),
            // A service's own calls (`aws:<service>`): many, and nobody's
            // access but AWS's.
            none: vec![is("_facts.principal", Scalar::Str("aws".to_string()))],
        }]
    }

    fn process_logs(input: Vec<Logview>) -> Result<Vec<u8>, String> {
        let mut out = Vec::new();
        for lv in input {
            out.extend(without_novel(lv.log()).into_bytes());
            out.push(b'\n');
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::without_novel;

    #[test]
    fn the_mark_is_taken_off_and_nothing_else() {
        let line = r#"{"_facts":{"root":true},"_novel":{"actor":["_norm.kind"]},"ts":"2026-10-28T13:00:00Z"}"#;
        assert_eq!(
            without_novel(line.into()),
            r#"{"_facts":{"root":true},"ts":"2026-10-28T13:00:00Z"}"#
        );
    }

    #[test]
    fn a_line_without_a_mark_or_not_json_goes_on_as_it_came() {
        for line in [
            r#"{"ts": "2026-10-28T13:00:00Z", "a": 1}"#,
            "not json",
            "[1,2]",
        ] {
            assert_eq!(without_novel(line.into()), line);
        }
    }
}
