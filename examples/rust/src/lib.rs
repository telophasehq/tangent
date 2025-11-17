use serde::Serialize;

wit_bindgen::generate!({
    path: ".tangent/wit",
    world: "processor",
});

struct Component;

export!(Component);

#[derive(Default, Serialize)]
struct ExampleOutput {
    message: String,
    level: String,
    seen: i64,
    duration: f64,
    service: String,
    tags: Option<Vec<String>>,
}

fn string_from_scalar(s: log::Scalar) -> Option<String> {
    match s {
        log::Scalar::Str(v) => Some(v),
        _ => None,
    }
}

fn int_from_scalar(s: log::Scalar) -> Option<i64> {
    match s {
        log::Scalar::Int(v) => Some(v),
        _ => None,
    }
}

fn float_from_scalar(s: log::Scalar) -> Option<f64> {
    match s {
        log::Scalar::Float(v) => Some(v),
        _ => None,
    }
}

impl exports::tangent::logs::mapper::Guest for Component {
    fn metadata() -> exports::tangent::logs::mapper::Meta {
        exports::tangent::logs::mapper::Meta {
            name: "rust".to_string(),
            version: "0.1.0".to_string(),
        }
    }

    fn probe() -> Vec<exports::tangent::logs::mapper::Selector> {
        use exports::tangent::logs::mapper as mapper;

        vec![mapper::Selector {
            any: Vec::new(),
            all: vec![mapper::Pred::Eq((
                "source.name".to_string(),
                log::Scalar::Str("myservice".to_string()),
            ))],
            none: Vec::new(),
        }]
    }

    fn process_logs(
        input: Vec<exports::tangent::logs::log::Logview>,
    ) -> Result<Vec<u8>, String> {
        let mut buf = Vec::new();

        for lv in input {
            let mut out = ExampleOutput::default();

            if let Some(val) = lv.get("msg").and_then(string_from_scalar) {
                out.message = val;
            }

            if let Some(val) = lv.get("msg.level").and_then(string_from_scalar) {
                out.level = val;
            }

            if let Some(val) = lv.get("seen").and_then(int_from_scalar) {
                out.seen = val;
            }

            if let Some(val) = lv.get("duration").and_then(float_from_scalar) {
                out.duration = val;
            }

            if let Some(val) = lv.get("source.name").and_then(string_from_scalar) {
                out.service = val;
            }

            if let Some(items) = lv.get_list("tags") {
                let mut tags = Vec::with_capacity(items.len());
                for item in items {
                    if let log::Scalar::Str(val) = item {
                        tags.push(val);
                    }
                }
                if !tags.is_empty() {
                    out.tags = Some(tags);
                }
            }

            let json_line = serde_json::to_vec(&out).map_err(|e| e.to_string())?;
            buf.extend(json_line);
            buf.push(b'\n');
        }

        Ok(buf)
    }
}
