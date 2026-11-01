use normkit::{obj, pick};
use serde_json::{Map, Value};

pub fn google_workspace(m: &Map<String, Value>) -> Map<String, Value> {
    let mut out = pick(m, &["ts", "source", "ipAddress", "ownerDomain"]);
    if let Some(id) = obj(m.get("id")) {
        out.insert(
            "id".into(),
            Value::Object(pick(id, &["time", "applicationName"])),
        );
    }
    if let Some(a) = obj(m.get("actor")) {
        out.insert(
            "actor".into(),
            Value::Object(pick(a, &["email", "callerType"])),
        );
    }
    if let Some(events) = m.get("events").and_then(Value::as_array) {
        let evs: Vec<Value> = events
            .iter()
            .filter_map(Value::as_object)
            .map(|e| {
                let mut ev = pick(e, &["type", "name"]);
                if let Some(params) = e.get("parameters").and_then(Value::as_array) {
                    let mut pm = Map::new();
                    for p in params.iter().filter_map(Value::as_object) {
                        let Some(name) = p
                            .get("name")
                            .and_then(Value::as_str)
                            .filter(|n| *n != "billable")
                        else {
                            continue;
                        };
                        let Some(v) = [
                            "value",
                            "boolValue",
                            "intValue",
                            "multiValue",
                            "multiIntValue",
                            "messageValue",
                        ]
                        .iter()
                        .find_map(|k| p.get(*k)) else {
                            continue;
                        };
                        pm.insert(name.to_string(), v.clone());
                    }
                    if !pm.is_empty() {
                        ev.insert("parameters".into(), Value::Object(pm));
                    }
                }
                Value::Object(ev)
            })
            .collect();
        out.insert("events".into(), Value::Array(evs));
    }
    out
}
