//! The app pack's audit shape: `app`'s, and any other source's with an `event`.

use serde_json::Value;

use super::{auth, clean, login, quoted, s, str_of, text, Ev};

pub(super) fn read(r: &Value, e: &mut Ev) {
    let ev = s(&r["event"]);
    let method = s(&r["method"]);
    e.verb = if ev == "api_request" {
        format!("api_request {}", clean(method, 20))
            .trim()
            .to_string()
    } else {
        ev.to_string()
    };
    e.ip = str_of(&r["ip"]);
    let status = text(&r["status"])
        .and_then(|x| x.parse::<i64>().ok())
        .unwrap_or(200);
    e.ok = r["result"].as_str().unwrap_or("success") == "success" && status < 400;
    let path: Vec<&str> = s(&r["path"])
        .split('?')
        .next()
        .unwrap_or("")
        .split('/')
        .map(|p| {
            if !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit()) {
                "N"
            } else {
                p
            }
        })
        .collect();
    let path = path.join("/");
    if !path.is_empty() {
        e.res.push(format!("path:{}", quoted(&path)));
    }
    let key = str_of(&r["key_id"]);
    if let Some(k) = &key {
        e.res.push(format!("key:{k}"));
    }
    e.typ = Some(
        if e.principal.as_deref() == Some("key") || ev == "api_request" {
            "app api key"
        } else {
            "app user"
        }
        .into(),
    );
    if ev == "auth.login" && !method.is_empty() {
        e.client = Some(format!("login {}", clean(method, 20)));
    }
    e.cat = if ev.starts_with("auth.") {
        'a'
    } else if ev.starts_with("api_key.") {
        'c'
    } else if ev.starts_with("member.") || ev.starts_with("org.settings") {
        'i'
    } else if ev.ends_with(".revoke") {
        'd'
    } else if matches!(ev, "page.view" | "org.export.started") || method == "GET" {
        'r'
    } else {
        'w'
    };
    e.auth = match ev {
        "auth.login" => Some(auth(
            login(e.ok),
            vec![],
            format!("app {}", clean(method, 20)).trim().into(),
            r["mfa"].as_bool(),
        )),
        "api_key.create" => {
            let sc = match r.get("scopes").or_else(|| r.get("scope")) {
                Some(Value::Array(a)) => a
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join(","),
                Some(v) => s(v).to_string(),
                None => String::new(),
            };
            let k = key.clone().unwrap_or_default();
            Some(auth(
                "key created",
                key.into_iter().collect(),
                format!("{k} scopes {}", clean(&sc, 40)),
                None,
            ))
        }
        "member.role.changed" => str_of(&r["target"]).map(|t| {
            let what = format!("role {} -> {}", s(&r["from_role"]), s(&r["to_role"]));
            auth("grant", vec![t], what, None)
        }),
        _ => None,
    };
}
