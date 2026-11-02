//! Google Workspace's activity reports.

use serde_json::Value;

use super::{at, auth, clean, quoted, s, str_of, text, Ev, NULL};

pub(super) fn read(r: &Value, e: &mut Ev) {
    let app = s(at(r, "id.applicationName"));
    let evs = r["events"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default();
    let names: Vec<&str> = evs.iter().map(|x| s(&x["name"])).collect();
    let param = |k: &str| {
        evs.iter()
            .filter_map(|x| x["parameters"].get(k))
            .find(|v| !v.is_null())
            .unwrap_or(&NULL)
    };
    e.verb = format!("{app}:{}", names.join(","));
    e.ip = str_of(&r["ipAddress"]);
    e.typ = Some(format!(
        "workspace {}",
        s(at(r, "actor.callerType")).to_lowercase()
    ));
    e.cat = if matches!(app, "login" | "token") {
        'a'
    } else if app == "user_accounts" {
        'c'
    } else if matches!(app, "admin" | "groups_enterprise")
        || evs.iter().any(|x| x["type"] == "acl_change")
    {
        'i'
    } else if names.iter().any(|n| matches!(*n, "delete" | "trash")) {
        'd'
    } else if evs.iter().any(|x| x["type"] == "access") || (app == "drive" && names == ["download"])
    {
        'r'
    } else {
        'w'
    };
    e.res = str_of(param("doc_title"))
        .map(|t| format!("doc:{}", quoted(&t)))
        .into_iter()
        .collect();
    if let Some(id) = str_of(param("client_id")) {
        e.oauth.push((id, str_of(param("app_name"))));
    } else if let Some(id) = text(param("originating_app_id")) {
        e.oauth.push((id, None));
    }
    let challenge = param("login_challenge_method");
    let strong = challenge
        .as_array()
        .map(|a| a.iter().filter_map(Value::as_str).collect::<Vec<_>>())
        .unwrap_or_else(|| challenge.as_str().into_iter().collect())
        .iter()
        .any(|m| !matches!(m.to_ascii_lowercase().as_str(), "password" | "none" | ""));
    let second = param("is_second_factor") == true;
    // an admin resetting someone's password or second factor
    let user = str_of(param("USER_EMAIL")).filter(|_| app == "admin");
    let named = |ws: &[&str]| names.iter().any(|n| ws.iter().any(|w| n.contains(w)));
    let reset = if named(&["PASSWORD"]) {
        Some("password change")
    } else if named(&[
        "2SV",
        "2_STEP",
        "STRONG_AUTH",
        "SECURITY_KEY",
        "BACKUP_VERIFICATION",
    ]) {
        Some("mfa change")
    } else {
        None
    };
    e.auth = if let (Some(kind), Some(u)) = (reset, user) {
        Some(auth(kind, vec![u], clean(&names.join(","), 60), None))
    } else if names.contains(&"login_success") {
        Some(auth(
            "login",
            vec![],
            "workspace".into(),
            (strong || second).then_some(true),
        ))
    } else if names.contains(&"login_failure") {
        Some(auth("login failed", vec![], "workspace".into(), None))
    } else if names.iter().any(|n| n.starts_with("2sv_")) {
        Some(auth("mfa change", vec![], names.join(","), None))
    } else if names.contains(&"password_edit") {
        Some(auth("password change", vec![], "workspace".into(), None))
    } else if names.contains(&"authorize") {
        let n = param("scope").as_array().map_or(0, Vec::len);
        let app = str_of(param("app_name")).unwrap_or_else(|| "?".into());
        Some(auth(
            "oauth grant",
            vec![],
            format!("{} ({n} scopes)", quoted(&app)),
            None,
        ))
    } else {
        None
    };
}
