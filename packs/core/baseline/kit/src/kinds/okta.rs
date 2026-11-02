//! Okta's System Log.

use serde_json::Value;

use super::{at, auth, clean, login, s, str_of, Ev};

const OKTA_I: &[&str] = &[
    "user.lifecycle.",
    "group.user_membership.",
    "group.privilege.",
    "user.account.privilege.",
    "system.api_token.",
    "policy.lifecycle.",
    "policy.rule.",
    "application.user_membership.",
    "application.lifecycle.",
];

pub(super) fn read(r: &Value, e: &mut Ev) {
    let t = s(&r["eventType"]);
    let c = &r["client"];
    e.verb = t.to_string();
    e.ip = str_of(&c["ipAddress"]);
    e.typ = Some(format!("okta {}", s(at(r, "actor.type")).to_lowercase()));
    e.ok = matches!(
        at(r, "outcome.result").as_str().unwrap_or("SUCCESS"),
        "SUCCESS" | "ALLOW"
    );
    let ua: Vec<&str> = [
        at(c, "userAgent.browser"),
        at(c, "userAgent.os"),
        &c["device"],
    ]
    .into_iter()
    .filter_map(Value::as_str)
    .collect();
    e.client = (!ua.is_empty()).then(|| format!("{} (browser)", clean(&ua.join(" "), 60)));
    e.device = str_of(at(r, "device.id"));
    let targets = r["target"]
        .as_array()
        .map(Vec::as_slice)
        .unwrap_or_default();
    let of = |kind: &str, f: &str| -> Vec<String> {
        targets
            .iter()
            .filter(|x| x["type"] == kind)
            .filter_map(|x| str_of(&x[f]))
            .collect()
    };
    let apps = of("AppInstance", "displayName");
    let users = of("User", "alternateId");
    e.res = apps
        .iter()
        .map(|a| format!("app:{}", clean(a, 60)))
        .collect();
    let own = users.iter().all(|u| u.eq_ignore_ascii_case(&e.actor));
    e.cat = if [
        "user.session.",
        "user.authentication.",
        "policy.evaluate_sign_on",
        "app.oauth2.",
    ]
    .iter()
    .any(|p| t.starts_with(p))
    {
        'a'
    } else if ["user.mfa.", "user.account.", "user.credential."]
        .iter()
        .any(|p| t.starts_with(p))
        && !t.starts_with("user.account.privilege.")
        && own
    {
        'c'
    } else if ["user.mfa.", "user.account.", "user.credential."]
        .iter()
        .any(|p| t.starts_with(p))
        || OKTA_I.iter().any(|p| t.starts_with(p))
    {
        'i'
    } else if t.ends_with(".delete") {
        'd'
    } else {
        'w'
    };
    e.auth = match t {
        "user.session.start" | "user.authentication.sso" => {
            let what = apps
                .first()
                .map_or("okta".to_string(), |a| format!("okta sso {}", clean(a, 40)));
            Some(auth(login(e.ok), vec![], what, None))
        }
        "user.authentication.auth_via_mfa" if e.ok => {
            Some(auth("mfa ok", vec![], "okta mfa".into(), Some(true)))
        }
        "user.authentication.auth_via_mfa" | "user.authentication.verify" if !e.ok => {
            Some(auth("login failed", vec![], "okta".into(), None))
        }
        "user.account.update_password"
        | "user.account.reset_password"
        | "user.credential.forgot_password" => Some(auth("password change", users, t.into(), None)),
        "system.api_token.create" => {
            Some(auth("key created", vec![], "okta api token".into(), None))
        }
        "user.account.lock" => Some(auth("locked", users, t.into(), None)),
        "user.account.privilege.grant" => Some(auth("grant", users, t.into(), None)),
        _ if t.starts_with("user.mfa.") => Some(auth("mfa change", users, t.into(), None)),
        _ if t.starts_with("app.oauth2.") && t.contains("consent.grant") => {
            Some(auth("oauth grant", users, t.into(), None))
        }
        _ => None,
    };
    let beh = at(r, "debugData.behaviors")
        .as_str()
        .or_else(|| at(r, "debugContext.debugData.behaviors").as_str());
    if let (Some(_), Some(b)) = (&e.auth, beh) {
        e.okta_says = b
            .split(',')
            .filter(|x| x.contains("POSITIVE"))
            .map(|x| {
                clean(
                    x.split('=').next().unwrap_or("").trim_matches([' ', '{']),
                    40,
                )
            })
            .collect();
    }
}
