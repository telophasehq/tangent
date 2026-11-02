//! GitHub's audit log.

use serde_json::Value;

use super::{auth, clean, s, str_of, text, Ev};

const GH_CREDS: &[&str] = &[
    "public_key.",
    "personal_access_token",
    "oauth_authorization",
    "org_credential_authorization",
];
const GH_IAM: &[&str] = &[
    "org.update_member",
    "org.add_member",
    "org.remove_member",
    "team.add_member",
    "repo.add_member",
    "deploy_key.",
    "protected_branch.",
    "repo.access",
    "org.self_hosted_runner",
    "oauth_application",
    "integration_installation",
];

pub(super) fn read(r: &Value, e: &mut Ev) {
    let a = s(&r["action"]);
    e.verb = a.to_string();
    e.ip = str_of(&r["actor_ip"]);
    e.res = str_of(&r["repo"])
        .map(|x| format!("repo:{}", clean(&x, 80)))
        .into_iter()
        .collect();
    e.client = Some(str_of(&r["programmatic_access_type"]).unwrap_or_else(|| "web".into()));
    e.typ = Some(
        if s(&r["actor"]).ends_with("[bot]") || r["actor_is_bot"] == true {
            "github bot"
        } else {
            "github user"
        }
        .into(),
    );
    e.cat = if GH_CREDS.iter().any(|p| a.starts_with(p)) {
        'c'
    } else if GH_IAM.iter().any(|p| a.starts_with(p)) {
        'i'
    } else {
        match s(&r["operation_type"]) {
            "access" => 'r',
            "remove" => 'd',
            "create" | "modify" | "transfer" | "restore" => 'w',
            "authentication" => 'a',
            _ if matches!(a, "git.clone" | "git.fetch") => 'r',
            _ if [".destroy", ".delete", ".remove"]
                .iter()
                .any(|x| a.ends_with(x)) =>
            {
                'd'
            }
            _ => 'w',
        }
    };
    if let Some(i) = str_of(&r["integration"]) {
        e.oauth.push((i.clone(), Some(i)));
    }
    e.token = text(&r["token_id"]).or_else(|| str_of(&r["hashed_token"]));
    e.auth = if matches!(
        a,
        "deploy_key.create"
            | "public_key.create"
            | "personal_access_token.request_created"
            | "personal_access_token.access_granted"
    ) {
        Some(auth("key created", vec![], a.into(), None))
    } else if [
        "oauth_authorization.create",
        "integration_installation.create",
        "org_credential_authorization.grant",
    ]
    .iter()
    .any(|p| a.starts_with(p))
    {
        Some(auth("oauth grant", vec![], a.into(), None))
    } else if matches!(
        a,
        "org.update_member" | "org.add_member" | "team.add_member"
    ) {
        str_of(&r["user"]).map(|u| {
            auth(
                "grant",
                vec![u],
                format!("{a} {}", s(&r["permission"])).trim().into(),
                None,
            )
        })
    } else {
        None
    };
}
