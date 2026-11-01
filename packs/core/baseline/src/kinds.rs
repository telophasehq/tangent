//! What baseline keeps of one record, per source: its verb, category,
//! resources, client, identity and auth event. Categories: r read, w write,
//! i iam/policy on someone or something else, d delete, a sign-in, c the
//! actor's own credentials.

use std::net::IpAddr;

use serde_json::Value;

pub struct Auth {
    /// login, login failed, mfa ok, denied, key created, mfa change, password change, grant, user created, oauth grant, locked
    pub kind: &'static str,
    pub subjects: Vec<String>,
    pub what: String,
    pub mfa: Option<bool>,
}

#[derive(Default)]
pub struct Ev {
    pub src: String,
    pub org: String,
    pub actor: String,
    pub person: String,
    pub ms: i64,
    pub context: bool,
    pub principal: Option<String>,
    pub verb: String,
    pub cat: char,
    pub ok: bool,
    pub res: Vec<String>,
    pub client: Option<String>,
    pub ip: Option<String>,
    pub asn: Option<String>,
    pub asorg: Option<String>,
    pub cc: Option<String>,
    pub typ: Option<String>,
    pub arn: String,
    /// (account, role, session) of an assumed-role caller
    pub session: Option<(String, String, String)>,
    pub issuer: Option<String>,
    pub mfa: Option<bool>,
    /// (account, role, session) this call assumed
    pub assume: Option<(String, String, String)>,
    pub auth: Option<Auth>,
    pub oauth: Vec<(String, Option<String>)>,
    pub okta_says: Vec<String>,
    pub device: Option<String>,
    pub token: Option<String>,
    /// the record's hash, for an auth row a re-delivery writes again
    pub h: String,
}

pub fn ev(r: &Value) -> Option<Ev> {
    let actor = at(r, "_norm.actor").as_str().filter(|a| !a.is_empty())?;
    let ms = r["ts"].as_str().and_then(ms_of)?;
    let ip = at(r, "_facts.ip");
    let mut e = Ev {
        src: r["source"].as_str()?.to_string(),
        org: at(r, "_norm.org").as_str().unwrap_or("").to_string(),
        actor: actor.to_string(),
        ms,
        context: r["_context"] == true,
        principal: str_of(at(r, "_facts.principal")),
        cat: 'w',
        ok: true,
        asn: text(&ip["asn"]),
        asorg: str_of(&ip["org"]),
        cc: str_of(&ip["country"]),
        ..Ev::default()
    };
    match e.src.as_str() {
        "cloudtrail" => cloudtrail(r, &mut e),
        "okta" => okta(r, &mut e),
        "github_audit" => github(r, &mut e),
        "google_workspace" => workspace(r, &mut e),
        "app" => app(r, &mut e),
        "gha_ci" => {
            e.verb = format!("step {}", quoted(s(&r["step"])));
            e.typ = Some("ci repo".into());
        }
        "runner_egress" => {
            e.verb = "egress".into();
            e.res = str_of(&r["dst_host"])
                .map(|h| format!("host:{}", clean(&h, 80)))
                .into_iter()
                .collect();
            e.typ = Some("ci repo".into());
        }
        // The app pack's audit shape under a name of yours (its `sources` or
        // `extra_sources`): it reads the fields `app` does.
        _ if r["event"].is_string() => app(r, &mut e),
        _ => return None,
    }
    e.ip = e.ip.filter(|ip| ip.parse::<IpAddr>().is_ok());
    Some(e)
}

const READ: &[&str] = &[
    "Get",
    "List",
    "Describe",
    "Head",
    "Lookup",
    "Search",
    "Scan",
    "BatchGet",
    "Select",
    "Query",
    "AssumeRole",
];
/// Queries that log readOnly false.
const CT_QUERY: &[&str] = &[
    "StartQuery",
    "StopQuery",
    "StartQueryExecution",
    "StopQueryExecution",
    "StartLiveTail",
];
const DELETE: &[&str] = &[
    "Delete",
    "Remove",
    "Terminate",
    "Detach",
    "Deregister",
    "Disable",
    "Stop",
    "Revoke",
    "Leave",
    "Purge",
];
const CT_IAM_SVC: &[&str] = &[
    "iam",
    "sso",
    "identitystore",
    "organizations",
    "sso-directory",
];
const CT_IAM_WORDS: &[&str] = &[
    "Policy",
    "Permission",
    "AccessKey",
    "LoginProfile",
    "MFADevice",
    "Password",
    "PublicAccessBlock",
    "Acl",
    "Grant",
    "TrustAnchor",
    "SSHPublicKey",
];
const CT_IAM: &[&str] = &[
    "ModifyDBSnapshotAttribute",
    "ModifyDBClusterSnapshotAttribute",
    "ModifySnapshotAttribute",
    "ModifyImageAttribute",
    "AuthorizeSecurityGroupIngress",
    "CreateUser",
    "CreateRole",
    "AddUserToGroup",
];
const CT_LOGIN: &[&str] = &[
    "ConsoleLogin",
    "AssumeRoleWithSAML",
    "AssumeRoleWithWebIdentity",
    "GetFederationToken",
    "GetSessionToken",
    "CredentialChallenge",
    "UserAuthentication",
    "Authenticate",
    "Federate",
];
/// An IAM user's own credentials, when the call names no other user.
const CT_CREDS: &[&str] = &[
    "AccessKey",
    "MFADevice",
    "LoginProfile",
    "ChangePassword",
    "SSHPublicKey",
    "ServiceSpecificCredential",
    "SigningCertificate",
];
const CT_RES: &[(&str, &str)] = &[
    ("bucketName", "bucket"),
    ("roleArn", "role"),
    ("roleName", "role"),
    ("userName", "user"),
    ("secretId", "secret"),
    ("policyArn", "policy"),
    ("name", "name"),
    ("functionName", "lambda"),
    ("keyId", "kms"),
    ("dBSnapshotIdentifier", "db-snapshot"),
    ("dBInstanceIdentifier", "db"),
    ("repositoryName", "ecr"),
    ("groupId", "sg"),
    ("trailName", "trail"),
];

fn cloudtrail(r: &Value, e: &mut Ev) {
    let n = s(&r["eventName"]);
    let svc = s(&r["eventSource"]).split('.').next().unwrap_or("");
    let (u, rp) = (&r["userIdentity"], &r["requestParameters"]);
    e.verb = format!("{svc}:{n}");
    // as in norm: the SSO broker's or the token provider's address, not the caller's
    e.ip = str_of(&r["sourceIPAddress"])
        .filter(|_| !matches!(n, "AssumeRoleWithSAML" | "AssumeRoleWithWebIdentity"));
    e.typ = str_of(&u["type"]).map(|t| aws_type(&t));
    e.arn = s(&u["arn"]).to_string();
    e.ok = s(&r["errorCode"]).is_empty() && at(r, "responseElements.ConsoleLogin") != "Failure";
    e.client = agent(s(&r["userAgent"]));
    e.session = assumed(&e.arn);
    if u["type"] == "FederatedUser" {
        e.issuer = str_of(&u["role"]);
    }
    e.mfa = match &u["mfaAuthenticated"] {
        Value::Bool(b) => Some(*b),
        Value::String(t) => Some(t.eq_ignore_ascii_case("true")),
        _ => None,
    };
    let who = str_of(&rp["userName"]).or_else(|| str_of(&rp["roleName"]));
    let own = CT_CREDS.iter().any(|w| n.contains(w))
        && who
            .as_deref()
            .is_none_or(|w| w == s(&u["userName"]) || w == e.actor);
    let ro = &r["readOnly"];
    e.cat = if CT_LOGIN.contains(&n) {
        'a'
    } else if ro == true
        || ro == "true"
        || (ro.is_null() && READ.iter().any(|p| n.starts_with(p)))
        || CT_QUERY.contains(&n)
    {
        'r'
    } else if own {
        'c'
    } else if CT_IAM_SVC.contains(&svc)
        || CT_IAM.contains(&n)
        || CT_IAM_WORDS.iter().any(|w| n.contains(w))
    {
        'i'
    } else if DELETE.iter().any(|p| n.starts_with(p)) {
        'd'
    } else {
        'w'
    };
    for (f, kind) in CT_RES {
        if let Some(v) = str_of(&rp[*f]).filter(|v| !v.is_empty()) {
            let v = if v.starts_with("arn:") { last(&v) } else { &v };
            e.res.push(format!("{kind}:{}", clean(v, 80)));
        }
    }
    let role_arn = s(&rp["roleArn"]);
    if n.starts_with("AssumeRole") && e.ok && !role_arn.is_empty() {
        let re = &r["responseElements"];
        let sess = str_of(&rp["roleSessionName"])
            .or_else(|| assumed(s(at(re, "assumedRoleUser.arn"))).map(|(_, _, s)| s))
            .or_else(|| str_of(&re["subject"]))
            .unwrap_or_default();
        e.assume = Some((
            account(role_arn).to_string(),
            last(role_arn).to_string(),
            sess,
        ));
    }
    let subj = || who.clone().into_iter().collect::<Vec<_>>();
    // A federated sign-in says No: its MFA, if any, was at the IdP.
    let mfa_used = str_of(at(r, "additionalEventData.MFAUsed"))
        .filter(|_| matches!(s(&u["type"]), "IAMUser" | "Root"))
        .map(|m| m == "Yes");
    e.auth = if n == "ConsoleLogin" {
        Some(auth(login(e.ok), vec![], "aws console".into(), mfa_used))
    } else if CT_LOGIN.contains(&n) {
        Some(auth(login(e.ok), vec![], format!("aws {n}"), None))
    } else if n.starts_with("AssumeRole") && !e.ok {
        Some(auth(
            "denied",
            vec![],
            format!("{n} {}", clean(last(role_arn), 64))
                .trim()
                .to_string(),
            None,
        ))
    } else if !e.ok || e.cat == 'r' {
        None
    } else if n == "CreateAccessKey" {
        Some(auth("key created", subj(), "access key".into(), None))
    } else if n.contains("MFADevice") {
        Some(auth("mfa change", subj(), n.into(), None))
    } else if matches!(
        n,
        "ChangePassword" | "CreateLoginProfile" | "UpdateLoginProfile" | "DeleteLoginProfile"
    ) {
        Some(auth("password change", subj(), n.into(), None))
    } else if matches!(
        n,
        "AttachUserPolicy" | "AttachRolePolicy" | "PutUserPolicy" | "PutRolePolicy"
    ) && who.is_some()
    {
        let pol = str_of(&rp["policyArn"])
            .or_else(|| str_of(&rp["policyName"]))
            .unwrap_or_default();
        Some(auth("grant", subj(), format!("{n} {}", last(&pol)), None))
    } else if n == "CreateUser" && who.is_some() {
        Some(auth("user created", subj(), "IAM user".into(), None))
    } else {
        None
    };
}

fn aws_type(t: &str) -> String {
    match t {
        "IAMUser" => "IAM user",
        "AssumedRole" => "assumed role",
        "FederatedUser" => "federated user",
        "Root" => "root",
        "AWSService" => "AWS service",
        "AWSAccount" => "another AWS account",
        "SAMLUser" => "SAML user",
        "WebIdentityUser" => "web identity user",
        other => other,
    }
    .to_string()
}

/// `product/major (class)` from a user agent.
pub fn agent(ua: &str) -> Option<String> {
    let low = ua.to_ascii_lowercase();
    let (name, class) = if ua.is_empty() {
        return None;
    } else if low.contains("console.amazonaws.com")
        || low.contains("signin.amazonaws.com")
        || low.starts_with("[s3console")
        || low.starts_with("mozilla")
    {
        ("browser".to_string(), "console")
    } else if low.ends_with("amazonaws.com") || low == "aws internal" {
        ("aws service".to_string(), "service")
    } else {
        let first = ua
            .trim_start_matches('[')
            .split_whitespace()
            .next()
            .unwrap_or("");
        let (n, v) = first.split_once('/').unwrap_or((first, ""));
        let class = if n.starts_with("aws-cli") {
            "cli"
        } else if low.contains("terraform") {
            "terraform"
        } else if low.contains("sdk") || low.contains("boto") {
            "sdk"
        } else {
            "other"
        };
        let major = v.split('.').next().unwrap_or("");
        (
            clean(
                &if major.is_empty() {
                    n.to_string()
                } else {
                    format!("{n}/{major}")
                },
                40,
            ),
            class,
        )
    };
    Some(format!("{name} ({class})"))
}

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

fn okta(r: &Value, e: &mut Ev) {
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

fn github(r: &Value, e: &mut Ev) {
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

fn workspace(r: &Value, e: &mut Ev) {
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

fn app(r: &Value, e: &mut Ev) {
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

fn auth(kind: &'static str, subjects: Vec<String>, what: String, mfa: Option<bool>) -> Auth {
    Auth {
        kind,
        subjects,
        what,
        mfa,
    }
}

fn login(ok: bool) -> &'static str {
    if ok {
        "login"
    } else {
        "login failed"
    }
}

/// `arn:aws:sts::<acct>:assumed-role/<role>/<session>` -> (acct, role, session)
pub fn assumed(arn: &str) -> Option<(String, String, String)> {
    let (_, rest) = arn.split_once(":assumed-role/")?;
    let (role, sess) = rest.split_once('/').unwrap_or((rest, ""));
    Some((account(arn).to_string(), role.to_string(), sess.to_string()))
}

fn account(arn: &str) -> &str {
    arn.split(':').nth(4).unwrap_or("")
}

fn last(arn: &str) -> &str {
    arn.rsplit('/').next().unwrap_or(arn)
}

static NULL: Value = Value::Null;

/// `a.b.c` in a record, Null when any step is missing.
pub fn at<'a>(v: &'a Value, path: &str) -> &'a Value {
    path.split('.')
        .try_fold(v, |v, k| v.get(k))
        .unwrap_or(&NULL)
}

fn s(v: &Value) -> &str {
    v.as_str().unwrap_or("")
}

fn str_of(v: &Value) -> Option<String> {
    v.as_str().filter(|x| !x.is_empty()).map(str::to_string)
}

fn text(v: &Value) -> Option<String> {
    match v {
        Value::String(x) if !x.is_empty() => Some(x.clone()),
        Value::Number(n) => Some(n.to_string()),
        _ => None,
    }
}

/// Printable ASCII without quotes, `\`, `=` or `;`, at most `max` characters:
/// text a log's writer chose cannot pass for the header's own structure.
pub fn clean(t: &str, max: usize) -> String {
    let mut out: String = t
        .chars()
        .map(|c| {
            if matches!(c, ' '..='~') && !matches!(c, '"' | '\'' | '\\' | '=' | ';') {
                c
            } else {
                '?'
            }
        })
        .take(max + 1)
        .collect();
    if out.len() > max {
        out.truncate(max.saturating_sub(3));
        out.push_str("...");
    }
    out
}

/// Free text (a title, a path, an app's name), capped and quoted.
pub fn quoted(t: &str) -> String {
    format!("'{}'", clean(t, 40))
}

/// RFC 3339 to epoch milliseconds.
pub fn ms_of(ts: &str) -> Option<i64> {
    let num = |a: usize, b: usize| ts.get(a..b).and_then(|x| x.parse::<i64>().ok());
    let (y, mo, d) = (num(0, 4)?, num(5, 7)?, num(8, 10)?);
    let (h, mi, sec) = (num(11, 13)?, num(14, 16)?, num(17, 19)?);
    let rest = ts.get(19..)?;
    let frac_len = rest
        .strip_prefix('.')
        .map_or(0, |f| f.bytes().take_while(u8::is_ascii_digit).count());
    let frac = if frac_len > 0 {
        rest.get(1..1 + frac_len.min(3))
            .and_then(|f| f.parse::<i64>().ok())
    } else {
        None
    };
    let ms = frac.map_or(0, |f| f * 10_i64.pow(3 - frac_len.min(3) as u32));
    let tz = rest.get(if frac_len > 0 { 1 + frac_len } else { 0 }..)?;
    let off = match tz {
        "" | "Z" | "z" => 0,
        _ => {
            let sign = if tz.starts_with('-') { -1 } else { 1 };
            let hh = tz.get(1..3)?.parse::<i64>().ok()?;
            let mm = tz
                .get(4..6)
                .and_then(|x| x.parse::<i64>().ok())
                .unwrap_or(0);
            sign * (hh * 60 + mm) * 60_000
        }
    };
    Some((days_from_civil(y, mo, d) * 86_400 + h * 3600 + mi * 60 + sec) * 1000 + ms - off)
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (m + if m > 2 { -3 } else { 9 }) + 2) / 5 + d - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

/// Epoch milliseconds to `YYYY-MM-DD`.
pub fn date(ms: i64) -> String {
    let z = ms.div_euclid(86_400_000) + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    format!("{:04}-{m:02}-{d:02}", yoe + era * 400 + i64::from(m <= 2))
}
