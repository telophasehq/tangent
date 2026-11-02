//! CloudTrail: the call, its category, the caller's identity and session, the
//! role it assumed, and its sign-in or credential change.

use serde_json::Value;

use super::{account, assumed, at, auth, clean, last, login, s, str_of, Ev};

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

pub(super) fn read(r: &Value, e: &mut Ev) {
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
fn agent(ua: &str) -> Option<String> {
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
