//! What baseline keeps of one record, per source: its verb, category,
//! resources, client, identity and auth event. Categories: r read, w write,
//! i iam/policy on someone or something else, d delete, a sign-in, c the
//! actor's own credentials.

mod app;
mod cloudtrail;
mod github;
mod okta;
mod workspace;

use std::net::IpAddr;

use serde_json::Value;

use crate::time::ms_of;

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
    /// The actor's person: the record's `_norm.person`, else the actor
    /// lowercased, its own person. Both plugins read it so.
    pub person: String,
    /// The record names `person` in `_norm.person`.
    pub named: bool,
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
    let person = str_of(at(r, "_norm.person")).map(|p| p.to_lowercase());
    let mut e = Ev {
        src: r["source"].as_str()?.to_string(),
        org: at(r, "_norm.org").as_str().unwrap_or("").to_string(),
        actor: actor.to_string(),
        named: person.is_some(),
        person: person.unwrap_or_else(|| actor.to_lowercase()),
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
        "cloudtrail" => cloudtrail::read(r, &mut e),
        "okta" => okta::read(r, &mut e),
        "github_audit" => github::read(r, &mut e),
        "google_workspace" => workspace::read(r, &mut e),
        "app" => app::read(r, &mut e),
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
        _ if r["event"].is_string() => app::read(r, &mut e),
        _ => return None,
    }
    e.ip = e.ip.filter(|ip| ip.parse::<IpAddr>().is_ok());
    Some(e)
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
