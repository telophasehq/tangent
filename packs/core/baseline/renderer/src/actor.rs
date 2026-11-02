//! `actor`: who it is to the profile, its person and the actors that person
//! was seen as, the identities seen here; for a role, who assumed it, and who
//! assumed them, or its SSO permission set; the privilege it used before; a
//! session's MFA against its earlier sessions'.

use std::collections::BTreeSet;

use baselinekit::kinds::assumed;
use baselinekit::time::H;
use baselinekit::{Kv, R};

use crate::before::{Before, CHAIN};
use crate::header::Win;
use crate::text::{ago, join};

/// Who is behind a role or a federated user: their sign-ins count too.
pub struct Behind {
    pub name: String,
    /// their person, when who is who names one
    pub person: Option<String>,
}

impl Behind {
    /// Their person: a name no one is named for is its own.
    pub fn person(&self) -> String {
        self.person
            .clone()
            .unwrap_or_else(|| self.name.to_lowercase())
    }
}

/// The line, and who is behind a role or a federated user.
pub fn actor<K: Kv>(b: &mut Before<K>, w: &Win) -> R<(String, Option<Behind>)> {
    let (a, fresh, ws) = (w.actor, &w.fresh, w.ws);
    let own = &w.own;
    let mut behind: Option<Behind> = None;
    let mut parts = vec![match w.e0.principal.as_deref() {
        Some("member") => "human (profile member)",
        Some("service") => "service account (profile)",
        Some("key") => "API key (profile)",
        Some("aws") => "AWS service",
        _ => "not in profile",
    }
    .to_string()];
    if w.named {
        // the actors the builder saw the person as; nothing to say of a
        // person seen by no name but the actor's
        let names: BTreeSet<String> = (b.firsts(&["p", own, "n", "actor"])?.into_iter())
            .map(|(n, _)| n)
            .collect();
        if *own != a.to_lowercase() || names.iter().any(|n| n != own) {
            parts.push(if names.is_empty() {
                format!("person {own}")
            } else {
                format!("person {own} ({})", join(names.iter()))
            });
        }
    }
    let types: BTreeSet<&str> = fresh.iter().filter_map(|e| e.typ.as_deref()).collect();
    if !types.is_empty() {
        parts.push(format!("here as {}", join(types.iter())));
    }
    let sessions: BTreeSet<&(String, String, String)> =
        fresh.iter().filter_map(|e| e.session.as_ref()).collect();
    for (acct, role, sess) in sessions.into_iter().take(2) {
        if let Some(set) = role.strip_prefix("AWSReservedSSO_") {
            parts.push(format!(
                "SSO permission set {}",
                set.rsplit_once('_').map_or(set, |(s, _)| s)
            ));
        } else {
            let (text, root) = chain(b, acct, role, sess, ws, 0)?;
            parts.push(format!("chain {role}{text}"));
            behind = root.or(behind);
        }
    }
    let issuers: BTreeSet<&str> = fresh.iter().filter_map(|e| e.issuer.as_deref()).collect();
    for iss in issuers {
        parts.push(format!("federated by {iss}"));
        // the issuer's person, filed from its records or its federated users'
        let person = b.firsts(&["a", iss, "n", "person"])?.pop().map(|(p, _)| p);
        behind = Some(Behind {
            name: iss.to_string(),
            person,
        });
    }
    // an AWS service signs in nowhere
    let behind = behind.filter(|r| !r.name.starts_with("aws:") && r.person() != *own);
    let used: BTreeSet<String> = b
        .firsts(&["a", a, "n", "cat"])?
        .into_iter()
        .map(|(c, _)| c)
        .collect();
    let tier = if used.contains("i") {
        "admin (iam/policy changes)"
    } else if used.contains("w") || used.contains("d") {
        "write"
    } else if used.contains("r") {
        "read-only"
    } else {
        "none"
    };
    parts.push(format!("privilege used before: {tier}"));
    let now: BTreeSet<bool> = fresh.iter().filter_map(|e| e.mfa).collect();
    if !now.is_empty() {
        let was: BTreeSet<String> = b
            .firsts(&["a", a, "n", "mfa"])?
            .into_iter()
            .map(|(v, _)| v)
            .collect();
        let (yes, no) = (was.contains("yes"), was.contains("no"));
        let line = match (now.contains(&false), yes, no) {
            (true, true, false) => Some("session mfa no, first session without mfa"),
            (true, ..) => None, // never MFA, or not always: nothing new
            (false, true, false) => Some("session mfa yes, as in every earlier session"),
            (false, false, true) => Some("session mfa yes, first session with mfa"),
            (false, ..) => Some("session mfa yes"),
        };
        parts.extend(line.map(str::to_string));
    }
    Ok((parts.join("; "), behind))
}

/// Who assumed `role` as `sess` last before `before`, and who assumed them: (text, root actor).
fn chain<K: Kv>(
    b: &mut Before<K>,
    acct: &str,
    role: &str,
    sess: &str,
    before: i64,
    depth: usize,
) -> R<(String, Option<Behind>)> {
    let Some(x) = b.assumed(acct, role, sess, before)? else {
        return Ok((format!(" <- not seen in {}h", CHAIN / H), None));
    };
    let first = if b.first(&["a", role, "n", "by", &x.by])? == Some(x.t) {
        ", first time"
    } else {
        ""
    };
    let mut out = format!(
        " <- {} ({}, {} before{first})",
        x.by,
        x.typ,
        ago(b.ws - x.t)
    );
    let by = Behind {
        name: x.by,
        person: x.person,
    };
    if let Some((a2, r2, s2)) = assumed(&x.arn).filter(|_| depth < 3) {
        let (more, root) = chain(b, &a2, &r2, &s2, x.t, depth + 1)?;
        out += &more;
        return Ok((out, root.or(Some(by))));
    }
    Ok((out, Some(by)))
}
