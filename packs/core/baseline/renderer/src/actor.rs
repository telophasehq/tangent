//! `actor`: who it is to the profile, its person and that person's other
//! names, the identities seen here; for a role, who assumed it, and who
//! assumed them, or its SSO permission set; the privilege it used before; a
//! session's MFA against its earlier sessions'.

use std::collections::BTreeSet;

use baselinekit::kinds::assumed;
use baselinekit::time::H;
use baselinekit::{Kv, R};

use crate::before::{Before, CHAIN};
use crate::header::Win;
use crate::text::{ago, join};

/// The line, and who is behind a role or a federated user: their sign-ins
/// count too.
pub fn actor<K: Kv>(b: &mut Before<K>, w: &Win) -> R<(String, Option<String>)> {
    let (a, fresh, people, ws) = (w.actor, &w.fresh, w.people, w.ws);
    let own = &w.own;
    let mut behind: Option<String> = None;
    let mut parts = vec![match w.e0.principal.as_deref() {
        Some("member") => "human (profile member)",
        Some("service") => "service account (profile)",
        Some("key") => "API key (profile)",
        Some("aws") => "AWS service",
        _ => "not in profile",
    }
    .to_string()];
    if let Some(names) = people.names.get(own).filter(|n| n.len() > 1) {
        parts.push(format!("person {own} ({})", join(names.iter())));
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
        behind = Some(iss.to_string());
    }
    // an AWS service signs in nowhere
    let behind = behind.filter(|r| !r.starts_with("aws:") && people.person(r) != *own);
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
) -> R<(String, Option<String>)> {
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
    if let Some((a2, r2, s2)) = assumed(&x.arn).filter(|_| depth < 3) {
        let (more, root) = chain(b, &a2, &r2, &s2, x.t, depth + 1)?;
        out += &more;
        return Ok((out, root.or(Some(x.by))));
    }
    Ok((out, Some(x.by)))
}
