use baseline_builder::build;
use baselinekit::people::{Profile, PEOPLE};
use baselinekit::{Mem, Scalar};
use serde_json::{json, Value};

use crate::render;

fn profile() -> Profile {
    Profile::from_value(json!({"dave@acme.io": ["dave"]}))
}

const HOME: (&str, i64, &str) = ("203.0.113.10", 64500, "US");
const AWAY: (&str, i64, &str) = ("198.51.100.23", 64501, "NL");

/// `ts` is in 2026, `01-05T13:00:00`.
fn okta(ts: &str, event: &str, (ip, asn, cc): (&str, i64, &str)) -> Value {
    json!({"source": "okta", "ts": format!("2026-{ts}Z"), "eventType": event, "outcome": {"result": "SUCCESS"},
           "actor": {"alternateId": "dave@acme.io", "type": "User"}, "client": {"ipAddress": ip},
           "_norm": {"actor": "dave@acme.io", "org": "acme"},
           "_facts": {"principal": "member", "ip": {"asn": asn, "country": cc}}})
}

fn failed(mut r: Value) -> Value {
    if let Some(o) = r.get_mut("outcome") {
        *o = json!({"result": "FAILURE"});
    }
    r
}

/// `who`: an IAM user's name, or `role/session`.
fn ct(who: &str, ts: &str, name: &str, ip: &str, rp: Value) -> Value {
    let (actor, id) = match who.split_once('/') {
        Some((role, _)) => (
            role,
            json!({"type": "AssumedRole", "arn": format!("arn:aws:sts::111122223333:assumed-role/{who}")}),
        ),
        None => (
            who,
            json!({"type": "IAMUser", "userName": who, "arn": format!("arn:aws:iam::111122223333:user/{who}")}),
        ),
    };
    let svc = if name.starts_with("Assume") {
        "sts"
    } else if name.contains("Object") {
        "s3"
    } else {
        "iam"
    };
    json!({"source": "cloudtrail", "ts": format!("2026-{ts}Z"), "eventName": name, "eventSource": format!("{svc}.amazonaws.com"),
           "sourceIPAddress": ip, "userAgent": "aws-cli/2.17.0", "requestParameters": rp, "userIdentity": id,
           "_norm": {"actor": actor, "org": "acme"},
           "_facts": {"principal": "member", "ip": {"asn": 64500, "country": "US"}}})
}

fn fold(recs: &[Value]) -> Mem {
    let mut st = Mem::default();
    let lines: Vec<String> = recs.iter().map(Value::to_string).collect();
    assert!(build(&mut st, &profile(), &lines).is_ok());
    st
}

fn baseline(st: &mut Mem, rec: &Value) -> Value {
    // the judge's: what the builder filed, no profile
    let out = render(st, &[rec.to_string()]).unwrap_or_default();
    let rec: Value = serde_json::from_slice(&out).unwrap_or_default();
    rec.get("_baseline").cloned().unwrap_or_default()
}

fn has(h: &Value, k: &str, text: &str) {
    assert!(
        h[k].as_str().is_some_and(|s| s.contains(text)),
        "{text}\n{h}"
    );
}

#[test]
fn okta_sign_in_from_a_new_country_then_aws_from_the_aliased_user() {
    let mut st = fold(&[
        okta("01-01T15:00:00", "user.session.start", HOME),
        ct(
            "dave",
            "01-02T15:00:00",
            "GetObject",
            HOME.0,
            json!({"bucketName": "x"}),
        ),
        // anyone can fail a sign-in in dave's name, from anywhere
        failed(okta("01-05T11:00:00", "user.session.start", AWAY)),
        // one sign-in, three events, its first two seconds before the session
        okta("01-05T12:39:58", "policy.evaluate_sign_on", AWAY),
        okta("01-05T12:39:59", "user.authentication.verify", AWAY),
        okta("01-05T12:40:00", "user.session.start", AWAY),
    ]);
    let h = baseline(
        &mut st,
        &ct(
            "dave",
            "01-05T13:00:00",
            "PutObject",
            "192.0.2.77",
            json!({"bucketName": "x"}),
        ),
    );
    let link = "okta login from a network new for dave@acme.io 20m before this window";
    has(&h, "links", link);
    has(
        &h,
        "auth",
        "from 198.51.100.23 ASN 64501 NL (new asn/cc for person then)",
    );
    has(&h, "auth", "1 failed");
    has(&h, "actor", "person dave@acme.io (dave, dave@acme.io)");
}

#[test]
fn a_chain_goes_back_to_who_is_behind_the_role() {
    let role = |r: &str, s: &str| json!({"roleArn": format!("arn:aws:iam::111122223333:role/{r}"), "roleSessionName": s});
    let mut st = fold(&[
        ct(
            "eve",
            "01-05T12:00:00",
            "AttachRolePolicy",
            HOME.0,
            json!({"roleName": "admin", "policyArn": "arn:aws:iam::aws:policy/AdministratorAccess"}),
        ),
        ct(
            "alice@acme.io",
            "01-05T12:55:10",
            "AssumeRole",
            HOME.0,
            role("hop", "s1"),
        ),
        ct(
            "alice@acme.io",
            "01-05T12:59:00",
            "AssumeRole",
            HOME.0,
            role("hop", "s1"),
        ),
        ct(
            "hop/s1",
            "01-05T12:59:30",
            "AssumeRole",
            HOME.0,
            role("admin", "s2"),
        ),
    ]);
    let h = baseline(
        &mut st,
        &ct("admin/s2", "01-05T13:00:00", "ListUsers", HOME.0, json!({})),
    );
    let want = "chain admin <- hop (assumed role, 30s before, first time) <- alice@acme.io (IAM user, 60s before)";
    has(&h, "actor", want);
    // the role's own grants, and the sign-ins of who is behind it
    has(&h, "auth", "last 72h, admin and alice@acme.io: ");
    has(
        &h,
        "auth",
        "grant AttachRolePolicy AdministratorAccess by eve 60m before",
    );
}

#[test]
fn nothing_at_or_after_the_window_start_counts() {
    let old = ct(
        "dave",
        "01-01T15:00:00",
        "GetObject",
        HOME.0,
        json!({"bucketName": "x"}),
    );
    let win = ct(
        "dave",
        "01-05T13:00:00",
        "PutObject",
        "192.0.2.77",
        json!({"bucketName": "x"}),
    );
    let same = ct(
        "dave",
        "01-05T13:00:00",
        "DeleteUser",
        "192.0.2.78",
        json!({"userName": "eve"}),
    );
    let h = baseline(&mut fold(std::slice::from_ref(&old)), &win);
    has(&h, "seen", "seen 3.9 days");
    assert_eq!(h, baseline(&mut fold(&[old, win.clone(), same]), &win));
}

#[test]
fn a_burst_goes_on_from_its_context() {
    let day = |d: i64| {
        ct(
            "dave",
            &format!("01-0{d}T12:30:00"),
            "GetObject",
            HOME.0,
            json!({"bucketName": "x"}),
        )
    };
    let mut st = fold(&[day(1), day(2), day(3), day(4)]);
    // judged already, in the window's own five minutes, so in no slot `_baseline` reads
    let mut ctx = ct(
        "dave",
        "01-05T13:00:50",
        "GetObject",
        HOME.0,
        json!({"bucketName": "x"}),
    );
    if let Some(m) = ctx.as_object_mut() {
        m.insert("_context".into(), json!(true));
    }
    let key = ct(
        "dave",
        "01-05T13:00:55",
        "CreateAccessKey",
        HOME.0,
        json!({}),
    );
    let lines = [ctx.to_string(), key.to_string()];
    let out = render(&mut st, &lines).unwrap_or_default();
    let last: Value = (out.split(|b| *b == b'\n'))
        .nth(1)
        .and_then(|l| serde_json::from_slice(l).ok())
        .unwrap_or_default();
    let h = last.get("_baseline").cloned().unwrap_or_default();
    has(&h, "time", "last active 5s before");
    has(&h, "volume", "own creds none before");
}

#[test]
fn own_credentials_are_not_admin() {
    let own = [
        ct(
            "dave",
            "01-01T15:00:00",
            "CreateAccessKey",
            HOME.0,
            json!({}),
        ),
        ct(
            "dave",
            "01-01T15:01:00",
            "ChangePassword",
            HOME.0,
            json!({}),
        ),
        ct(
            "dave",
            "01-01T15:02:00",
            "GetObject",
            HOME.0,
            json!({"bucketName": "x"}),
        ),
    ];
    let win = ct(
        "dave",
        "01-05T13:00:00",
        "GetObject",
        HOME.0,
        json!({"bucketName": "x"}),
    );
    has(
        &baseline(&mut fold(&own), &win),
        "actor",
        "privilege used before: read-only",
    );
    let other = ct(
        "dave",
        "01-04T15:03:00",
        "CreateAccessKey",
        HOME.0,
        json!({"userName": "deploy-bot"}),
    );
    let h = baseline(&mut fold(&[own.as_slice(), &[other]].concat()), &win);
    has(&h, "actor", "privilege used before: admin");
    has(&h, "auth", "key created access key on deploy-bot");
}

/// The renderer reads who is who from what the builder filed, not a
/// profile of its own; a changed profile is filed again.
#[test]
fn the_renderer_reads_the_builder_s_people() {
    let mut st = fold(&[ct(
        "dave",
        "01-01T15:00:00",
        "GetObject",
        HOME.0,
        json!({"bucketName": "x"}),
    )]);
    let win = ct(
        "dave",
        "01-05T13:00:00",
        "GetObject",
        HOME.0,
        json!({"bucketName": "x"}),
    );
    has(
        &baseline(&mut st, &win),
        "actor",
        "person dave@acme.io (dave, dave@acme.io)",
    );
    let alone = Profile::from_value(json!({}));
    assert!(build(&mut st, &alone, &[]).is_ok());
    assert!(matches!(st.0.get(PEOPLE), Some(Scalar::Str(t)) if t == "{}"));
    let h = baseline(&mut st, &win);
    let actor = h.get("actor").and_then(Value::as_str).unwrap_or_default();
    assert!(!actor.is_empty() && !actor.contains("person"), "{h}");
}

/// The app pack's audit shape under a name of yours (its `sources` or
/// `extra_sources`) is learned as `app` is; a record of a source baseline
/// does not know, without an `event`, is not.
#[test]
fn the_app_shape_under_another_name() {
    let rec = |src: &str, ts: &str| {
        json!({"source": src, "ts": format!("2026-{ts}Z"), "event": "invoice.view", "ip": HOME.0,
               "path": "/invoices/42", "_norm": {"actor": "carol@acme.io", "org": "acme"}})
    };
    let mut st = fold(&[rec("billing", "01-01T15:00:00")]);
    let h = baseline(&mut st, &rec("billing", "01-05T13:00:00"));
    has(&h, "seen", "seen 3.9 days");
    let mut other = rec("mystery", "01-05T13:00:00");
    if let Some(m) = other.as_object_mut() {
        m.remove("event");
    }
    assert_eq!(baseline(&mut st, &other), Value::Null);
}
