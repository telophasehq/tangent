//! `baseline_builder`, in ingest: folds every record it can read into the
//! keys the renderer reads in the judge (../kit has them); emits nothing.
//! Every fold is a min, a sum or a row keyed by event time, so records
//! arriving in any order leave the same state. Who is who is resolved here,
//! where the profile is, and filed with what it is about, so the renderer
//! resolves no one.

mod counts;
mod firsts;
mod rows;
mod sessions;
// wasm only: the renderer's tests link this crate, and a binary holds one
// plugin's exports.
#[cfg(target_arch = "wasm32")]
mod wit;

use baselinekit::kinds::{ev, Ev};
use baselinekit::{Kv, R};
pub use identitykit::People;
use serde_json::Value;

/// Folds `lines` in, each actor under its person as the renderer reads it:
/// the record's `_norm.person`, else the actor lowercased. `people` resolves
/// only the names a record holds besides its actor: whom a credential change
/// was done to, and a federated user's issuer.
pub fn build(kv: &mut impl Kv, people: &People, lines: &[String]) -> R<()> {
    let evs: Vec<Ev> = lines
        .iter()
        .filter_map(|l| {
            let r: Value = serde_json::from_str(l).ok()?;
            let mut e = ev(&r)?;
            if e.auth.is_some() {
                e.h = hash(&r.to_string());
            }
            Some(e)
        })
        .collect();
    let evs: Vec<&Ev> = evs.iter().collect();
    firsts::write(kv, people, &evs)?;
    counts::write(kv, &evs)?;
    sessions::write(kv, &evs)?;
    rows::write(kv, people, &evs)
}

/// FNV-1a: a re-delivered record writes the same auth row.
fn hash(s: &str) -> String {
    let h = s.bytes().fold(0xcbf2_9ce4_8422_2325_u64, |h, b| {
        (h ^ u64::from(b)).wrapping_mul(0x100_0000_01b3)
    });
    format!("{:08x}", h >> 32)
}

#[cfg(test)]
mod tests {
    use baselinekit::keys::string;
    use baselinekit::Mem;
    use serde_json::{json, Value};

    use super::{build, People};

    fn people() -> People {
        People::from_value(&json!({"dave@acme.io": ["dave"], "erin@acme.io": ["erin"]}))
    }

    /// An IAM user's call, `_norm` as the packs write it: the person, when
    /// the profile names them.
    fn ct(user: &str, name: &str, rp: Value) -> Value {
        let people = people();
        let p = Some(people.person(user)).filter(|p| people.knows(p));
        let rec = json!({"source": "cloudtrail", "ts": "2026-01-05T13:00:00Z", "eventName": name,
                         "eventSource": "sts.amazonaws.com", "requestParameters": rp,
                         "userIdentity": {"type": "IAMUser", "userName": user},
                         "_norm": {"actor": user, "org": "acme"}});
        with_person(rec, p.as_deref())
    }

    /// `rec` with this `_norm.person`, or none.
    fn with_person(mut rec: Value, p: Option<&str>) -> Value {
        if let Some(n) = rec.get_mut("_norm").and_then(Value::as_object_mut) {
            match p {
                Some(p) => n.insert("person".into(), json!(p)),
                None => n.remove("person"),
            };
        }
        rec
    }

    /// Who is who goes in with what it is about: the actors a person was
    /// seen as and back, who signed in, who assumed a role, whose a federated
    /// user is. An actor's person is the one its record names; the profile
    /// is asked only for the names besides it.
    #[test]
    fn people_are_filed_with_what_they_did() {
        let role =
            json!({"roleArn": "arn:aws:iam::111122223333:role/admin", "roleSessionName": "s1"});
        let mallory = ct("mallory", "ConsoleLogin", json!({}));
        let fed = "arn:aws:sts::111122223333:federated-user/build";
        let recs = [
            ct("Dave", "ConsoleLogin", json!({})),
            ct("dave", "AssumeRole", role),
            ct("eve", "ConsoleLogin", json!({})),
            with_person(mallory, Some("Mallory@acme.io")),
            // a record that names no person: dave is his own here, as the
            // renderer reads him
            with_person(ct("dave", "ConsoleLogin", json!({})), None),
            json!({"source": "cloudtrail", "ts": "2026-01-05T13:00:00Z", "eventName": "ListBuckets",
                   "eventSource": "s3.amazonaws.com",
                   "userIdentity": {"type": "FederatedUser", "arn": fed, "role": "erin"},
                   "_norm": {"actor": fed, "org": "acme"}}),
        ];
        let lines: Vec<String> = recs.iter().map(Value::to_string).collect();
        let mut st = Mem::default();
        assert!(build(&mut st, &people(), &lines).is_ok());
        for k in [
            "acme|p|dave@acme.io|n|actor|dave",
            "acme|a|Dave|n|person|dave@acme.io",
            "acme|p|mallory@acme.io|n|actor|mallory",
            "acme|a|erin|n|person|erin@acme.io",
        ] {
            assert!(st.0.contains_key(k), "{k}");
        }
        assert!(!st.0.keys().any(|k| k.starts_with("acme|a|eve|n|person|")));
        // erin issued, but was not seen acting
        assert!(!st.0.contains_key("acme|p|erin@acme.io|n|actor|erin"));
        // the first row under a prefix
        let first = |pre: &str| {
            (st.0.range(pre.to_string()..).next())
                .filter(|(k, _)| k.starts_with(pre))
                .and_then(|(_, v)| string(v.clone()))
                .and_then(|v| serde_json::from_str::<Value>(&v).ok())
                .unwrap_or_default()
        };
        let dave = json!("dave@acme.io");
        assert_eq!(first("acme|p|dave@acme.io|e|").get("person"), Some(&dave));
        assert_eq!(first("acme|p|dave|e|").get("person"), Some(&Value::Null));
        assert_eq!(first("acme|p|eve|e|").get("person"), Some(&Value::Null));
        assert_eq!(first("acme|s|").pointer("/0/person"), Some(&dave));
    }
}
