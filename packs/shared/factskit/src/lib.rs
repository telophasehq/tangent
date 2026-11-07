//! `_facts`: what the company profile says about a record and what is known
//! about its address, and the record written out with `_norm` and `_facts`.
//!
//! The profile is the runtime's `profile` enrichment table, a document; its
//! keys are listed in packs/enrich-ip/README.md. The addresses come from
//! [`enrichkit`]. Every pack's plugin ends in [`Kit::write`].

use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;
use std::sync::OnceLock;

use enrichkit::{parse_net, Ip, Lookup, Tables};
use identitykit::People;
use ipnet::IpNet;
use normkit::{Norm, Stamp};
use serde::Serialize;
use serde_json::{json, Map, Value};

/// What every pack writes for a record, as one line of JSON: the time and
/// source it was read with, `_norm`, `_facts`, then the provider's fields
/// the pack kept. A sink can rely on `ts`, `source` and `_norm` being there
/// in every record; the rest is the provider's, and nothing is promised.
#[derive(Serialize)]
pub struct Finished {
    pub ts: String,
    pub source: String,
    #[serde(rename = "_norm")]
    pub norm: Norm,
    #[serde(rename = "_facts")]
    pub facts: Map<String, Value>,
    #[serde(flatten)]
    pub rest: Map<String, Value>,
}

/// The company, as no log says it.
#[derive(Default)]
pub struct Profile {
    org: Option<String>,
    own_sources: BTreeSet<String>, // their records are all `org`'s
    countries: BTreeSet<String>,
    watch: BTreeSet<String>,
    members: BTreeSet<String>,
    services: BTreeSet<String>,
    keys: BTreeMap<String, Vec<IpNet>>, // empty: any network
    cloud: Option<String>,
    about: BTreeMap<String, String>, // actor: who it is
    people: People,                  // which names are one person
    hosts: BTreeSet<String>,
    sensitive: BTreeSet<String>,
}

fn text(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

fn strs(v: &Value) -> BTreeSet<String> {
    v.as_array()
        .map(|a| a.iter().filter_map(text).collect())
        .unwrap_or_default()
}

impl Profile {
    /// The profile document; null is an empty profile.
    pub fn from_value(f: &Value) -> Self {
        Profile {
            org: f["org"].as_str().map(str::to_string),
            own_sources: strs(&f["own_sources"]),
            countries: strs(&f["countries"]),
            watch: strs(&f["watch"]),
            members: strs(&f["members"]),
            services: strs(&f["services"]),
            keys: match &f["keys"] {
                Value::Object(m) => m
                    .iter()
                    .map(|(k, nets)| {
                        (
                            k.clone(),
                            strs(nets).iter().filter_map(|n| parse_net(n)).collect(),
                        )
                    })
                    .collect(),
                other => strs(other).into_iter().map(|k| (k, Vec::new())).collect(),
            },
            cloud: f["cloud"].as_str().map(str::to_string),
            about: serde_json::from_value(f["about"].clone()).unwrap_or_default(),
            people: People::from_value(&f["people"]),
            hosts: strs(&f["hosts"]),
            sensitive: strs(&f["sensitive"]),
        }
    }

    /// Who the actor is to the profile.
    fn principal(&self, actor: &str) -> &'static str {
        if self.services.contains(actor) {
            "service"
        } else if self.members.contains(actor) {
            "member"
        } else if self.keys.contains_key(actor) {
            "key"
        } else {
            "unknown"
        }
    }
}

/// The profile and the tables, read once per plugin instance.
pub struct Kit {
    pub profile: Profile,
    pub tables: Tables,
}

/// The kit, from the runtime's tables. Without a `profile` table the profile
/// is empty, and the runtime has said so.
pub fn kit(lookup: Lookup) -> &'static Kit {
    static K: OnceLock<Kit> = OnceLock::new();
    K.get_or_init(|| Kit::new(lookup))
}

impl Kit {
    pub fn new(lookup: Lookup) -> Self {
        let profile = lookup("profile", "")
            .and_then(|s| serde_json::from_str::<Value>(&s).ok())
            .unwrap_or(Value::Null);
        Kit {
            profile: Profile::from_value(&profile),
            tables: Tables::new(lookup),
        }
    }

    /// The [`Finished`] record, as one line of JSON: `stamp` from the record
    /// the pack read, `rec` as the pack trimmed it, `_norm` and `_facts`.
    pub fn write(
        &self,
        out: &mut Vec<u8>,
        stamp: Stamp,
        mut rec: Map<String, Value>,
        mut n: Norm,
    ) -> Result<(), String> {
        let p = &self.profile;
        // The company's own infrastructure is one tenant: the company.
        if let Some(org) = &p.org {
            if p.own_sources.contains(&stamp.source) {
                n.org = Some(org.clone());
            }
        }
        // The person behind the actor, only when the profile names them.
        if n.person.is_none() {
            n.person = n
                .actor
                .as_deref()
                .map(|a| p.people.person(a))
                .filter(|w| p.people.knows(w));
        }
        let ip = n.ip.as_deref().and_then(|a| self.tables.ip(a));
        let mut facts = facts_of(p, &stamp.source, &n, ip);
        let ips: Map<String, Value> = n
            .addresses
            .iter()
            .filter_map(|(field, a)| {
                Some((field.clone(), Value::Object(self.tables.ip(a)?.facts())))
            })
            .collect();
        if !ips.is_empty() {
            facts.insert("ips".into(), Value::Object(ips));
        }
        rec.remove("ts");
        rec.remove("source");
        unrepeat(&rec, &stamp, &mut n);
        let finished = Finished {
            ts: stamp.ts,
            source: stamp.source,
            norm: n,
            facts,
            rest: rec,
        };
        serde_json::to_writer(&mut *out, &finished).map_err(|e| e.to_string())?;
        out.push(b'\n');
        Ok(())
    }
}

/// What the profile and the tables know about this record, with the notes a
/// model reads them by.
fn facts_of(p: &Profile, source: &str, n: &Norm, ip: Option<Ip>) -> Map<String, Value> {
    let mut facts = Map::new();
    let mut notes = Vec::new();
    let kind = n.kind.clone().unwrap_or_default();
    let actor = n.actor.clone();

    if p.watch.contains(&kind) {
        facts.insert("watch".into(), json!(true));
        notes.push(format!("{kind} is a kind the org watches every time"));
    }
    // What only the record's own log says, as its pack read it.
    for (key, note) in &n.facts {
        facts.insert(key.clone(), json!(true));
        notes.push(note.clone());
    }

    let about = actor.as_ref().and_then(|a| p.about.get(a));
    if let Some(s) = about {
        facts.insert("about".into(), json!(s));
    }

    // The principal: as its pack named it, known to the profile, or nobody it
    // knows. Only a pack's mark counts: an `aws:` name could be anyone's.
    let who = actor.as_deref().and_then(|a| match n.principal {
        Some("none") => None, // a repository or nothing
        Some(named) => Some(named),
        None => Some(p.principal(a)),
    });
    if let (Some(a), Some(w)) = (&actor, who) {
        facts.insert("principal".into(), json!(w));
        if w == "unknown" && about.is_none() {
            notes.push(format!("principal {a} is nobody the org profile knows"));
        }
    }

    // The address, as the tables describe it.
    if let Some(ip) = ip {
        let addr = ip.ip.as_str();
        let desc = ip.org.as_deref().unwrap_or("unknown network");
        let scope = actor.clone().unwrap_or_else(|| kind.clone());
        if ip.tor {
            notes.push(format!("{addr} is a Tor exit ({desc})"));
        }
        if ip.vpn && !ip.ours {
            notes.push(format!("{addr} is a VPN address ({desc})"));
        }
        let hosting = ip.hosting.as_deref();
        let country = ip.country.as_deref();
        let foreign = country.is_some_and(|c| !p.countries.is_empty() && !p.countries.contains(c));
        match who {
            // A person: hosting or a country the org is not in is worth a look.
            Some("member") | Some("unknown") if !ip.ours => {
                if let Some(h) = hosting {
                    notes.push(format!("{scope} acting from {h} hosting, {addr} ({desc})"));
                }
                if foreign {
                    notes.push(format!(
                        "{scope} acting from {}, {addr} ({desc})",
                        country.unwrap_or("?")
                    ));
                }
            }
            // A service runs in the org's cloud, or from its own addresses.
            Some("service") if !ip.ours && hosting != p.cloud.as_deref() => {
                let where_ = hosting.map_or_else(
                    || country.unwrap_or("an unknown network").to_string(),
                    |h| format!("{h} hosting"),
                );
                notes.push(format!(
                    "service {scope} acting from {where_}, {addr} ({desc}), not the org's {}",
                    p.cloud.as_deref().unwrap_or("cloud")
                ));
            }
            // A key calls from the networks the org expects it from.
            Some("key") if !ip.ours => {
                let nets = actor.as_deref().and_then(|a| p.keys.get(a));
                let known = nets.is_none_or(|nets| {
                    nets.is_empty()
                        || addr
                            .parse::<IpAddr>()
                            .is_ok_and(|a| nets.iter().any(|n| n.contains(&a)))
                });
                facts.insert("key_known_network".into(), json!(known));
                if !known {
                    notes.push(format!(
                        "key {scope} used from {addr} ({desc}), outside the networks it calls from"
                    ));
                }
            }
            _ => {}
        }
        facts.insert("ip".into(), Value::Object(ip.facts()));
    }

    // Where the request went.
    if let Some(d) = &n.dest {
        let known = p.hosts.contains(d)
            || p.hosts.iter().any(|h| {
                h.strip_prefix("*.")
                    .is_some_and(|suffix| d.ends_with(suffix))
            });
        facts.insert("dest_known".into(), json!(known));
        if !known {
            let by = actor.clone().unwrap_or_else(|| source.to_string());
            notes.push(format!(
                "{by} talked to {d}, not a host this org's systems talk to"
            ));
        }
    }

    // What it touched.
    if let Some(t) = &n.target {
        if p.sensitive.iter().any(|s| t == s || t.contains(s.as_str())) {
            facts.insert("sensitive_target".into(), json!(true));
            let by = actor.clone().unwrap_or_else(|| source.to_string());
            notes.push(format!("{by} touched {t}, which the org marks sensitive"));
        }
    }
    facts.insert("notes".into(), json!(notes));
    facts
}

/// `_norm` keeps what routes a record (actor, org), the actor's person, and
/// what the log does not already say: a kind, address or target copied from a
/// field goes.
fn unrepeat(rec: &Map<String, Value>, stamp: &Stamp, n: &mut Norm) {
    fn values(v: &Value, out: &mut BTreeSet<String>) {
        match v {
            Value::Object(m) => m.values().for_each(|x| values(x, out)),
            Value::Array(a) => a.iter().for_each(|x| values(x, out)),
            _ => {
                out.insert(v.to_string());
            }
        }
    }
    let mut logged = BTreeSet::new();
    rec.values().for_each(|v| values(v, &mut logged));
    for s in [&stamp.ts, &stamp.source] {
        logged.insert(Value::String(s.clone()).to_string());
    }
    // actor, org and person always stay: they route the record
    for f in [
        &mut n.kind,
        &mut n.ip,
        &mut n.country,
        &mut n.dest,
        &mut n.target,
    ] {
        if f.as_ref()
            .is_some_and(|v| logged.contains(&Value::String(v.clone()).to_string()))
        {
            *f = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tables(table: &str, key: &str) -> Option<String> {
        let answer = match (table, key) {
            ("profile", "") => {
                r#"{"org":"acme","own_sources":["cloudtrail"],"countries":["US"],"cloud":"aws",
                   "members":["alice@example.com"],"services":["deploy-bot"],
                   "keys":{"ak_1":["203.0.113.0/24"]},"about":{"VendorRole":"the vendor"},
                   "hosts":["*.example.internal"],"sensitive":["prod-db"],"watch":["StopLogging"],
                   "networks":["10.0.0.0/8"],"people":{"carol@example.com":["Carol-GH"]}}"#
            }
            ("profile", "networks") => r#"["10.0.0.0/8"]"#,
            ("geo", "198.51.100.7") => r#"{"country":{"iso_code":"NL"}}"#,
            ("asn", "198.51.100.7") => r#"{"autonomous_system_organization":"Example Hosting"}"#,
            ("networks", "198.51.100.7") => r#"["datacenter","tor"]"#,
            _ => return None,
        };
        Some(answer.to_string())
    }

    fn written(rec: &str, n: Norm) -> Value {
        let mut out = Vec::new();
        let rec = normkit::record(rec).unwrap();
        Kit::new(tables)
            .write(&mut out, Stamp::of(&rec), rec, n)
            .unwrap();
        assert_eq!(out.last(), Some(&b'\n'));
        serde_json::from_slice(&out).unwrap()
    }

    #[test]
    fn a_member_from_a_tor_exit_abroad() {
        let n = Norm {
            actor: Some("alice@example.com".into()),
            org: Some("111122223333".into()),
            kind: Some("StopLogging".into()),
            ip: Some("198.51.100.7".into()),
            ..Norm::default()
        };
        let rec = written(
            r#"{"source":"cloudtrail","eventName":"StopLogging","sourceIPAddress":"198.51.100.7"}"#,
            n,
        );
        assert_eq!(
            rec["_norm"],
            json!({"actor": "alice@example.com", "org": "acme"})
        );
        assert_eq!(
            rec["_facts"],
            json!({
                "watch": true,
                "principal": "member",
                "ip": {"country": "NL", "org": "Example Hosting", "hosting": "datacenter", "tor": true},
                "notes": [
                    "StopLogging is a kind the org watches every time",
                    "198.51.100.7 is a Tor exit (Example Hosting)",
                    "alice@example.com acting from datacenter hosting, 198.51.100.7 (Example Hosting)",
                    "alice@example.com acting from NL, 198.51.100.7 (Example Hosting)",
                ],
            })
        );
    }

    #[test]
    fn a_pack_s_facts_come_after_the_watch_note() {
        let mut n = Norm {
            actor: Some("root".into()),
            kind: Some("StopLogging".into()),
            principal: Some("aws"),
            ..Norm::default()
        };
        n.fact("root", "AWS root credentials used");
        let rec = written(
            r#"{"source":"cloudtrail","_facts":{"notes":["forged"]}}"#,
            n,
        );
        assert_eq!(rec["_facts"]["root"], json!(true));
        assert_eq!(rec["_facts"]["principal"], json!("aws"));
        assert_eq!(
            rec["_facts"]["notes"],
            json!([
                "StopLogging is a kind the org watches every time",
                "AWS root credentials used"
            ])
        );
        assert_eq!(
            rec["_norm"],
            json!({"actor": "root", "kind": "StopLogging", "org": "acme"}),
            "cloudtrail is the org's own"
        );
    }

    #[test]
    fn no_principal_a_key_and_where_it_went() {
        let n = Norm {
            actor: Some("acme/web".into()),
            principal: Some("none"),
            dest: Some("paste.example.com".into()),
            target: Some("prod-db-replica".into()),
            ..Norm::default()
        };
        let rec = written(
            r#"{"source":"runner_egress","dst_host":"paste.example.com"}"#,
            n,
        );
        assert_eq!(
            rec["_facts"],
            json!({
                "dest_known": false,
                "sensitive_target": true,
                "notes": [
                    "acme/web talked to paste.example.com, not a host this org's systems talk to",
                    "acme/web touched prod-db-replica, which the org marks sensitive",
                ],
            })
        );
        assert_eq!(
            rec["_norm"],
            json!({"actor": "acme/web", "target": "prod-db-replica"}),
            "the dest the log holds goes"
        );

        let n = Norm {
            actor: Some("ak_1".into()),
            ip: Some("10.1.2.3".into()),
            ..Norm::default()
        };
        let rec = written(r#"{"source":"app"}"#, n);
        assert_eq!(rec["_facts"]["principal"], json!("key"));
        assert_eq!(rec["_facts"]["ip"], json!({"ours": true, "private": true}));
        assert!(rec["_facts"].get("key_known_network").is_none(), "ours");

        // a dual-stack server's IPv4, as IPv4
        for ip in ["::ffff:203.0.113.9", "[::ffff:203.0.113.9]:443"] {
            let n = Norm {
                actor: Some("ak_1".into()),
                ip: Some(ip.into()),
                ..Norm::default()
            };
            let rec = written(r#"{"source":"app"}"#, n);
            assert_eq!(rec["_facts"]["key_known_network"], json!(true), "{ip}");
            assert_eq!(rec["_facts"]["notes"], json!([]), "{ip}");
        }
        let n = Norm {
            actor: Some("ak_1".into()),
            ip: Some("::ffff:198.51.100.7".into()),
            ..Norm::default()
        };
        let rec = written(r#"{"source":"app"}"#, n);
        assert_eq!(rec["_facts"]["key_known_network"], json!(false));
        assert_eq!(
            rec["_facts"]["notes"][0],
            json!("198.51.100.7 is a Tor exit (Example Hosting)")
        );
    }

    #[test]
    fn more_addresses_than_one() {
        let mut n = Norm {
            actor: Some("VendorRole".into()),
            ..Norm::default()
        };
        n.address("client.ipChain[1].ip", "198.51.100.7");
        n.address("nothing", "not an address");
        let rec = written(r#"{"source":"okta"}"#, n);
        assert_eq!(
            rec["_facts"]["ips"],
            json!({"client.ipChain[1].ip": {"country": "NL", "org": "Example Hosting", "hosting": "datacenter", "tor": true}})
        );
        assert_eq!(rec["_facts"]["about"], json!("the vendor"));
        assert_eq!(rec["_facts"]["notes"], json!([]));
    }

    #[test]
    fn the_person_an_actor_is() {
        let actor = |a: &str| Norm {
            actor: Some(a.into()),
            ..Norm::default()
        };
        let rec = written(
            r#"{"source":"github_audit","actor":"carol-gh"}"#,
            actor("carol-gh"),
        );
        assert_eq!(
            rec["_norm"],
            json!({"actor": "carol-gh", "person": "carol@example.com"})
        );
        let rec = written(
            r#"{"source":"okta","user":"carol@example.com"}"#,
            actor("Carol@example.com"),
        );
        assert_eq!(
            rec["_norm"],
            json!({"actor": "Carol@example.com", "person": "carol@example.com"}),
            "the person's own name, kept though the log holds it"
        );
        let rec = written(r#"{"source":"okta"}"#, actor("carol"));
        assert_eq!(
            rec["_norm"],
            json!({"actor": "carol"}),
            "no one the map names"
        );
    }

    /// `ts` and `source` come from the record the pack read, whatever its
    /// trim kept; a record with no `ts` gets the time it was read.
    #[test]
    fn every_record_has_ts_source_and_norm() {
        let read =
            normkit::record(r#"{"ts":"2026-10-20T13:00:00Z","source":"okta","x":1}"#).unwrap();
        let mut out = Vec::new();
        let kit = Kit::new(|_, _| None);
        kit.write(&mut out, Stamp::of(&read), Map::new(), Norm::default())
            .unwrap();
        let v: Value = serde_json::from_slice(&out).unwrap();
        assert_eq!(v["ts"], "2026-10-20T13:00:00Z");
        assert_eq!(v["source"], "okta");
        assert!(v["_norm"].is_object() && v.get("x").is_none(), "{v}");

        let read = normkit::record(r#"{"source":"app","ts":1760965200}"#).unwrap();
        let mut out = Vec::new();
        kit.write(&mut out, Stamp::of(&read), read.clone(), Norm::default())
            .unwrap();
        let v: Value = serde_json::from_slice(&out).unwrap();
        let ts = v["ts"].as_str().unwrap();
        assert!(
            ts.len() == 24 && ts.starts_with("20") && ts.ends_with('Z'),
            "{ts}"
        );
    }

    #[test]
    fn no_profile_no_tables() {
        let mut out = Vec::new();
        let n = Norm {
            actor: Some("bob".into()),
            ip: Some("192.0.2.1".into()),
            ..Norm::default()
        };
        let rec = normkit::record(r#"{"ts":"2026-10-20T13:00:00Z","source":"app"}"#).unwrap();
        Kit::new(|_, _| None)
            .write(&mut out, Stamp::of(&rec), rec, n)
            .unwrap();
        assert_eq!(
            String::from_utf8(out).unwrap(),
            "{\"ts\":\"2026-10-20T13:00:00Z\",\"source\":\"app\",\"_norm\":{\"actor\":\"bob\",\"ip\":\"192.0.2.1\"},\"_facts\":{\"ip\":{},\"notes\":[\"principal bob is nobody the org profile knows\"],\"principal\":\"unknown\"}}\n"
        );
    }
}
