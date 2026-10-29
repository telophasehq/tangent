//! What the org's profile and the address data say about a record, for the
//! models downstream. Nothing is decided here and nothing is dropped: every
//! record goes on with `_facts` attached. Upstream plugins normalized the
//! record (`_norm`) and decorated its address (`_ip`, from `examples/enrich`:
//! country, ASN, hosting, Tor, VPN, ours). This plugin holds the profile
//! (where the people are, who the members, services and keys are, which hosts
//! the systems talk to, which assets are sensitive, which kinds are watched)
//! and writes what it knows:
//!   - `principal`: `member`, `service`, `key`, `aws` (AWS acting on its own,
//!     an actor `aws:<service>`), or `unknown`;
//!   - `ip`: the address as decorated; `key_known_network` for a key;
//!   - `dest_known`, `sensitive_target`, `watch`, `root`, `world_open`;
//!   - `notes`: the same, in sentences a model reads.
//!
//! The profile is `profile.yaml` in the plugin's data directory (`data:`),
//! or the same keys under `plugins.facts.config`:
//!   countries: [US]     cloud: aws       watch: [StopLogging, ...]
//!   members: [...]      services: [...]  keys: {ak_1: [1.2.3.0/24]}
//!   hosts: [...]        sensitive: [...] ip: _norm.ip     # the field `enrich` decorated

wit_bindgen::generate!({
    path: ".tangent/wit",
    world: "processor",
    generate_all,
});

use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;
use std::sync::OnceLock;

use ipnet::IpNet;

use exports::tangent::logs::mapper::{Guest, Meta, Pred, Selector};
use serde_json::{json, Map, Value};
use tangent::logs::log::Logview;
use tangent::logs::config;

struct Component;

export!(Component);

/// Sources whose `_norm.actor` is a repository or nothing, not a principal.
const NO_PRINCIPAL: &[&str] = &["hiro", "gha_ci", "runner_egress"];

struct Profile {
    countries: BTreeSet<String>,
    /// Kinds the org wants seen every time, however usual.
    watch: BTreeSet<String>,
    members: BTreeSet<String>,
    services: BTreeSet<String>,
    /// The org's API keys and the networks each calls from (`keys: {ak_1: [1.2.3.0/24]}`;
    /// a plain list means any network).
    keys: BTreeMap<String, Vec<IpNet>>,
    /// Where the org's services run (`aws`): a service seen elsewhere is news.
    cloud: Option<String>,
    hosts: BTreeSet<String>,
    sensitive: BTreeSet<String>,
    ip_field: String,
}

fn setting(name: &str) -> Option<String> {
    let raw = config::get(name)?;
    let t = raw.trim();
    let t = t
        .strip_prefix('"')
        .and_then(|s| s.strip_suffix('"'))
        .unwrap_or(t);
    (!t.is_empty()).then(|| t.to_string())
}

fn list_setting(name: &str) -> BTreeSet<String> {
    let Some(raw) = setting(name) else {
        return BTreeSet::new();
    };
    if raw.starts_with('[') {
        if let Ok(v) = serde_json::from_str::<Vec<String>>(&raw) {
            return v.into_iter().collect();
        }
    }
    raw.split(',')
        .map(|s| s.trim().trim_matches('"').to_string())
        .filter(|s| !s.is_empty())
        .collect()
}

/// `10.0.0.0/8`, or a bare address as its own /32 or /128.
fn parse_net(s: &str) -> Option<IpNet> {
    let s = s.trim();
    s.parse::<IpNet>()
        .ok()
        .or_else(|| s.parse::<IpAddr>().ok().map(IpNet::from))
}

/// `keys`: a map of key to its networks, or a list of keys (any network).
fn keys_setting() -> BTreeMap<String, Vec<IpNet>> {
    let Some(raw) = setting("keys") else {
        return BTreeMap::new();
    };
    if raw.starts_with('{') {
        if let Ok(m) = serde_json::from_str::<BTreeMap<String, Vec<String>>>(&raw) {
            return m
                .into_iter()
                .map(|(k, nets)| (k, nets.iter().filter_map(|n| parse_net(n)).collect()))
                .collect();
        }
    }
    list_setting("keys").into_iter().map(|k| (k, Vec::new())).collect()
}

/// `/data/profile.yaml`, when the plugin was given a data directory.
fn profile_file() -> Option<Value> {
    let text = std::fs::read_to_string("/data/profile.yaml").ok()?;
    match serde_yaml::from_str::<Value>(&text) {
        Ok(v) => Some(v),
        Err(e) => {
            eprintln!("facts: profile.yaml: {e}");
            None
        }
    }
}

fn strs(v: &Value) -> BTreeSet<String> {
    v.as_array()
        .map(|a| a.iter().filter_map(text).collect())
        .unwrap_or_default()
}

fn profile() -> &'static Profile {
    static P: OnceLock<Profile> = OnceLock::new();
    P.get_or_init(|| match profile_file() {
        Some(f) => Profile {
            countries: strs(&f["countries"]),
            watch: strs(&f["watch"]),
            members: strs(&f["members"]),
            services: strs(&f["services"]),
            keys: match &f["keys"] {
                Value::Object(m) => m
                    .iter()
                    .map(|(k, nets)| (k.clone(), strs(nets).iter().filter_map(|n| parse_net(n)).collect()))
                    .collect(),
                other => strs(other).into_iter().map(|k| (k, Vec::new())).collect(),
            },
            cloud: f["cloud"].as_str().map(str::to_string),
            hosts: strs(&f["hosts"]),
            sensitive: strs(&f["sensitive"]),
            ip_field: f["ip"].as_str().unwrap_or("_norm.ip").to_string(),
        },
        None => Profile {
            countries: list_setting("countries"),
            watch: list_setting("watch"),
            members: list_setting("members"),
            services: list_setting("services"),
            keys: keys_setting(),
            cloud: setting("cloud"),
            hosts: list_setting("hosts"),
            sensitive: list_setting("sensitive"),
            ip_field: setting("ip").unwrap_or_else(|| "_norm.ip".to_string()),
        },
    })
}



fn lookup<'a>(v: &'a Value, path: &str) -> Option<&'a Value> {
    if let Some(direct) = v.get(path) {
        return Some(direct);
    }
    let mut cur = v;
    for seg in path.split('.') {
        cur = cur.get(seg)?;
    }
    Some(cur)
}

fn text(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// Who the actor is to the profile.
fn principal(p: &Profile, actor: &str) -> &'static str {
    if actor.starts_with("aws:") {
        "aws"
    } else if p.services.contains(actor) {
        "service"
    } else if p.members.contains(actor) {
        "member"
    } else if p.keys.contains_key(actor) {
        "key"
    } else {
        "unknown"
    }
}

/// What the profile knows about this record, and the notes a model reads it
/// with. The second element is (sentence, class); only the sentences go on.
fn facts_of(p: &Profile, rec: &Value) -> (Map<String, Value>, Vec<(String, String)>) {
    let mut facts = Map::new();
    let mut reasons: Vec<(String, String)> = Vec::new();
    let source = rec["source"].as_str().unwrap_or("");
    let kind = lookup(rec, "_norm.kind").and_then(text).unwrap_or_default();
    let actor = lookup(rec, "_norm.actor").and_then(text);
    let dest = lookup(rec, "_norm.dest").and_then(text);
    let target = lookup(rec, "_norm.target").and_then(text);

    if p.watch.contains(&kind) {
        facts.insert("watch".into(), json!(true));
        reasons.push((format!("{kind} is a kind the org watches every time"), format!("watch:{kind}")));
    }
    if rec["userIdentity"]["type"].as_str() == Some("Root") {
        facts.insert("root".into(), json!(true));
        reasons.push(("AWS root credentials used".into(), "root".into()));
    }
    if rec["requestParameters"].to_string().contains("0.0.0.0/0") {
        facts.insert("world_open".into(), json!(true));
        reasons.push(("opened to 0.0.0.0/0".into(), "world".into()));
    }

    // The principal: known to the profile, or nobody it knows.
    let who = actor
        .as_deref()
        .filter(|_| !NO_PRINCIPAL.contains(&source))
        .map(|a| principal(p, a));
    if let (Some(a), Some(w)) = (&actor, who) {
        facts.insert("principal".into(), json!(w));
        if w == "unknown" {
            reasons.push((
                format!("principal {a} is nobody the org profile knows"),
                format!("principal:{a}"),
            ));
        }
    }

    // The address, as `enrich` described it.
    if let Some(ip) = rec["_ip"][&p.ip_field].as_object() {
        facts.insert("ip".into(), Value::Object(ip.clone()));
        let ours = ip.get("ours").and_then(Value::as_bool).unwrap_or(false);
        let addr = ip.get("ip").and_then(Value::as_str).unwrap_or("?");
        let desc = ip.get("org").and_then(Value::as_str).unwrap_or("unknown network");
        let scope = actor.clone().unwrap_or_else(|| kind.clone());
        if ip.get("tor").and_then(Value::as_bool) == Some(true) {
            reasons.push((format!("{addr} is a Tor exit ({desc})"), format!("ip:tor:{scope}")));
        }
        if ip.get("vpn").and_then(Value::as_bool) == Some(true) && !ours {
            reasons.push((format!("{addr} is a VPN address ({desc})"), format!("ip:vpn:{scope}")));
        }
        let hosting = ip.get("hosting").and_then(Value::as_str);
        let country = ip.get("country").and_then(Value::as_str);
        let foreign = country.is_some_and(|c| !p.countries.is_empty() && !p.countries.contains(c));
        match who {
            // A person: hosting or a country the org is not in is worth a look.
            Some("member") | Some("unknown") if !ours => {
                if let Some(h) = hosting {
                    reasons.push((
                        format!("{scope} acting from {h} hosting, {addr} ({desc})"),
                        format!("ip:hosting:{scope}"),
                    ));
                }
                if foreign {
                    reasons.push((
                        format!("{scope} acting from {}, {addr} ({desc})", country.unwrap_or("?")),
                        format!("ip:country:{scope}"),
                    ));
                }
            }
            // A service runs in the org's cloud, or from its own addresses.
            Some("service") if !ours && hosting != p.cloud.as_deref() => {
                let where_ = hosting.map_or_else(
                    || country.unwrap_or("an unknown network").to_string(),
                    |h| format!("{h} hosting"),
                );
                reasons.push((
                    format!(
                        "service {scope} acting from {where_}, {addr} ({desc}), not the org's {}",
                        p.cloud.as_deref().unwrap_or("cloud")
                    ),
                    format!("ip:service:{scope}"),
                ));
            }
            // A key calls from the networks the org expects it from.
            Some("key") if !ours => {
                let nets = actor.as_deref().and_then(|a| p.keys.get(a));
                let known = nets.is_none_or(|nets| {
                    nets.is_empty()
                        || addr
                            .parse::<IpAddr>()
                            .is_ok_and(|a| nets.iter().any(|n| n.contains(&a)))
                });
                facts.insert("key_known_network".into(), json!(known));
                if !known {
                    reasons.push((
                        format!("key {scope} used from {addr} ({desc}), outside the networks it calls from"),
                        format!("ip:key:{scope}"),
                    ));
                }
            }
            _ => {}
        }
    }

    // Where the request went.
    if let Some(d) = &dest {
        let known = p.hosts.contains(d) || p.hosts.iter().any(|h| {
            h.strip_prefix("*.")
                .is_some_and(|suffix| d.ends_with(suffix))
        });
        facts.insert("dest_known".into(), json!(known));
        if !known {
            let by = actor.clone().unwrap_or_else(|| source.to_string());
            reasons.push((
                format!("{by} talked to {d}, not a host this org's systems talk to"),
                format!("dest:{d}"),
            ));
        }
    }

    // What it touched.
    if let Some(t) = &target {
        if p.sensitive.iter().any(|s| t == s || t.contains(s.as_str())) {
            facts.insert("sensitive_target".into(), json!(true));
            let by = actor.clone().unwrap_or_else(|| source.to_string());
            reasons.push((
                format!("{by} touched {t}, which the org marks sensitive"),
                format!("sensitive:{by}:{t}"),
            ));
        }
    }
    (facts, reasons)
}

impl Guest for Component {
    fn metadata() -> Meta {
        Meta {
            name: "facts".to_string(),
            version: "0.1.0".to_string(),
        }
    }

    fn probe() -> Vec<Selector> {
        vec![Selector {
            any: Vec::new(),
            all: vec![Pred::Has("_norm".to_string())],
            none: Vec::new(),
        }]
    }

    fn process_logs(input: Vec<Logview>) -> Result<Vec<u8>, String> {
        let p = profile();
        let mut out = Vec::new();
        for lv in input {
            let mut rec: Value = serde_json::from_str(&lv.log()).map_err(|e| e.to_string())?;
            let (mut facts, notes) = facts_of(p, &rec);
            let notes: Vec<String> = notes.into_iter().map(|(text, _)| text).collect();
            facts.insert("notes".into(), json!(notes));
            rec["_facts"] = Value::Object(facts);
            out.extend(serde_json::to_vec(&rec).map_err(|e| e.to_string())?);
            out.push(b'\n');
        }
        Ok(out)
    }
}
