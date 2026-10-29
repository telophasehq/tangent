//! `_facts`: what the profile (`/data/profile.yaml`, or the same keys under `config:`) says about a record.

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
    watch: BTreeSet<String>,
    members: BTreeSet<String>,
    services: BTreeSet<String>,
    keys: BTreeMap<String, Vec<IpNet>>, // empty: any network
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
    if p.services.contains(actor) {
        "service"
    } else if p.members.contains(actor) {
        "member"
    } else if p.keys.contains_key(actor) {
        "key"
    } else {
        "unknown"
    }
}

/// What the profile knows about this record, and the notes a model reads it with.
fn facts_of(p: &Profile, rec: &Value, ip: Option<Value>) -> (Map<String, Value>, Vec<String>) {
    let mut facts = Map::new();
    let mut notes = Vec::new();
    let source = rec["source"].as_str().unwrap_or("");
    let kind = lookup(rec, "_norm.kind").and_then(text).unwrap_or_default();
    let actor = lookup(rec, "_norm.actor").and_then(text);
    let dest = lookup(rec, "_norm.dest").and_then(text);
    let target = lookup(rec, "_norm.target").and_then(text);

    if p.watch.contains(&kind) {
        facts.insert("watch".into(), json!(true));
        notes.push(format!("{kind} is a kind the org watches every time"));
    }
    if rec["userIdentity"]["type"].as_str() == Some("Root") {
        facts.insert("root".into(), json!(true));
        notes.push("AWS root credentials used".into());
    }
    if rec["requestParameters"].to_string().contains("0.0.0.0/0") {
        facts.insert("world_open".into(), json!(true));
        notes.push("opened to 0.0.0.0/0".into());
    }

    // The principal: known to the profile, or nobody it knows.
    let who = actor
        .as_deref()
        .filter(|_| !NO_PRINCIPAL.contains(&source))
        .map(|a| principal(p, a));
    if let (Some(a), Some(w)) = (&actor, who) {
        facts.insert("principal".into(), json!(w));
        if w == "unknown" {
            notes.push(format!("principal {a} is nobody the org profile knows"));
        }
    }

    // The address, as `enrich` described it.
    if let Some(Value::Object(ip)) = ip {
        let ours = ip.get("ours").and_then(Value::as_bool).unwrap_or(false);
        let addr = ip.get("ip").and_then(Value::as_str).unwrap_or("?");
        let desc = ip.get("org").and_then(Value::as_str).unwrap_or("unknown network");
        let scope = actor.clone().unwrap_or_else(|| kind.clone());
        if ip.get("tor").and_then(Value::as_bool) == Some(true) {
            notes.push(format!("{addr} is a Tor exit ({desc})"));
        }
        if ip.get("vpn").and_then(Value::as_bool) == Some(true) && !ours {
            notes.push(format!("{addr} is a VPN address ({desc})"));
        }
        let hosting = ip.get("hosting").and_then(Value::as_str);
        let country = ip.get("country").and_then(Value::as_str);
        let foreign = country.is_some_and(|c| !p.countries.is_empty() && !p.countries.contains(c));
        match who {
            // A person: hosting or a country the org is not in is worth a look.
            Some("member") | Some("unknown") if !ours => {
                if let Some(h) = hosting {
                    notes.push(format!("{scope} acting from {h} hosting, {addr} ({desc})"));
                }
                if foreign {
                    notes.push(format!("{scope} acting from {}, {addr} ({desc})", country.unwrap_or("?")));
                }
            }
            // A service runs in the org's cloud, or from its own addresses.
            Some("service") if !ours && hosting != p.cloud.as_deref() => {
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
                    notes.push(format!("key {scope} used from {addr} ({desc}), outside the networks it calls from"));
                }
            }
            _ => {}
        }
        facts.insert("ip".into(), Value::Object(ip));
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
            notes.push(format!("{by} talked to {d}, not a host this org's systems talk to"));
        }
    }

    // What it touched.
    if let Some(t) = &target {
        if p.sensitive.iter().any(|s| t == s || t.contains(s.as_str())) {
            facts.insert("sensitive_target".into(), json!(true));
            let by = actor.clone().unwrap_or_else(|| source.to_string());
            notes.push(format!("{by} touched {t}, which the org marks sensitive"));
        }
    }
    (facts, notes)
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
            let Some(m) = rec.as_object_mut() else {
                return Err("record is not a JSON object".to_string());
            };
            let ip = m.get_mut("_ip").and_then(Value::as_object_mut).and_then(|i| i.remove(&p.ip_field));
            if m.get("_ip").and_then(Value::as_object).is_some_and(Map::is_empty) {
                m.remove("_ip");
            }
            let (mut facts, notes) = facts_of(p, &rec, ip);
            facts.insert("notes".into(), json!(notes));
            rec["_facts"] = Value::Object(facts);
            out.extend(serde_json::to_vec(&rec).map_err(|e| e.to_string())?);
            out.push(b'\n');
        }
        Ok(out)
    }
}
