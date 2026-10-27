//! Which records a model should read, decided from context, not statistics.
//! Upstream plugins normalized the record (`_norm`) and decorated its address
//! (`_ip`, from `examples/enrich`: country, ASN, hosting, Tor, VPN, ours). This
//! plugin holds the org's profile from config (where its people are, who its
//! members, services and keys are, which hosts its systems talk to, which
//! assets are sensitive) and passes a record when:
//!   - its kind is on the always-look list, or a hard rule names it (root
//!     credentials, something opened to the world);
//!   - its address is a Tor exit or a VPN; for a person, hosting or a country
//!     the org is not in; for a service, anywhere but the org's cloud; for one
//!     of the org's API keys, a network it does not call from;
//!   - its principal is nobody the profile knows;
//!   - it talks to a host the org's systems do not talk to;
//!   - it touches an asset the profile marks sensitive.
//! Each reason fires once per partition per hour of stream time. The reasons
//! and the context they rest on land under `_gate`; everything else is dropped.
//!
//! The profile is `profile.yaml` in the plugin's data directory (`data:`),
//! or the same keys under `plugins.gate.config`:
//!   countries: [US]     cloud: aws
//!   members: [...]      services: [...]      keys: {ak_1: [1.2.3.0/24]}
//!   hosts: [...]        sensitive: [...]     ip: _norm.ip     # the field `enrich` decorated

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
use tangent::logs::log::{Logview, Scalar};
use tangent::logs::{config, state};

struct Component;

export!(Component);

const HOUR_MS: i64 = 3_600_000;

/// Kinds a person looks at every time, however usual they are.
const WATCH: &[&str] = &[
    // CloudTrail
    "StopLogging",
    "DeleteTrail",
    "PutBucketPolicy",
    "PutBucketPublicAccessBlock",
    "AuthorizeSecurityGroupIngress",
    "ModifyDBSnapshotAttribute",
    "CreateUser",
    "CreateAccessKey",
    "AttachUserPolicy",
    // Okta
    "user.mfa.factor.deactivate",
    "system.api_token.create",
    // GitHub audit log
    "protected_branch.destroy",
    "deploy_key.create",
    "org.update_member",
    "org.self_hosted_runner_register",
    "repo.rename",
    "repo.access",
    "pull_request_target",
    // Google Workspace
    "EMAIL_FORWARDING_OUT_OF_DOMAIN",
    // the app's own audit log
    "member.role.changed",
    "org.settings.changed",
    "api_key.create",
    "org.export.started",
    // Hiro
    "aws_api_call",
    "step_failed",
];

/// Sources whose `_norm.actor` is a repository or nothing, not a principal.
const NO_PRINCIPAL: &[&str] = &["hiro", "gha_ci", "runner_egress"];

struct Profile {
    countries: BTreeSet<String>,
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
            eprintln!("gate: profile.yaml: {e}");
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

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146097 + doe - 719468
}

/// `2026-10-20T13:00:00Z` (fractions and offsets allowed) as epoch ms.
fn parse_ts(s: &str) -> Option<i64> {
    let s = s.trim();
    if let Ok(n) = s.parse::<f64>() {
        return Some(if n.abs() > 1e11 { n as i64 } else { (n * 1000.0) as i64 });
    }
    let num = |a: usize, l: usize| s.get(a..a + l)?.parse::<i64>().ok();
    let (y, mo, d) = (num(0, 4)?, num(5, 2)?, num(8, 2)?);
    let (h, mi, se) = (num(11, 2)?, num(14, 2)?, num(17, 2)?);
    let b = s.as_bytes();
    let mut i = 19;
    let mut ms = 0;
    if b.get(i) == Some(&b'.') {
        i += 1;
        let start = i;
        while i < b.len() && b[i].is_ascii_digit() {
            i += 1;
        }
        let frac3: String = format!("{:0<3}", &s[start..i]).chars().take(3).collect();
        ms = frac3.parse::<i64>().unwrap_or(0);
    }
    let offset = match b.get(i) {
        None | Some(b'Z') | Some(b'z') => 0,
        Some(sign @ (b'+' | b'-')) => {
            let oh = num(i + 1, 2)?;
            let om = num(i + 4, 2)?;
            (oh * 3600 + om * 60) * if *sign == b'+' { 1 } else { -1 }
        }
        _ => return None,
    };
    Some((days_from_civil(y, mo, d) * 86400 + h * 3600 + mi * 60 + se - offset) * 1000 + ms)
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

/// Why a model should read this record, if it should, and the context the
/// reasons rest on.
fn judge_worthy(p: &Profile, rec: &Value) -> (Map<String, Value>, Vec<(String, String)>) {
    let mut facts = Map::new();
    let mut reasons: Vec<(String, String)> = Vec::new();
    let source = rec["source"].as_str().unwrap_or("");
    let kind = lookup(rec, "_norm.kind").and_then(text).unwrap_or_default();
    let actor = lookup(rec, "_norm.actor").and_then(text);
    let dest = lookup(rec, "_norm.dest").and_then(text);
    let target = lookup(rec, "_norm.target").and_then(text);

    if WATCH.contains(&kind.as_str()) {
        reasons.push((format!("watch {kind}"), format!("watch:{kind}")));
    }
    if rec["userIdentity"]["type"].as_str() == Some("Root") {
        reasons.push(("rule: AWS root credentials used".into(), "rule:root".into()));
    }
    if rec["requestParameters"].to_string().contains("0.0.0.0/0") {
        reasons.push(("rule: opened to 0.0.0.0/0".into(), "rule:world".into()));
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
            name: "gate".to_string(),
            version: "0.2.0".to_string(),
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
            let ts = rec["ts"].as_str().and_then(parse_ts).unwrap_or(0);
            let (facts, reasons) = judge_worthy(p, &rec);
            let mut fresh = Vec::new();
            for (text, class) in reasons {
                // Once an hour of stream time: the key holds when it last fired.
                let key = format!("gated:{class}");
                let fired = match state::get(&key)? {
                    Some(Scalar::Int(last)) => ts - last < HOUR_MS,
                    _ => false,
                };
                if !fired {
                    state::set(&key, &Scalar::Int(ts), None)?;
                    fresh.push(text);
                }
            }
            if fresh.is_empty() {
                continue;
            }
            rec["_gate"] = json!({"reasons": fresh, "facts": facts});
            out.extend(serde_json::to_vec(&rec).map_err(|e| e.to_string())?);
            out.push(b'\n');
        }
        Ok(out)
    }
}
