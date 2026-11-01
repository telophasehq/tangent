//! `_hist`: what state knew about an actor before the first of its records in a
//! call, the facts one window cannot give: who it is and who assumed it, how long
//! it has been seen, what it does for the first time, its volume and hours against
//! its own, and its person's sign-ins and credential changes in every source.
//! `mode: learn` folds every record into state and emits nothing (ingest); `header`
//! stamps the same `_hist` on each of an actor's records and writes nothing (the
//! judge); `both` is header, then learn (tests). Every read is cut at the window's
//! start, so nothing at or after it counts. Keys start with the org; `%` and `|`
//! in a value are %-escaped. People's names across sources: `people` in the
//! runtime's `profile` table, which learn files in state for the header.

wit_bindgen::generate!({
    path: ".tangent/wit",
    world: "processor",
    generate_all,
});

mod kinds;

use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv6Addr};
use std::sync::OnceLock;

use exports::tangent::logs::mapper::{Guest, Meta, Selector};
use kinds::{date, ev, quoted, Ev};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use tangent::logs::config;
use tangent::logs::enrichment::lookup;
use tangent::logs::log::{Logview, Scalar};
use tangent::logs::state;

struct Component;

export!(Component);

type R<T> = Result<T, String>;

const H: i64 = 3_600_000;
const D: i64 = 24 * H;
const SLOT: i64 = 300_000;
const BASE: i64 = 28 * D;
const AUTH: i64 = 72 * H;
const CHAIN: i64 = 12 * H; // the longest STS session
const RECENT: i64 = H; // first seen this recently is still new
const M: i64 = 60_000;
const SIGN_IN: i64 = 10 * M; // an Okta sign-in's several events
const MFA_NEAR: i64 = 5 * 60_000;
const FLOOR: f64 = 3.0; // events an hour below which volume is normal

trait Kv {
    fn get(&mut self, key: &str) -> R<Option<Scalar>>;
    fn set(&mut self, key: &str, v: Scalar) -> R<()>;
    fn incr(&mut self, key: &str, by: i64) -> R<()>;
    fn scan(&mut self, lo: &str, hi: &str) -> R<Vec<(String, Scalar)>>;
}

struct Wit;

impl Kv for Wit {
    fn get(&mut self, key: &str) -> R<Option<Scalar>> {
        state::get(key)
    }
    fn set(&mut self, key: &str, v: Scalar) -> R<()> {
        state::set(key, &v, Some(0))
    }
    fn incr(&mut self, key: &str, by: i64) -> R<()> {
        state::incr(key, by, Some(0)).map(|_| ())
    }
    fn scan(&mut self, lo: &str, hi: &str) -> R<Vec<(String, Scalar)>> {
        state::scan(lo, hi, 100_000)
    }
}

#[derive(Default)]
struct People {
    of: BTreeMap<String, String>,
    names: BTreeMap<String, BTreeSet<String>>,
}

impl People {
    /// The profile's `people`: each person's other names, a list or one name.
    /// What it cannot read is said, once per instance, on stderr.
    fn from_value(people: &Value) -> People {
        let mut p = People::default();
        let entries = match people {
            Value::Object(m) => m,
            Value::Null => return p,
            v => {
                eprintln!(
                    "history: profile `people` is {v}, not a map of person to names; ignored"
                );
                return p;
            }
        };
        for (person, names) in entries {
            let person = person.to_lowercase();
            let names: Vec<&Value> = match names {
                Value::Array(a) => a.iter().collect(),
                Value::String(_) => vec![names],
                Value::Null => Vec::new(),
                v => {
                    eprintln!(
                        "history: profile `people.{person}` is {v}, not a list of names; ignored"
                    );
                    Vec::new()
                }
            };
            let mut all = BTreeSet::new();
            for n in names {
                match n.as_str() {
                    Some(n) => {
                        all.insert(n.to_lowercase());
                    }
                    None => eprintln!(
                        "history: profile `people.{person}` holds {n}, not a name; ignored"
                    ),
                }
            }
            for n in &all {
                p.of.insert(n.clone(), person.clone());
            }
            all.insert(person.clone());
            p.names.insert(person, all);
        }
        p
    }

    fn person(&self, actor: &str) -> String {
        let a = actor.to_lowercase();
        self.of.get(&a).cloned().unwrap_or(a)
    }

    /// The people learn filed, `{}` before it has run.
    fn filed(kv: &mut impl Kv) -> R<People> {
        Ok(match kv.get(PEOPLE)? {
            Some(Scalar::Str(json)) => serde_json::from_str::<Value>(&json)
                .map(|v| People::from_value(&v))
                .unwrap_or_default(),
            _ => People::default(),
        })
    }
}

/// Where learn files the profile's `people` for the header: the judge reads
/// ingest's, and needs no profile of its own. No org's keys can be it: theirs
/// all have a `|`.
const PEOPLE: &str = "people";

/// The profile's `people`, as its JSON text and read.
struct Profile {
    text: String,
    people: People,
}

impl Profile {
    fn from_value(v: Value) -> Profile {
        let v = if v.is_null() { json!({}) } else { v };
        Profile {
            text: v.to_string(),
            people: People::from_value(&v),
        }
    }

    /// Filed when it differs from what is: the same text from every writer.
    fn file(&self, kv: &mut impl Kv) -> R<()> {
        match kv.get(PEOPLE)? {
            Some(Scalar::Str(t)) if t == self.text => Ok(()),
            _ => kv.set(PEOPLE, Scalar::Str(self.text.clone())),
        }
    }
}

/// Read once per instance from the `profile` table: learn's `people`. None
/// there, no one has another name.
fn profile() -> &'static Profile {
    static P: OnceLock<Profile> = OnceLock::new();
    P.get_or_init(|| {
        Profile::from_value(
            lookup("profile", "people")
                .and_then(|json| serde_json::from_str::<Value>(&json).ok())
                .unwrap_or(Value::Null),
        )
    })
}

#[derive(Clone, Copy, PartialEq)]
enum Mode {
    Learn,
    Header,
    Both,
}

impl Guest for Component {
    fn metadata() -> Meta {
        Meta {
            name: "history".to_string(),
            version: "0.1.0".to_string(),
        }
    }

    // Everything: records with no actor go on unchanged.
    fn probe() -> Vec<Selector> {
        vec![Selector {
            any: Vec::new(),
            all: Vec::new(),
            none: Vec::new(),
        }]
    }

    fn process_logs(input: Vec<Logview>) -> Result<Vec<u8>, String> {
        let mode = match config::get("mode")
            .as_deref()
            .map(|m| m.trim().trim_matches('"'))
        {
            Some("learn") => Mode::Learn,
            Some("header") => Mode::Header,
            Some("both") => Mode::Both,
            m => return Err(format!("history: mode {m:?}: learn, header or both")),
        };
        let lines: Vec<String> = input.iter().map(Logview::log).collect();
        // The judge's history reads only state: no profile, no lookup.
        let none = Profile::from_value(Value::Null);
        let profile = if mode == Mode::Header {
            &none
        } else {
            profile()
        };
        run(&mut Wit, profile, mode, &lines)
    }
}

/// Learn files the profile's `people` and reads with them; the header reads
/// with what learn filed, so ingest and the judge agree on who is who.
fn run(kv: &mut impl Kv, profile: &Profile, mode: Mode, lines: &[String]) -> R<Vec<u8>> {
    if mode != Mode::Header {
        profile.file(kv)?;
    }
    let filed;
    let people = if mode == Mode::Learn {
        &profile.people
    } else {
        filed = People::filed(kv)?;
        &filed
    };
    let recs: Vec<Option<Value>> = lines.iter().map(|l| serde_json::from_str(l).ok()).collect();
    let evs: Vec<Option<Ev>> = recs
        .iter()
        .map(|r| {
            let r = r.as_ref()?;
            let mut e = ev(r)?;
            e.person = people.person(&e.actor);
            if e.auth.is_some() {
                e.h = hash(&r.to_string());
            }
            Some(e)
        })
        .collect();
    let mut out = Vec::new();
    if mode != Mode::Learn {
        let mut groups: BTreeMap<(&str, &str), Vec<&Ev>> = BTreeMap::new();
        for e in evs.iter().flatten() {
            groups.entry((&e.org, &e.actor)).or_default().push(e);
        }
        let mut hists = BTreeMap::new();
        for (k, g) in &groups {
            if let Some(h) = header(kv, people, g)? {
                hists.insert(*k, Value::Object(h));
            }
        }
        for ((line, rec), e) in lines.iter().zip(&recs).zip(&evs) {
            let h = e
                .as_ref()
                .and_then(|e| hists.get(&(e.org.as_str(), e.actor.as_str())));
            match (rec, h) {
                (Some(Value::Object(m)), Some(h)) => {
                    let mut m = m.clone();
                    m.insert("_hist".into(), h.clone());
                    out.extend(serde_json::to_vec(&m).map_err(|e| e.to_string())?);
                }
                _ => out.extend(line.trim_end().as_bytes()),
            }
            out.push(b'\n');
        }
    }
    if mode != Mode::Header {
        learn(kv, people, &evs.iter().flatten().collect::<Vec<_>>())?;
    }
    Ok(out)
}

#[derive(Serialize, Deserialize)]
struct Assume {
    t: i64,
    by: String,
    #[serde(rename = "type")]
    typ: String,
    arn: String,
}

/// Every fold is a min, a sum or a row keyed by event time, so records arriving
/// in any order leave the same state. Only what succeeded is learned as seen:
/// anyone can fail a sign-in in someone's name, from anywhere.
fn learn(kv: &mut impl Kv, people: &People, evs: &[&Ev]) -> R<()> {
    let mut mins: BTreeMap<String, i64> = BTreeMap::new();
    let mut sums: BTreeMap<String, i64> = BTreeMap::new();
    let mut hours: BTreeMap<String, Vec<(i64, char)>> = BTreeMap::new();
    let mut sessions: BTreeMap<String, Vec<Assume>> = BTreeMap::new();
    let mut rows: BTreeMap<String, String> = BTreeMap::new();
    let mut low = |k: String, t: i64| {
        let v = mins.entry(k).or_insert(t);
        *v = (*v).min(t);
    };
    for e in evs {
        let (o, a, t) = (e.org.as_str(), e.actor.as_str(), e.ms);
        low(key(&[o, "a", a, "first"]), t);
        *sums
            .entry(key(&[o, "a", a, "d", &format!("{:05}", t.div_euclid(D))]))
            .or_default() += 1;
        hours
            .entry(key(&[o, "a", a, "h", &format!("{:07}", t.div_euclid(H))]))
            .or_default()
            .push((t.rem_euclid(H) / SLOT, e.cat));
        if e.ok {
            for (dim, v) in values(e) {
                low(key(&[o, "a", a, "n", dim, &v]), t);
            }
            low(key(&[o, "a", a, "n", "cat", &e.cat.to_string()]), t);
            for (dim, v) in nets(e) {
                low(key(&[o, "p", &e.person, "n", dim, &v]), t);
                low(key(&[o, "o", "n", dim, &v]), t);
            }
            if let Some(ip) = &e.ip {
                low(key(&[o, "o", "ip", ip, a]), t);
            }
        }
        if let Some(m) = e.mfa {
            low(
                key(&[o, "a", a, "n", "mfa", if m { "yes" } else { "no" }]),
                t,
            );
        }
        if let Some((acct, role, sess)) = &e.assume {
            let typ = e.typ.clone().unwrap_or_default();
            let x = Assume {
                t,
                by: a.into(),
                typ,
                arn: e.arn.clone(),
            };
            let hour = format!("{:07}", t.div_euclid(H));
            sessions
                .entry(key(&[o, "s", acct, role, sess, &hour]))
                .or_default()
                .push(x);
            low(key(&[o, "a", role, "n", "by", a]), t);
        }
        match &e.auth {
            // a count a minute: a spray is one row a minute, not one an attempt
            Some(x) if x.kind == "login failed" => {
                *sums
                    .entry(key(&[o, "p", &e.person, "f", &t13(t - t.rem_euclid(M))]))
                    .or_default() += 1;
            }
            Some(x) => {
                let to: BTreeSet<String> = x.subjects.iter().map(|s| people.person(s)).collect();
                let row = json!({"k": x.kind, "what": x.what, "by": a, "to": to, "src": e.src, "ip": e.ip,
                                 "asn": e.asn, "cc": e.cc, "mfa": x.mfa, "okta": e.okta_says})
                .to_string();
                for p in to.iter().chain([&e.person]) {
                    rows.insert(key(&[o, "p", p, "e", &t13(t), &e.h]), row.clone());
                }
            }
            None => {}
        }
    }
    for (k, t) in mins {
        if kv.get(&k)?.as_ref().and_then(int).is_none_or(|v| t < v) {
            kv.set(&k, Scalar::Int(t))?;
        }
    }
    for (k, n) in sums {
        kv.incr(&k, n)?;
    }
    for (k, add) in hours {
        let mut blob = blob(kv.get(&k)?);
        for (slot, cat) in add {
            *blob
                .entry(slot.to_string())
                .or_default()
                .entry(cat.to_string())
                .or_default() += 1;
        }
        kv.set(&k, Scalar::Str(json!(blob).to_string()))?;
    }
    for (k, add) in sessions {
        let mut list = assumes(kv.get(&k)?);
        list.extend(add);
        list.sort_by(|x, y| (x.t, &x.by, &x.arn).cmp(&(y.t, &y.by, &y.arn)));
        // A caller's earliest and latest in each five minutes: an hour of a
        // session re-assumed all day is a short row.
        let mut ends: BTreeMap<(String, String, i64), Vec<Assume>> = BTreeMap::new();
        for x in list {
            let v = ends
                .entry((x.by.clone(), x.arn.clone(), x.t.div_euclid(SLOT)))
                .or_default();
            match v.get_mut(1) {
                Some(latest) => *latest = x,
                None => v.push(x),
            }
        }
        let mut list: Vec<Assume> = ends.into_values().flatten().collect();
        list.sort_by(|x, y| (x.t, &x.by, &x.arn).cmp(&(y.t, &y.by, &y.arn)));
        kv.set(
            &k,
            Scalar::Str(serde_json::to_string(&list).map_err(|e| e.to_string())?),
        )?;
    }
    for (k, v) in rows {
        kv.set(&k, Scalar::Str(v))?;
    }
    Ok(())
}

/// The dimensions an actor's first-seen times are kept for.
fn values(e: &Ev) -> Vec<(&'static str, String)> {
    let mut out = vec![("api", e.verb.clone()), ("src", e.src.clone())];
    out.extend(e.res.iter().map(|r| ("res", r.clone())));
    out.extend(e.client.iter().map(|c| ("client", c.clone())));
    out.extend(e.oauth.iter().map(|(i, _)| ("oauth", i.clone())));
    out.extend(e.device.iter().map(|d| ("device", d.clone())));
    out.extend(e.token.iter().map(|t| ("token", t.clone())));
    out
}

fn nets(e: &Ev) -> Vec<(&'static str, String)> {
    let net =
        e.ip.as_deref()
            .and_then(|ip| match ip.parse::<IpAddr>().ok()? {
                IpAddr::V4(a) => {
                    let [x, y, z, _] = a.octets();
                    Some(format!("{x}.{y}.{z}.0/24"))
                }
                IpAddr::V6(a) => {
                    let [p, q, r, ..] = a.segments();
                    Some(format!("{}/48", Ipv6Addr::new(p, q, r, 0, 0, 0, 0, 0)))
                }
            });
    [("asn", e.asn.clone()), ("cc", e.cc.clone()), ("net", net)]
        .into_iter()
        .filter_map(|(d, v)| Some((d, v?)))
        .collect()
}

/// State as of the window's start: everything it hands out is from before `ws`.
struct Before<'a, K> {
    kv: &'a mut K,
    org: String,
    ws: i64,
}

impl<K: Kv> Before<'_, K> {
    fn key(&self, parts: &[&str]) -> String {
        let mut all = vec![self.org.as_str()];
        all.extend(parts);
        key(&all)
    }

    fn first(&mut self, parts: &[&str]) -> R<Option<i64>> {
        let k = self.key(parts);
        Ok(self
            .kv
            .get(&k)?
            .as_ref()
            .and_then(int)
            .filter(|t| *t < self.ws))
    }

    /// The first-seen times under a prefix, by what follows it, earliest first.
    fn firsts(&mut self, parts: &[&str]) -> R<Vec<(String, i64)>> {
        let pre = self.key(parts) + "|";
        let hi = format!("{}}}", self.key(parts)); // `}` sorts right after `|`
        let mut out: Vec<(String, i64)> = (self.kv.scan(&pre, &hi)?)
            .into_iter()
            .filter_map(|(k, v)| Some((unesc(k.strip_prefix(&pre)?), int(&v)?)))
            .filter(|(_, t)| *t < self.ws)
            .collect();
        out.sort_by(|x, y| (x.1, &x.0).cmp(&(y.1, &y.0)));
        Ok(out)
    }

    /// Rows keyed by time in [lo, ws).
    fn events(&mut self, parts: &[&str], lo: i64) -> R<Vec<(i64, Value)>> {
        let pre = self.key(parts) + "|";
        if lo >= self.ws {
            return Ok(Vec::new());
        }
        let rows = self.kv.scan(
            &format!("{pre}{}", t13(lo)),
            &format!("{pre}{}", t13(self.ws)),
        )?;
        Ok(rows
            .into_iter()
            .filter_map(|(k, v)| {
                let t = k.strip_prefix(&pre)?.get(..13)?.parse::<i64>().ok()?;
                let v = serde_json::from_str(&string(v)?).ok()?;
                (t < self.ws).then_some((t, v))
            })
            .collect())
    }

    /// Events per day, for the days before the window's.
    fn days(&mut self, a: &str) -> R<BTreeMap<i64, i64>> {
        let pre = self.key(&["a", a, "d"]) + "|";
        let rows = self
            .kv
            .scan(&pre, &format!("{pre}{:05}", self.ws.div_euclid(D)))?;
        Ok(rows
            .into_iter()
            .filter_map(|(k, v)| Some((k.strip_prefix(&pre)?.parse().ok()?, int(&v)?)))
            .collect())
    }

    /// Five-minute slots wholly in [lo, ws): the one holding ws is left out whole.
    fn slots(&mut self, a: &str, lo: i64) -> R<BTreeMap<i64, BTreeMap<String, i64>>> {
        let pre = self.key(&["a", a, "h"]) + "|";
        let (from, to) = (lo.div_euclid(H), self.ws.div_euclid(H) + 1);
        let rows = self
            .kv
            .scan(&format!("{pre}{from:07}"), &format!("{pre}{to:07}"))?;
        let mut out = BTreeMap::new();
        for (k, v) in rows {
            let Some(h) = k.strip_prefix(&pre).and_then(|x| x.parse::<i64>().ok()) else {
                continue;
            };
            for (sl, cats) in blob(Some(v)) {
                let Some(t) = sl
                    .parse::<i64>()
                    .ok()
                    .filter(|x| (0..H / SLOT).contains(x))
                    .map(|x| h * H + x * SLOT)
                else {
                    continue;
                };
                if lo <= t && t + SLOT <= self.ws {
                    out.insert(t, cats);
                }
            }
        }
        Ok(out)
    }

    /// Failed sign-ins in the minutes wholly in [lo, ws).
    fn failed(&mut self, person: &str, lo: i64) -> R<i64> {
        let pre = self.key(&["p", person, "f"]) + "|";
        let rows = self.kv.scan(
            &format!("{pre}{}", t13(lo)),
            &format!("{pre}{}", t13(self.ws - M + 1)),
        )?;
        Ok(rows.iter().filter_map(|(_, v)| int(v)).sum())
    }

    /// The latest assumption of a role session in [before - 12h, before).
    fn assumed(&mut self, acct: &str, role: &str, sess: &str, before: i64) -> R<Option<Assume>> {
        let pre = self.key(&["s", acct, role, sess]) + "|";
        let (lo, cut) = (before - CHAIN, before.min(self.ws));
        let rows = self.kv.scan(
            &format!("{pre}{:07}", lo.div_euclid(H)),
            &format!("{pre}{:07}", cut.div_euclid(H) + 1),
        )?;
        Ok(rows
            .into_iter()
            .flat_map(|(_, v)| assumes(Some(v)))
            .filter(|x| x.t < cut && x.t >= lo)
            .max_by(|x, y| (x.t, &x.by).cmp(&(y.t, &y.by))))
    }
}

/// `_hist` for one actor's records in a call, or None when all are `_context`.
fn header<K: Kv>(kv: &mut K, people: &People, group: &[&Ev]) -> R<Option<Map<String, Value>>> {
    let fresh: Vec<&Ev> = group.iter().copied().filter(|e| !e.context).collect();
    let (Some(e0), Some(ws), Some(we)) = (
        fresh.first(),
        fresh.iter().map(|e| e.ms).min(),
        fresh.iter().map(|e| e.ms).max(),
    ) else {
        return Ok(None);
    };
    let a = e0.actor.as_str();
    let own = e0.person.clone();
    // who is behind a role or a federated user: their sign-ins count too
    let mut behind: Option<String> = None;
    let ctx: Vec<&Ev> = group
        .iter()
        .copied()
        .filter(|e| e.context && e.ms < ws)
        .collect();
    let mut b = Before {
        kv,
        org: e0.org.clone(),
        ws,
    };
    let mut h = Map::new();
    let mut links = Vec::new();

    // who
    let mut parts = vec![match e0.principal.as_deref() {
        Some("member") => "human (profile member)",
        Some("service") => "service account (profile)",
        Some("key") => "API key (profile)",
        Some("aws") => "AWS service",
        _ => "not in profile",
    }
    .to_string()];
    if let Some(names) = people.names.get(&own).filter(|n| n.len() > 1) {
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
            let (text, root) = chain(&mut b, acct, role, sess, ws, 0)?;
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
    let behind = behind.filter(|r| !r.starts_with("aws:") && people.person(r) != own);
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
    h.insert("actor".into(), json!(parts.join("; ")));

    // baseline
    let first = b.first(&["a", a, "first"])?;
    let days = b.days(a)?;
    let sl = b.slots(a, ws - BASE)?;
    let today: Vec<&BTreeMap<String, i64>> =
        sl.range(ws.div_euclid(D) * D..).map(|(_, c)| c).collect();
    let n = days.values().sum::<i64>() + today.iter().flat_map(|c| c.values()).sum::<i64>();
    let active = days.len() + usize::from(!today.is_empty());
    let mut srcs: Vec<String> = b
        .firsts(&["a", a, "n", "src"])?
        .into_iter()
        .map(|(s, _)| s)
        .collect();
    srcs.sort();
    let young = first.is_some_and(|f| ws - f < D);
    h.insert(
        "baseline".into(),
        json!(match first {
            None => "none: first seen in this window".to_string(),
            Some(f) => format!(
                "seen {:.1} days (since {}), {} on {}, from {}{}",
                (ws - f) as f64 / D as f64,
                date(f),
                nn(n, "event"),
                nn(active as i64, "day"),
                srcs.join(", "),
                if young {
                    "; under a day, too short for usual hours or volume"
                } else {
                    ""
                }
            ),
        }),
    );

    // what is new
    if first.is_some() {
        let mut win: Vec<(&str, Vec<String>)> = Vec::new();
        for e in &fresh {
            for (dim, v) in values(e) {
                match win.iter_mut().find(|(d, _)| *d == dim) {
                    Some((_, vs)) if !vs.contains(&v) => vs.push(v),
                    Some(_) => {}
                    None => win.push((dim, vec![v])),
                }
            }
        }
        let since = |f: Option<i64>| {
            f.map(|f| format!(" (first seen {} before)", ago(ws - f)))
                .unwrap_or_default()
        };
        let mut new = Map::new();
        for (dim, vals) in &win {
            let mut fresh_vals = Vec::new();
            for v in vals {
                let f = b.first(&["a", a, "n", dim, v])?;
                if f.is_some_and(|f| ws - f > RECENT) {
                    continue;
                }
                let name = (fresh.iter().flat_map(|e| &e.oauth))
                    .find(|(i, _)| *dim == "oauth" && i == v)
                    .map(|(i, n)| quoted(n.as_deref().unwrap_or(i)));
                fresh_vals.push(name.unwrap_or_else(|| v.clone()) + &since(f));
            }
            if fresh_vals.is_empty() {
                continue;
            }
            let note = match *dim {
                "api" | "res" => format!(" ({} of {} in window)", fresh_vals.len(), vals.len()),
                "client" => {
                    let mut before = b.firsts(&["a", a, "n", "client"])?;
                    before.retain(|(_, f)| ws - f > RECENT); // the new ones are listed
                    if before.is_empty() {
                        String::new()
                    } else {
                        format!("; before: {}", cap(before.into_iter().map(|(c, _)| c), 3))
                    }
                }
                _ => String::new(),
            };
            let label = match *dim {
                "res" => "resource",
                "src" => "source",
                other => other,
            };
            new.insert(label.into(), json!(cap(fresh_vals, 4) + &note));
        }
        let mut netnew = Vec::new();
        for (dim, label) in [("asn", "ASN"), ("cc", "country"), ("net", "net")] {
            let mut seen = BTreeSet::new();
            for e in &fresh {
                for (d, v) in nets(e) {
                    if d != dim || !seen.insert(v.clone()) {
                        continue;
                    }
                    let f = b.first(&["p", &own, "n", dim, &v])?;
                    if f.is_some_and(|f| ws - f > RECENT) {
                        continue;
                    }
                    let org_new = b.first(&["o", "n", dim, &v])?.is_none();
                    let name = e
                        .asorg
                        .as_ref()
                        .filter(|_| dim == "asn")
                        .map(|o| format!(" {o}"))
                        .unwrap_or_default();
                    netnew.push(format!(
                        "{label} {v}{name}{}{}",
                        if org_new { " (new to org)" } else { "" },
                        since(f)
                    ));
                }
            }
        }
        if !netnew.is_empty() {
            new.insert("network".into(), json!(netnew.join(", ")));
        }
        let nothing = "nothing: every api, resource, client and network here was first seen over an hour before";
        h.insert(
            "new".into(),
            if new.is_empty() {
                json!(nothing)
            } else {
                Value::Object(new)
            },
        );
        let mut seen = BTreeSet::new();
        for (id, name) in fresh.iter().flat_map(|e| &e.oauth) {
            if seen.insert(id) {
                if let Some(f) = b.first(&["a", a, "n", "oauth", id])? {
                    let app = name.as_deref().map_or_else(|| quoted(id), quoted);
                    links.push(format!(
                        "oauth app {app} first seen for this actor {} before",
                        ago(ws - f)
                    ));
                }
            }
        }
    }

    // volume and time, against this actor's own last 28 days
    if first.is_some() && !young {
        let mut hourly: BTreeMap<i64, BTreeMap<String, i64>> = BTreeMap::new();
        for (t, c) in sl.range(..ws.div_euclid(H) * H) {
            let hr = hourly.entry(t.div_euclid(H)).or_default();
            for (k, n) in c {
                *hr.entry(k.clone()).or_default() += n;
            }
        }
        // The slot holding ws is not in `sl`; the request's `_context` covers it.
        let open = ws - ws.rem_euclid(SLOT);
        let last = (sl.keys().next_back().map(|t| t + SLOT))
            .max(ctx.iter().map(|e| e.ms).max())
            .map(|t| ago(ws - t));
        let before_that = days.keys().next_back().map(|d| ago(ws - (d + 1) * D));
        match (&last, hourly.is_empty()) {
            (None, _) => {
                h.insert("volume".into(), json!("no activity in 28d to compare with"));
                let day = before_that
                    .map(|x| format!(", last active day {x} before"))
                    .unwrap_or_default();
                h.insert(
                    "time".into(),
                    json!(format!("dormant: no activity in 28d{day}")),
                );
            }
            (Some(last), true) => {
                h.insert(
                    "volume".into(),
                    json!("no earlier active hour in 28d to compare with"),
                );
                let day = before_that
                    .map(|x| format!("; before that, last active day {x} before"))
                    .unwrap_or_default();
                h.insert(
                    "time".into(),
                    json!(format!(
                        "active only this hour in 28d, last active {last} before{day}"
                    )),
                );
            }
            (Some(last), false) => {
                let period = (we - ws).max(H);
                let mut cur: BTreeMap<String, i64> = BTreeMap::new();
                let lately = ctx.iter().filter(|e| e.ms >= open.max(we - period));
                for e in fresh.iter().chain(lately) {
                    *cur.entry(e.cat.to_string()).or_default() += 1;
                }
                for (_, c) in sl.range(we - period..) {
                    for (k, n) in c {
                        *cur.entry(k.clone()).or_default() += n;
                    }
                }
                let rate = |n: i64| n as f64 * H as f64 / period as f64;
                let tot = p90(hourly.values().map(|c| c.values().sum()).collect());
                let mut out = vec![format!("total {}", bucket(rate(cur.values().sum()), tot))];
                let cols = [
                    ("r", "read"),
                    ("w", "write"),
                    ("i", "iam"),
                    ("c", "own creds"),
                    ("d", "delete"),
                ];
                for (k, name) in cols {
                    let base = p90(hourly
                        .values()
                        .filter_map(|c| c.get(k).copied())
                        .filter(|n| *n > 0)
                        .collect());
                    out.push(format!(
                        "{name} {}",
                        bucket(rate(cur.get(k).copied().unwrap_or(0)), base)
                    ));
                }
                h.insert(
                    "volume".into(),
                    json!(format!(
                        "last hour vs p90 of its {} in 28d: {}",
                        nn(hourly.len() as i64, "active hour"),
                        out.join(", ")
                    )),
                );
                let mut dayhours: BTreeMap<i64, BTreeSet<i64>> = BTreeMap::new();
                for hr in hourly.keys() {
                    dayhours
                        .entry(hr.rem_euclid(24))
                        .or_default()
                        .insert(hr.div_euclid(24));
                }
                let ndays = hourly
                    .keys()
                    .map(|hr| hr.div_euclid(24))
                    .collect::<BTreeSet<_>>()
                    .len();
                let usual: Vec<i64> = (dayhours.iter())
                    .filter(|(_, ds)| ds.len() as f64 >= (0.1 * ndays as f64).max(2.0))
                    .map(|(x, _)| *x)
                    .collect();
                let wh: BTreeSet<i64> = fresh
                    .iter()
                    .map(|e| e.ms.div_euclid(H).rem_euclid(24))
                    .collect();
                let at = wh.iter().map(|x| {
                    format!(
                        "{x:02} UTC active on {}",
                        dayhours.get(x).map_or(0, BTreeSet::len)
                    )
                });
                let usual = if usual.is_empty() {
                    "hours: none yet".to_string()
                } else {
                    format!("{} UTC", ranges(&usual))
                };
                h.insert(
                    "time".into(),
                    json!(format!(
                        "window {} of its {} in 28d; usual {usual}; last active {last} before",
                        join(at),
                        nn(ndays as i64, "active day")
                    )),
                );
            }
        }
    }

    // the sign-ins and credential changes of the actor's person, and of who
    // is behind it, in every source
    let mut whose = vec![own.clone()];
    whose.extend(behind.as_deref().map(|r| people.person(r)));
    let (mut ents, mut failed) = (Vec::new(), 0);
    for p in &whose {
        ents.extend(b.events(&["p", p, "e"], ws - AUTH)?);
        failed += b.failed(p, ws - AUTH)?;
    }
    ents.sort_by_cached_key(|(t, x)| (*t, x.to_string()));
    ents.dedup(); // a row filed under both
    let kind = |x: &Value| x["k"].as_str().unwrap_or("").to_string();
    let by_of = |x: &Value| people.person(x["by"].as_str().unwrap_or(""));
    let logins: Vec<&(i64, Value)> = ents.iter().filter(|(_, x)| kind(x) == "login").collect();
    let rest: Vec<&(i64, Value)> = ents
        .iter()
        .filter(|(_, x)| !matches!(kind(x).as_str(), "login" | "login failed" | "mfa ok"))
        .collect();
    let named = |x: &str| {
        let p = people.person(x);
        if people.names.contains_key(&p) {
            format!("person {p}")
        } else {
            x.to_string()
        }
    };
    let mut s = format!("last 72h, {}", named(a));
    if let Some(r) = &behind {
        s += &format!(" and {}", named(r));
    }
    s += ": ";
    if logins.is_empty() && failed == 0 && rest.is_empty() {
        s += "no logins or credential changes";
    } else {
        let mut bysrc: Vec<(String, i64)> = Vec::new();
        for (_, x) in &logins {
            let src = x["what"]
                .as_str()
                .unwrap_or("")
                .split(' ')
                .next()
                .unwrap_or("")
                .to_string();
            match bysrc.iter_mut().find(|(k, _)| *k == src) {
                Some((_, n)) => *n += 1,
                None => bysrc.push((src, 1)),
            }
        }
        bysrc.sort_by_key(|x| std::cmp::Reverse(x.1));
        s += &nn(logins.len() as i64, "login");
        if !bysrc.is_empty() {
            s += &format!(" ({})", join(bysrc.iter().map(|(k, n)| format!("{k} {n}"))));
        }
        s += &format!(", {failed} failed");
        let mut alike: Vec<([String; 5], i64, i64)> = Vec::new();
        for (t, x) in &rest {
            let [kd, what, by, ip] =
                ["k", "what", "by", "ip"].map(|f| x[f].as_str().unwrap_or("").to_string());
            // whom it was done to, when not the actor
            let on = (x["to"].as_array().into_iter().flatten())
                .filter_map(Value::as_str)
                .filter(|p| *p != own);
            let k = [kd, what, by, ip, join(on)];
            match alike.iter_mut().find(|(k2, ..)| *k2 == k) {
                Some((_, c, last)) => {
                    *c += 1;
                    *last = *t;
                }
                None => alike.push((k, 1, *t)),
            }
        }
        if alike.is_empty() {
            s += "; no mfa, password, key or grant changes";
        }
        for ([k, what, by, ip, on], c, t) in alike.iter().skip(alike.len().saturating_sub(4)) {
            let on = if on.is_empty() {
                String::new()
            } else {
                format!(" on {on}")
            };
            let by = if people.person(by) == own {
                String::new()
            } else {
                format!(" by {by}")
            };
            let times = if *c > 1 {
                format!(" x{c}")
            } else {
                String::new()
            };
            let from = if ip.is_empty() {
                String::new()
            } else {
                format!(" from {ip}")
            };
            s += &format!("; {k} {what}{on}{by}{times} {} before{from}", ago(ws - t));
        }
        if let Some((t, x)) = logins.last() {
            let newnet = new_net(&mut b, &by_of(x), x, *t)?;
            let mut mfa = match x["mfa"] {
                Value::Bool(true) => "yes",
                Value::Bool(false) => "no",
                _ => "unknown",
            };
            // Okta's MFA around an Okta sign-in is that sign-in's; a federated AWS one says
            // nothing of MFA, so Okta's stays a separate fact
            let near = |(t2, y): &(i64, Value)| kind(y) == "mfa ok" && (t2 - t).abs() <= MFA_NEAR;
            if mfa == "unknown" && ents.iter().any(near) {
                match x["src"].as_str() {
                    Some("okta") => mfa = "yes",
                    Some("cloudtrail") => mfa = "unknown (okta mfa within 5m)",
                    _ => {}
                }
            }
            s += &format!(
                "; last login {} {} before",
                x["what"].as_str().unwrap_or(""),
                ago(ws - t)
            );
            for (f, label) in [("ip", " from "), ("asn", " ASN "), ("cc", " ")] {
                if let Some(v) = x[f].as_str() {
                    s += &format!("{label}{v}");
                }
            }
            if !newnet.is_empty() {
                s += &format!(" (new {} for person then)", newnet.join("/"));
            }
            s += &format!(", mfa {mfa}");
            let okta: Vec<&str> = x["okta"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(Value::as_str)
                .collect();
            if !okta.is_empty() {
                s += &format!(" (okta: {})", okta.join(", "));
            }
        }
    }
    h.insert("auth".into(), json!(s));

    // the same person or address across sources and actors
    let mut wips: Vec<&str> = Vec::new();
    for ip in fresh.iter().filter_map(|e| e.ip.as_deref()) {
        if !wips.contains(&ip) {
            wips.push(ip);
        }
    }
    if let Some((t, x)) = logins
        .iter()
        .find(|(_, x)| x["ip"].as_str().is_some_and(|ip| wips.contains(&ip)))
    {
        let (ip, what, by) = (
            x["ip"].as_str().unwrap_or(""),
            x["what"].as_str().unwrap_or(""),
            x["by"].as_str().unwrap_or(""),
        );
        links.push(format!(
            "window ip {ip} matches {what} login by {by} {} before",
            ago(ws - t)
        ));
    }
    for (t, x) in &logins {
        let p = by_of(x);
        if ws - t <= H && !new_net(&mut b, &p, x, *t)?.is_empty() {
            let l = format!(
                "{} login from a network new for {p} {} before this window",
                x["what"].as_str().unwrap_or(""),
                ago(ws - t)
            );
            if !links.contains(&l) {
                links.push(l);
            }
        }
    }
    let mut shown = 0;
    for ip in wips {
        if shown == 2 || b.first(&["o", "ip", ip, a])?.is_some() {
            continue;
        }
        shown += 1;
        let others: Vec<String> = b
            .firsts(&["o", "ip", ip])?
            .into_iter()
            .map(|(x, _)| x)
            .filter(|x| x != a)
            .collect();
        links.push(if others.is_empty() {
            format!("ip {ip} new for this actor, no other actor used it before")
        } else {
            format!(
                "ip {ip} new for this actor, used before by {} ({})",
                nn(others.len() as i64, "other actor"),
                cap(others, 3)
            )
        });
    }
    if !links.is_empty() {
        h.insert("links".into(), json!(links.join("; ")));
    }
    Ok(Some(safe(h)))
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
    if let Some((a2, r2, s2)) = kinds::assumed(&x.arn).filter(|_| depth < 3) {
        let (more, root) = chain(b, &a2, &r2, &s2, x.t, depth + 1)?;
        out += &more;
        return Ok((out, root.or(Some(x.by))));
    }
    Ok((out, Some(x.by)))
}

/// Which of the login's ASN and country the person first used within this
/// sign-in, having used another before it.
fn new_net<K: Kv>(b: &mut Before<K>, person: &str, x: &Value, t: i64) -> R<Vec<&'static str>> {
    let mut out = Vec::new();
    for d in ["asn", "cc"] {
        let Some(v) = x[d].as_str() else {
            continue;
        };
        let seen = b.firsts(&["p", person, "n", d])?;
        if seen
            .iter()
            .any(|(w, f)| w == v && t - SIGN_IN <= *f && *f <= t)
            && seen.iter().any(|(_, f)| *f < t - SIGN_IN)
        {
            out.push(d);
        }
    }
    Ok(out)
}

fn bucket(n: f64, base: f64) -> &'static str {
    if n <= 0.0 {
        return "-";
    }
    if base <= 0.0 {
        return "none before";
    }
    match n / base.max(FLOOR) {
        x if x < 2.0 => "normal",
        x if x < 5.0 => "2-5x",
        x if x < 10.0 => "5-10x",
        _ => ">10x",
    }
}

fn p90(mut xs: Vec<i64>) -> f64 {
    xs.sort_unstable();
    let i = ((xs.len() as f64 * 0.9) as usize).min(xs.len().saturating_sub(1));
    xs.get(i).copied().unwrap_or(0) as f64
}

fn ago(ms: i64) -> String {
    let s = ms.max(0) / 1000;
    match s {
        0..90 => format!("{s}s"),
        90..5400 => format!("{}m", s / 60),
        5400..172_800 => format!("{}h", s / 3600),
        _ => format!("{}d", s / 86_400),
    }
}

fn ranges(hours: &[i64]) -> String {
    let mut out: Vec<(i64, i64)> = Vec::new();
    for &x in hours {
        match out.last_mut() {
            Some((_, b)) if x == *b + 1 => *b = x,
            _ => out.push((x, x)),
        }
    }
    join(out.iter().map(|(a, b)| {
        if a == b {
            format!("{a:02}")
        } else {
            format!("{a:02}-{b:02}")
        }
    }))
}

fn nn(n: i64, word: &str) -> String {
    format!("{n} {word}{}", if n == 1 { "" } else { "s" })
}

fn cap(xs: impl IntoIterator<Item = String>, n: usize) -> String {
    let xs: Vec<String> = xs.into_iter().collect();
    let more = if xs.len() > n {
        format!(" +{} more", xs.len() - n)
    } else {
        String::new()
    };
    join(xs.into_iter().take(n)) + &more
}

fn join<T: std::fmt::Display>(xs: impl IntoIterator<Item = T>) -> String {
    xs.into_iter()
        .map(|x| x.to_string())
        .collect::<Vec<_>>()
        .join(", ")
}

/// Printable ASCII with no `"`, `\` or `=`, so logfmt writes each line bare.
fn safe(h: Map<String, Value>) -> Map<String, Value> {
    h.into_iter()
        .map(|(k, v)| {
            let v = match v {
                Value::String(s) => Value::String(
                    s.chars()
                        .map(|c| {
                            if matches!(c, ' '..='~') && !matches!(c, '"' | '\\' | '=') {
                                c
                            } else {
                                '?'
                            }
                        })
                        .collect(),
                ),
                Value::Object(m) => Value::Object(safe(m)),
                other => other,
            };
            (k, v)
        })
        .collect()
}

fn key(parts: &[&str]) -> String {
    parts
        .iter()
        .map(|p| p.replace('%', "%25").replace('|', "%7C"))
        .collect::<Vec<_>>()
        .join("|")
}

fn unesc(x: &str) -> String {
    x.replace("%7C", "|").replace("%25", "%")
}

fn t13(ms: i64) -> String {
    format!("{:013}", ms.max(0))
}

fn int(v: &Scalar) -> Option<i64> {
    match v {
        Scalar::Int(i) => Some(*i),
        Scalar::Str(s) => s.parse().ok(),
        _ => None,
    }
}

fn string(v: Scalar) -> Option<String> {
    match v {
        Scalar::Str(s) => Some(s),
        _ => None,
    }
}

fn blob(v: Option<Scalar>) -> BTreeMap<String, BTreeMap<String, i64>> {
    v.and_then(string)
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}

fn assumes(v: Option<Scalar>) -> Vec<Assume> {
    v.and_then(string)
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
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
    use super::*;

    #[derive(Default)]
    struct Mem(BTreeMap<String, Scalar>);

    impl Kv for Mem {
        fn get(&mut self, key: &str) -> R<Option<Scalar>> {
            Ok(self.0.get(key).cloned())
        }
        fn set(&mut self, key: &str, v: Scalar) -> R<()> {
            self.0.insert(key.into(), v);
            Ok(())
        }
        fn incr(&mut self, key: &str, by: i64) -> R<()> {
            let n = self.0.get(key).and_then(int).unwrap_or(0);
            self.0.insert(key.into(), Scalar::Int(n + by));
            Ok(())
        }
        fn scan(&mut self, lo: &str, hi: &str) -> R<Vec<(String, Scalar)>> {
            let r = self
                .0
                .range::<str, _>((std::ops::Bound::Included(lo), std::ops::Bound::Excluded(hi)));
            Ok(r.map(|(k, v)| (k.clone(), v.clone())).collect())
        }
    }

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
        assert!(run(&mut st, &profile(), Mode::Learn, &lines).is_ok_and(|out| out.is_empty()));
        st
    }

    fn hist(st: &mut Mem, rec: &Value) -> Value {
        // the judge's: no profile, what learn filed
        let none = Profile::from_value(Value::Null);
        let out = run(st, &none, Mode::Header, &[rec.to_string()]).unwrap_or_default();
        let rec: Value = serde_json::from_slice(&out).unwrap_or_default();
        rec.get("_hist").cloned().unwrap_or_default()
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
        let h = hist(
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
        let h = hist(
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
        let h = hist(&mut fold(std::slice::from_ref(&old)), &win);
        has(&h, "baseline", "seen 3.9 days");
        assert_eq!(h, hist(&mut fold(&[old, win.clone(), same]), &win));
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
        // judged already, in the window's own five minutes, so in no slot `_hist` reads
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
        let out = run(&mut st, &profile(), Mode::Header, &lines).unwrap_or_default();
        let last: Value = (out.split(|b| *b == b'\n'))
            .nth(1)
            .and_then(|l| serde_json::from_slice(l).ok())
            .unwrap_or_default();
        let h = last.get("_hist").cloned().unwrap_or_default();
        has(&h, "time", "last active 5s before");
        has(&h, "volume", "own creds none before");
    }

    #[test]
    fn volume_buckets_have_a_floor() {
        assert_eq!(bucket(0.0, 4.0), "-");
        assert_eq!(bucket(4.0, 0.0), "none before");
        assert_eq!(bucket(5.0, 1.0), "normal"); // 5 against the floor of 3, not 5x
        assert_eq!(bucket(7.0, 1.0), "2-5x");
        assert_eq!(bucket(40.0, 2.0), ">10x");
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
            &hist(&mut fold(&own), &win),
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
        let h = hist(&mut fold(&[own.as_slice(), &[other]].concat()), &win);
        has(&h, "actor", "privilege used before: admin");
        has(&h, "auth", "key created access key on deploy-bot");
    }

    /// A person's one other name may be written bare.
    #[test]
    fn one_name_needs_no_list() {
        let p = People::from_value(&json!({"Dave@acme.io": "Dave", "eve@acme.io": [1, "eve"]}));
        assert_eq!(p.person("dave"), "dave@acme.io");
        assert_eq!(p.person("eve"), "eve@acme.io");
        assert_eq!(People::from_value(&json!(["dave"])).person("dave"), "dave");
    }

    /// The header reads who is who from what learn filed, not a profile of
    /// its own; a changed profile is filed again.
    #[test]
    fn the_header_reads_learn_s_people() {
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
            &hist(&mut st, &win),
            "actor",
            "person dave@acme.io (dave, dave@acme.io)",
        );
        let alone = Profile::from_value(json!({}));
        assert!(run(&mut st, &alone, Mode::Learn, &[]).is_ok());
        assert!(matches!(st.0.get(PEOPLE), Some(Scalar::Str(t)) if t == "{}"));
        let h = hist(&mut st, &win);
        let actor = h.get("actor").and_then(Value::as_str).unwrap_or_default();
        assert!(!actor.is_empty() && !actor.contains("person"), "{h}");
    }

    /// The app pack's audit shape under a name of yours (its `sources` or
    /// `extra_sources`) is learned as `app` is; a record of a source history
    /// does not know, without an `event`, is not.
    #[test]
    fn the_app_shape_under_another_name() {
        let rec = |src: &str, ts: &str| {
            json!({"source": src, "ts": format!("2026-{ts}Z"), "event": "invoice.view", "ip": HOME.0,
                   "path": "/invoices/42", "_norm": {"actor": "carol@acme.io", "org": "acme"}})
        };
        let mut st = fold(&[rec("billing", "01-01T15:00:00")]);
        let h = hist(&mut st, &rec("billing", "01-05T13:00:00"));
        has(&h, "baseline", "seen 3.9 days");
        let mut other = rec("mystery", "01-05T13:00:00");
        if let Some(m) = other.as_object_mut() {
            m.remove("event");
        }
        assert_eq!(hist(&mut st, &other), Value::Null);
    }
}
