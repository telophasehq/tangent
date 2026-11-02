//! `baseline_renderer`, in the judge: `_baseline` on each of an actor's
//! records, what state knew about the actor before the first of its records
//! in a call, the facts one window cannot give: who it is and who assumed it,
//! how long it has been seen, what it does for the first time, its volume and
//! hours against its own, and its person's sign-ins and credential changes in
//! every source. It reads what the builder wrote in ingest (../kit has the
//! keys) and writes nothing. Every read is cut at the window's start, so
//! nothing at or after it counts.

mod actor;
mod auth;
mod before;
mod header;
mod links;
mod new;
mod seen;
mod text;
mod time;
mod volume;
mod wit;

#[cfg(test)]
mod tests;

use std::collections::BTreeMap;

use baselinekit::kinds::{ev, Ev};
use baselinekit::people::People;
use baselinekit::{Kv, R};
use serde_json::Value;

/// `lines` with `_baseline` on each record of an actor it can read, and the
/// rest as they came. Who is who is what the builder filed.
pub fn render(kv: &mut impl Kv, lines: &[String]) -> R<Vec<u8>> {
    let people = People::filed(kv)?;
    let recs: Vec<Option<Value>> = lines.iter().map(|l| serde_json::from_str(l).ok()).collect();
    let evs: Vec<Option<Ev>> = recs
        .iter()
        .map(|r| {
            let mut e = ev(r.as_ref()?)?;
            e.person = people.person(&e.actor);
            Some(e)
        })
        .collect();
    let mut groups: BTreeMap<(&str, &str), Vec<&Ev>> = BTreeMap::new();
    for e in evs.iter().flatten() {
        groups.entry((&e.org, &e.actor)).or_default().push(e);
    }
    let mut baselines = BTreeMap::new();
    for (k, g) in &groups {
        if let Some(h) = header::header(kv, &people, g)? {
            baselines.insert(*k, Value::Object(h));
        }
    }
    let mut out = Vec::new();
    for ((line, rec), e) in lines.iter().zip(&recs).zip(&evs) {
        let h = e
            .as_ref()
            .and_then(|e| baselines.get(&(e.org.as_str(), e.actor.as_str())));
        match (rec, h) {
            (Some(Value::Object(m)), Some(h)) => {
                let mut m = m.clone();
                m.insert("_baseline".into(), h.clone());
                out.extend(serde_json::to_vec(&m).map_err(|e| e.to_string())?);
            }
            _ => out.extend(line.trim_end().as_bytes()),
        }
        out.push(b'\n');
    }
    Ok(out)
}
