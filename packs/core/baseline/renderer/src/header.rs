//! One actor's `_baseline`, a key at a time: actor.rs, seen.rs, new.rs,
//! volume.rs, time.rs, auth.rs and links.rs each write theirs.

use baselinekit::kinds::Ev;
use baselinekit::{Kv, R};
use serde_json::{json, Map, Value};

use crate::before::Before;
use crate::text::safe;
use crate::{actor, auth, links, new, seen, time, volume};

/// One actor's records in a call, as each key reads them.
pub struct Win<'a> {
    /// the records judged now: not `_context`
    pub fresh: Vec<&'a Ev>,
    /// `_context` records before the window, judged already
    pub ctx: Vec<&'a Ev>,
    pub e0: &'a Ev,
    pub actor: &'a str,
    /// the actor's person, as its first record has it
    pub own: String,
    /// who is who names `own`
    pub named: bool,
    /// the window's start and end: its first and last record's time
    pub ws: i64,
    pub we: i64,
}

/// `_baseline` for one actor's records in a call, or None when all are `_context`.
pub fn header<K: Kv>(kv: &mut K, group: &[&Ev]) -> R<Option<Map<String, Value>>> {
    let fresh: Vec<&Ev> = group.iter().copied().filter(|e| !e.context).collect();
    let (Some(e0), Some(ws), Some(we)) = (
        fresh.first().copied(),
        fresh.iter().map(|e| e.ms).min(),
        fresh.iter().map(|e| e.ms).max(),
    ) else {
        return Ok(None);
    };
    let ctx: Vec<&Ev> = group
        .iter()
        .copied()
        .filter(|e| e.context && e.ms < ws)
        .collect();
    let w = Win {
        fresh,
        ctx,
        e0,
        actor: &e0.actor,
        own: e0.person.clone(),
        named: e0.named,
        ws,
        we,
    };
    let mut b = Before {
        kv,
        org: e0.org.clone(),
        ws,
    };
    let mut h = Map::new();
    let mut links = Vec::new();

    let (who, behind) = actor::actor(&mut b, &w)?;
    h.insert("actor".into(), json!(who));
    let seen = seen::seen(&mut b, &w)?;
    h.insert("seen".into(), json!(seen.line));
    if seen.first.is_some() {
        h.insert("new".into(), new::new(&mut b, &w, &mut links)?);
    }
    // volume and time, against this actor's own last 28 days
    if seen.first.is_some() && !seen.young {
        let hourly = seen.hourly(ws);
        let last = time::last(&w, &seen.slots);
        let v = volume::volume(&w, &seen.slots, &hourly, last.is_some());
        h.insert("volume".into(), json!(v));
        let t = time::time(&w, &seen.days, &hourly, last.as_deref());
        h.insert("time".into(), json!(t));
    }
    let (line, logins) = auth::auth(&mut b, &w, behind.as_ref())?;
    h.insert("auth".into(), json!(line));
    links::links(&mut b, &w, &logins, &mut links)?;
    if !links.is_empty() {
        h.insert("links".into(), json!(links.join("; ")));
    }
    Ok(Some(safe(h)))
}
