//! `seen`: how long the actor has been seen, its events and active days, and
//! from which sources; what `volume` and `time` compare with.

use std::collections::BTreeMap;

use baselinekit::time::{D, H};
use baselinekit::{Kv, R};

use crate::before::Before;
use crate::header::Win;
use crate::text::{date, nn};

const BASE: i64 = 28 * D;

/// Five-minute slots, or hours: the time each starts, its events by category.
pub type Slots = BTreeMap<i64, BTreeMap<String, i64>>;

/// The actor's history before the window.
pub struct Seen {
    pub line: String,
    pub first: Option<i64>,
    /// events per day, for the days before the window's
    pub days: BTreeMap<i64, i64>,
    /// the last 28 days' slots
    pub slots: Slots,
    /// seen under a day: too short for usual hours or volume
    pub young: bool,
}

impl Seen {
    /// The slots by hour, the hours before the window's.
    pub fn hourly(&self, ws: i64) -> Slots {
        let mut hourly: Slots = BTreeMap::new();
        for (t, c) in self.slots.range(..ws.div_euclid(H) * H) {
            let hr = hourly.entry(t.div_euclid(H)).or_default();
            for (k, n) in c {
                *hr.entry(k.clone()).or_default() += n;
            }
        }
        hourly
    }
}

pub fn seen<K: Kv>(b: &mut Before<K>, w: &Win) -> R<Seen> {
    let (a, ws) = (w.actor, w.ws);
    let first = b.first(&["a", a, "first"])?;
    let days = b.days(a)?;
    let slots = b.slots(a, ws - BASE)?;
    let today: Vec<&BTreeMap<String, i64>> = slots
        .range(ws.div_euclid(D) * D..)
        .map(|(_, c)| c)
        .collect();
    let n = days.values().sum::<i64>() + today.iter().flat_map(|c| c.values()).sum::<i64>();
    let active = days.len() + usize::from(!today.is_empty());
    let mut srcs: Vec<String> = b
        .firsts(&["a", a, "n", "src"])?
        .into_iter()
        .map(|(s, _)| s)
        .collect();
    srcs.sort();
    let young = first.is_some_and(|f| ws - f < D);
    let line = match first {
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
    };
    Ok(Seen {
        line,
        first,
        days,
        slots,
        young,
    })
}
