//! Role sessions: who assumed each, an hour's row at a time, as the renderer
//! follows a chain of roles back to who is behind it.

use std::collections::BTreeMap;

use baselinekit::keys::{assumes, key, Assume};
use baselinekit::kinds::Ev;
use baselinekit::time::{H, SLOT};
use baselinekit::{Kv, Scalar, R};

pub fn write(kv: &mut impl Kv, evs: &[&Ev]) -> R<()> {
    let mut sessions: BTreeMap<String, Vec<Assume>> = BTreeMap::new();
    for e in evs {
        let (o, a, t) = (e.org.as_str(), e.actor.as_str(), e.ms);
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
        }
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
    Ok(())
}
