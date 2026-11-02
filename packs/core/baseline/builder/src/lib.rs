//! `baseline_builder`, in ingest: files the profile's `people` in state, and
//! folds every record it can read into the keys the renderer reads in the
//! judge (../kit has them); emits nothing. Every fold is a min, a sum or a row
//! keyed by event time, so records arriving in any order leave the same state.

mod counts;
mod firsts;
mod rows;
mod sessions;
// wasm only: the renderer's tests link this crate, and a binary holds one
// plugin's exports.
#[cfg(target_arch = "wasm32")]
mod wit;

use baselinekit::kinds::{ev, Ev};
use baselinekit::people::Profile;
use baselinekit::{Kv, R};
use serde_json::Value;

/// Files the profile's `people`, so the renderer reads with them too, then
/// folds `lines` in, each actor under its person.
pub fn build(kv: &mut impl Kv, profile: &Profile, lines: &[String]) -> R<()> {
    profile.file(kv)?;
    let people = &profile.people;
    let evs: Vec<Ev> = lines
        .iter()
        .filter_map(|l| {
            let r: Value = serde_json::from_str(l).ok()?;
            let mut e = ev(&r)?;
            e.person = people.person(&e.actor);
            if e.auth.is_some() {
                e.h = hash(&r.to_string());
            }
            Some(e)
        })
        .collect();
    let evs: Vec<&Ev> = evs.iter().collect();
    firsts::write(kv, &evs)?;
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
    use baselinekit::people::{Profile, PEOPLE};
    use baselinekit::{Mem, Scalar};
    use serde_json::json;

    use super::build;

    /// Filed before any record, and filed again when the profile changes.
    #[test]
    fn the_profile_s_people_are_filed() {
        let mut st = Mem::default();
        let dave = Profile::from_value(json!({"dave@acme.io": ["dave"]}));
        assert!(build(&mut st, &dave, &[]).is_ok());
        let filed = |st: &Mem| match st.0.get(PEOPLE) {
            Some(Scalar::Str(t)) => t.clone(),
            _ => String::new(),
        };
        assert_eq!(filed(&st), r#"{"dave@acme.io":["dave"]}"#);
        assert!(build(&mut st, &Profile::from_value(json!({})), &[]).is_ok());
        assert_eq!(filed(&st), "{}");
    }
}
