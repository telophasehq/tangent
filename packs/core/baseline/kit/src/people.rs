//! Who is who: the profile's `people`, each person's other names. The builder
//! reads them from the `profile` table and files them in state; the renderer
//! reads what it filed, so ingest and the judge agree, and the judge needs no
//! profile of its own.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::{json, Value};

use crate::{Kv, Scalar, R};

/// Where the builder files the profile's `people` for the renderer. No org's
/// keys can be it: theirs all have a `|`.
pub const PEOPLE: &str = "people";

#[derive(Default)]
pub struct People {
    of: BTreeMap<String, String>,
    pub names: BTreeMap<String, BTreeSet<String>>,
}

impl People {
    /// The profile's `people`: each person's other names, a list or one name.
    /// What it cannot read is said, once per instance, on stderr.
    pub fn from_value(people: &Value) -> People {
        let mut p = People::default();
        let entries = match people {
            Value::Object(m) => m,
            Value::Null => return p,
            v => {
                eprintln!(
                    "baseline: profile `people` is {v}, not a map of person to names; ignored"
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
                        "baseline: profile `people.{person}` is {v}, not a list of names; ignored"
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
                        "baseline: profile `people.{person}` holds {n}, not a name; ignored"
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

    pub fn person(&self, actor: &str) -> String {
        let a = actor.to_lowercase();
        self.of.get(&a).cloned().unwrap_or(a)
    }

    /// The people the builder filed, `{}` before it has run.
    pub fn filed(kv: &mut impl Kv) -> R<People> {
        Ok(match kv.get(PEOPLE)? {
            Some(Scalar::Str(json)) => serde_json::from_str::<Value>(&json)
                .map(|v| People::from_value(&v))
                .unwrap_or_default(),
            _ => People::default(),
        })
    }
}

/// The profile's `people`, as its JSON text and read.
pub struct Profile {
    pub text: String,
    pub people: People,
}

impl Profile {
    pub fn from_value(v: Value) -> Profile {
        let v = if v.is_null() { json!({}) } else { v };
        Profile {
            text: v.to_string(),
            people: People::from_value(&v),
        }
    }

    /// Filed when it differs from what is: the same text from every writer.
    pub fn file(&self, kv: &mut impl Kv) -> R<()> {
        match kv.get(PEOPLE)? {
            Some(Scalar::Str(t)) if t == self.text => Ok(()),
            _ => kv.set(PEOPLE, Scalar::Str(self.text.clone())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A person's one other name may be written bare.
    #[test]
    fn one_name_needs_no_list() {
        let p = People::from_value(&json!({"Dave@acme.io": "Dave", "eve@acme.io": [1, "eve"]}));
        assert_eq!(p.person("dave"), "dave@acme.io");
        assert_eq!(p.person("eve"), "eve@acme.io");
        assert_eq!(People::from_value(&json!(["dave"])).person("dave"), "dave");
    }
}
