//! Which names are one person: the profile's `people`, each person and the
//! other names the sources give them (an IAM user, a GitHub handle). Every
//! name is lowercased. `factskit` sets `_norm.person` from it.

use std::collections::{BTreeMap, BTreeSet};

use serde_json::Value;

/// Who each name belongs to.
#[derive(Debug, Default)]
pub struct People {
    of: BTreeMap<String, String>,
    names: BTreeMap<String, BTreeSet<String>>,
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
                eprintln!("profile `people` is {v}, not a map of person to names; ignored");
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
                    eprintln!("profile `people.{person}` is {v}, not a list of names; ignored");
                    Vec::new()
                }
            };
            let mut all = BTreeSet::new();
            for n in names {
                match n.as_str() {
                    Some(n) => {
                        all.insert(n.to_lowercase());
                    }
                    None => eprintln!("profile `people.{person}` holds {n}, not a name; ignored"),
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

    /// The person a name belongs to; a name the map does not hold is its own.
    pub fn person(&self, name: &str) -> String {
        let a = name.to_lowercase();
        self.of.get(&a).cloned().unwrap_or(a)
    }

    /// The person and their other names, when the map names them.
    pub fn names(&self, person: &str) -> Option<&BTreeSet<String>> {
        self.names.get(&person.to_lowercase())
    }

    /// The map names this person.
    pub fn knows(&self, person: &str) -> bool {
        self.names.contains_key(&person.to_lowercase())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A person's one other name may be written bare.
    #[test]
    fn one_name_needs_no_list() {
        let p = People::from_value(&json!({"Dave@acme.io": "Dave", "eve@acme.io": [1, "eve"]}));
        assert_eq!(p.person("dave"), "dave@acme.io");
        assert_eq!(p.person("eve"), "eve@acme.io");
        assert_eq!(People::from_value(&json!(["dave"])).person("dave"), "dave");
    }

    #[test]
    fn a_person_and_their_names() {
        let p = People::from_value(
            &json!({"dave@acme.io": ["dave", "dave-gh"], "grace@acme.io": null}),
        );
        assert_eq!(p.person("DAVE-GH"), "dave@acme.io");
        assert_eq!(p.person("dave@acme.io"), "dave@acme.io");
        assert_eq!(p.person("Mallory"), "mallory", "nobody's: its own");
        let all = BTreeSet::from(["dave", "dave-gh", "dave@acme.io"].map(String::from));
        assert_eq!(p.names("Dave@acme.io"), Some(&all));
        assert!(p.knows("dave@acme.io") && p.knows("grace@acme.io"));
        assert!(!p.knows("dave"), "a name, not a person");
        assert!(!p.knows("mallory") && p.names("mallory").is_none());
        assert!(!People::from_value(&Value::Null).knows("dave@acme.io"));
    }
}
