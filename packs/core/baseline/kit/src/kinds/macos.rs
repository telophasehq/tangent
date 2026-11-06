//! packs/macos's records: what a program did to which file, and what started
//! it. The verb is the event, with the group packs/macos marked the file
//! with (`write agent_mcp`, `read credentials`), so a program's first write to
//! an agent's MCP config is a new verb even when it has written files before;
//! the resources are the file, and for an exec the program that started it
//! (`started_by`, its name without the arguments `parents` shows it with).

use serde_json::Value;

use super::{clean, s, str_of, Ev};

/// The groups packs/macos marks in `_facts`, in its order: the first that
/// holds names the record's.
const GROUPS: &[&str] = &[
    "credentials",
    "agent_settings",
    "agent_instructions",
    "agent_mcp",
    "agent_state",
    "agent_extensions",
    "persistence",
];

pub(super) fn read(r: &Value, e: &mut Ev) {
    let ev = s(&r["event"]);
    let group = GROUPS
        .iter()
        .find(|g| r["_facts"].get(**g) == Some(&Value::Bool(true)))
        .copied();
    let read = ev == "open" && r["access"] != "write";
    let doing = match ev {
        "open" | "close" => {
            if read {
                "read"
            } else {
                "write"
            }
        }
        other => other,
    };
    e.verb = match group {
        Some(g) => format!("{doing} {g}"),
        None => doing.to_string(),
    };
    e.cat = match ev {
        "unlink" => 'd',
        _ if read => 'r',
        _ => 'w',
    };
    if let Some(f) = str_of(&r["file"]) {
        e.res.push(format!("file:{}", clean(&home(&f), 100)));
    }
    if ev == "exec" {
        if let Some(p) = str_of(&r["started_by"]) {
            e.res.push(format!("started_by:{}", clean(&home(&p), 100)));
        }
    }
    e.typ = Some("program".into());
}

/// `/Users/<name>/...` as `~/...`: the same file for every user's records.
fn home(path: &str) -> String {
    match path.strip_prefix("/Users/").and_then(|r| r.split_once('/')) {
        Some((name, tail)) if !name.is_empty() && name != "Shared" => format!("~/{tail}"),
        _ => path.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::super::ev;

    fn rec(extra: serde_json::Value) -> serde_json::Value {
        let mut r = json!({
            "ts": "2026-10-07T12:33:44.826Z",
            "source": "macos",
            "_norm": {"actor": "/opt/homebrew/bin/node", "org": "dev"},
        });
        if let (Some(m), Some(x)) = (r.as_object_mut(), extra.as_object()) {
            m.extend(x.clone());
        }
        r
    }

    #[test]
    fn a_write_to_a_group() {
        let e = ev(&rec(json!({
            "event": "open", "access": "write", "file": "/Users/dev/.claude.json",
            "_facts": {"agent_mcp": true},
        })))
        .unwrap_or_default();
        assert_eq!(e.verb, "write agent_mcp");
        assert_eq!(e.cat, 'w');
        assert_eq!(e.res, vec!["file:~/.claude.json".to_string()]);
    }

    #[test]
    fn a_read_and_an_exec() {
        let e = ev(&rec(json!({
            "event": "open", "access": "read", "file": "/Users/dev/.npmrc",
            "_facts": {"credentials": true},
        })))
        .unwrap_or_default();
        assert_eq!((e.verb.as_str(), e.cat), ("read credentials", 'r'));
        let e = ev(&rec(json!({
            "event": "exec", "started_by": "node /opt/homebrew/bin/npm",
            "parents": ["node /opt/homebrew/bin/npm test", "/bin/zsh -l"],
        })))
        .unwrap_or_default();
        assert_eq!(e.verb, "exec");
        assert_eq!(
            e.res,
            vec!["started_by:node /opt/homebrew/bin/npm".to_string()]
        );
    }
}
