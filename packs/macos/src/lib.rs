//! `eslogger`'s events, from a Mac, into the few records a model should read:
//! every program run, with its environment cut to the names that steer an AI
//! agent; every write, and the reads that matter, on the files an agent is
//! steered, extended and read through, on credentials and on what runs at
//! login (`paths.rs`); nothing else. Each record carries the program that
//! acted (for an exec, the one that started), the chain of programs above it
//! (from a process tree kept in state from `fork`, `exec` and `exit`), and
//! `_norm`/`_facts` as every pack's do; its actor is a program, so there is
//! no principal for a profile to know.
//! An event that is only noise here is dropped: there is no store downstream,
//! so what passes is what a model sees.

wit_bindgen::generate!({
    path: ".tangent/wit",
    world: "processor",
    generate_all,
});

mod paths;

use exports::tangent::logs::mapper::{Guest, Meta, Pred, Selector};
use normkit::{Norm, Stamp};
use paths::{group, home, scratch, Group};
use serde_json::{json, Map, Value};
use tangent::logs::enrichment::lookup;
use tangent::logs::log::{Logview, Scalar};
use tangent::logs::state;

struct Component;

export!(Component);

/// The environment names a record keeps. They steer an agent: where it
/// sends its API traffic, where it keeps its config and where its telemetry
/// goes. Every other variable is dropped, with its value: a program's
/// environment is where its keys are.
const ENV_KEEP: &[&str] = &[
    "ANTHROPIC_BASE_URL",
    "OPENAI_BASE_URL",
    "GOOGLE_GEMINI_BASE_URL",
    "CLAUDE_CONFIG_DIR",
    "CODEX_HOME",
    "GEMINI_CLI_HOME",
];

/// The `OTEL_*` names that say where telemetry goes and what it carries
/// (`OTEL_EXPORTER_OTLP_ENDPOINT`, `..._METRICS_ENDPOINT`, `..._HEADERS`).
/// The rest say nothing: Claude Code sets
/// `OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE` on every child.
fn otel_matters(name: &str) -> bool {
    name.starts_with("OTEL_EXPORTER_OTLP_")
        && (name.ends_with("_ENDPOINT") || name.ends_with("_HEADERS"))
}

/// Programs above the actor named in a record, nearest first.
const PARENTS: usize = 6;

/// `open`'s `fflag`: the kernel's FREAD and FWRITE bits.
const FWRITE: i64 = 2;

/// Arguments past this many bytes are cut to their count (normkit::shrink).
const ARGS_BYTES: usize = 1500;

fn str_at<'a>(v: &'a Value, ptr: &str) -> Option<&'a str> {
    v.pointer(ptr).and_then(Value::as_str)
}

fn int_at(v: &Value, ptr: &str) -> Option<i64> {
    v.pointer(ptr).and_then(Value::as_i64)
}

fn put(m: &mut Map<String, Value>, key: &str, v: Option<impl Into<Value>>) {
    if let Some(v) = v {
        m.insert(key.to_string(), v.into());
    }
}

/// A process block (`process`, `event.exec.target`, `event.fork.child`), as
/// much of it as a verdict reads.
struct Proc {
    pid: i64,
    ppid: i64,
    path: String,
    /// its signing identity
    signer: Option<String>,
    platform: bool,
    /// its Developer ID team: Apple issues it to the vendor, so an ad hoc
    /// signature cannot claim one
    team: Option<String>,
    /// for an interpreter or a shell running a file, the script it runs
    name: Option<String>,
    uid: Option<i64>,
    tty: Option<String>,
    /// the process macOS holds responsible (the app a terminal is), when another
    responsible: Option<i64>,
}

fn proc_of(v: &Value) -> Option<Proc> {
    let pid = int_at(v, "/audit_token/pid")?;
    let signer = str_at(v, "/signing_id")
        .filter(|s| !s.is_empty())
        .map(str::to_string);
    Some(Proc {
        pid,
        ppid: int_at(v, "/ppid").unwrap_or(0),
        path: str_at(v, "/executable/path").unwrap_or("").to_string(),
        signer,
        platform: v["is_platform_binary"].as_bool().unwrap_or(false),
        team: str_at(v, "/team_id")
            .filter(|t| !t.is_empty())
            .map(str::to_string),
        name: None,
        uid: int_at(v, "/audit_token/euid"),
        tty: str_at(v, "/tty/path").map(str::to_string),
        responsible: int_at(v, "/responsible_audit_token/pid").filter(|r| *r != pid),
    })
}

impl Proc {
    /// Who the program is, for the queue's windows and the baseline: for an
    /// interpreter, the script it runs, so npm, a package's postinstall and
    /// an MCP server are not one `node`; else a vendor's signing identity,
    /// which an update keeps (Claude Code's path names its version); else
    /// its path. Apple's command-line tools share one signing identity
    /// (/usr/bin/git and python3 are both the Xcode shim), and an ad hoc
    /// signature is the binary's hash, so those are their path.
    fn ident(&self) -> String {
        if let Some(n) = &self.name {
            return n.clone();
        }
        match &self.signer {
            Some(s) if self.team.is_some() && !self.platform => s.clone(),
            _ => self.path.clone(),
        }
    }

    fn json(&self) -> Value {
        let mut m = Map::new();
        m.insert("pid".into(), json!(self.pid));
        m.insert("ppid".into(), json!(self.ppid));
        m.insert("path".into(), json!(self.path));
        put(&mut m, "name", self.name.clone());
        put(&mut m, "signer", self.signer.clone());
        put(&mut m, "team", self.team.clone());
        m.insert("platform".into(), json!(self.platform));
        put(&mut m, "uid", self.uid);
        put(&mut m, "tty", self.tty.clone());
        put(&mut m, "responsible", self.responsible);
        Value::Object(m)
    }
}

// The process tree, in state: `p:<pid>` holds "<ppid>\t<program>\t<name>".
// fork adds the child with its parent's program and name, exec replaces
// them, exit removes the entry; a process seen before any of those is added
// as the event shows it. The tree answers which programs are above this one,
// and the name of a process whose exec it saw.

struct Known {
    ppid: i64,
    path: String,
    name: Option<String>,
}

fn known(pid: i64) -> Option<Known> {
    match state::get(&format!("p:{pid}")) {
        Ok(Some(Scalar::Str(s))) => {
            let mut f = s.splitn(3, '\t');
            let ppid = f.next()?.parse().ok()?;
            let path = f.next()?.to_string();
            let name = f.next().filter(|n| !n.is_empty()).map(str::to_string);
            Some(Known { ppid, path, name })
        }
        _ => None,
    }
}

/// A process the tree never saw exit is forgotten after a day. Core's state
/// keeps keys until told otherwise, and pids come back.
const PROC_TTL_MS: u64 = 86_400_000;

fn remember(p: &Proc) {
    let _ = state::set(
        &format!("p:{}", p.pid),
        &Scalar::Str(format!(
            "{}\t{}\t{}",
            p.ppid,
            p.path,
            p.name.as_deref().unwrap_or("")
        )),
        Some(PROC_TTL_MS),
    );
}

fn forget(pid: i64) {
    let _ = state::del(&format!("p:{pid}"));
}

/// The programs above `ppid`, nearest first, as far as the tree knows them:
/// each its name when it has one, else its path.
fn parents(mut ppid: i64) -> Vec<String> {
    let mut out = Vec::new();
    while ppid > 1 && out.len() < PARENTS {
        let Some(k) = known(ppid) else { break };
        out.push(k.name.unwrap_or(k.path));
        ppid = k.ppid;
    }
    out
}

/// Interpreters and shells: the program they run is the file they are given.
fn interpreter(path: &str) -> Option<&'static str> {
    let base = path.rsplit('/').next().unwrap_or(path);
    Some(match base {
        "node" => "node",
        "bun" => "bun",
        "deno" => "deno",
        "ruby" => "ruby",
        "perl" => "perl",
        "osascript" => "osascript",
        "sh" | "bash" | "zsh" | "dash" => base_static(base),
        "Python" => "python",
        b if b.starts_with("python") => "python",
        _ => return None,
    })
}

fn base_static(b: &str) -> &'static str {
    match b {
        "sh" => "sh",
        "bash" => "bash",
        "zsh" => "zsh",
        _ => "dash",
    }
}

/// Long flags that take the next argument as their value, before the script.
const VALUE_FLAGS: &[&str] = &["--require", "--import", "--loader", "--inspect-port"];

/// An interpreter's short flags, which come in clusters (`zsh -lc`,
/// `node -pe`, `python -Bc`): the letters that run inline code, and the
/// letters that take a value (the rest of the cluster, or the next argument).
fn short_flags(interp: &str) -> (&'static str, &'static str) {
    match interp {
        "sh" | "bash" | "zsh" | "dash" => ("c", "o"),
        "python" => ("c", "WX"),
        "node" | "bun" => ("ep", "r"),
        "ruby" => ("e", "rI"),
        "perl" => ("eE", "IM"),
        "osascript" => ("e", "l"),
        _ => ("", ""),
    }
}

/// What an exec runs, when the program is an interpreter: `node <script>`,
/// `python -m <module>`, the script joined to the working directory and
/// written from `~`. None for inline code (`-c`, `-lc`, `-e`) and a bare
/// shell.
fn script_name(path: &str, args: &Value, cwd: Option<&str>) -> Option<String> {
    let interp = interpreter(path)?;
    let (inline, valued) = short_flags(interp);
    let args: Vec<&str> = args.as_array()?.iter().filter_map(Value::as_str).collect();
    let mut i = 1;
    while let Some(a) = args.get(i) {
        match *a {
            "--eval" | "--print" => return None,
            "-m" => return args.get(i + 1).map(|m| format!("{interp} -m {m}")),
            f if VALUE_FLAGS.contains(&f) => i += 1,
            f if f.starts_with("--") => {}
            f if f.starts_with('-') => {
                let letters: Vec<char> = f.chars().skip(1).collect();
                for (j, c) in letters.iter().enumerate() {
                    if inline.contains(*c) {
                        return None;
                    }
                    if valued.contains(*c) {
                        if j + 1 == letters.len() {
                            i += 1; // the value is the next argument
                        }
                        break;
                    }
                }
            }
            script => {
                let full = if script.starts_with('/') {
                    script.to_string()
                } else {
                    let rel = script.trim_start_matches("./");
                    match cwd {
                        Some(d) => format!("{}/{rel}", d.trim_end_matches('/')),
                        None => rel.to_string(),
                    }
                };
                let shown = home(&full).map(|(_, t)| t).unwrap_or(full);
                return Some(format!("{interp} {shown}"));
            }
        }
        i += 1;
    }
    None
}

/// The path a `create` or `rename` wrote to: an existing file, or a new name
/// in a directory.
fn destination(body: &Value) -> Option<String> {
    if let Some(p) = str_at(body, "/destination/existing_file/path") {
        return Some(p.to_string());
    }
    let dir = str_at(body, "/destination/new_path/dir/path")?;
    let name = str_at(body, "/destination/new_path/filename")?;
    Some(format!("{}/{name}", dir.trim_end_matches('/')))
}

/// `K=V` entries to the kept names, and how many there were.
fn environment(env: Option<&Value>) -> (Map<String, Value>, usize) {
    let mut kept = Map::new();
    let Some(entries) = env.and_then(Value::as_array) else {
        return (kept, 0);
    };
    for e in entries.iter().filter_map(Value::as_str) {
        let Some((k, v)) = e.split_once('=') else {
            continue;
        };
        if ENV_KEEP.contains(&k) || otel_matters(k) {
            kept.insert(k.to_string(), json!(v));
        }
    }
    (kept, entries.len())
}

/// The record as a model reads it, and its `_norm`; None for an event that
/// is nobody's business.
fn finished(rec: &Map<String, Value>) -> Option<(Map<String, Value>, Norm)> {
    let event = rec.get("event")?.as_object()?;
    let (kind, body) = event.iter().next()?;
    let actor = proc_of(rec.get("process")?)?;

    // the tree first, whatever becomes of the event
    if known(actor.pid).is_none() {
        remember(&actor);
    }
    match kind.as_str() {
        "fork" => {
            if let Some(mut child) = proc_of(&body["child"]) {
                child.name = known(actor.pid).and_then(|k| k.name);
                remember(&child);
            }
            return None;
        }
        "exit" => {
            forget(actor.pid);
            return None;
        }
        _ => {}
    }

    // An exec's process is the program that started, under the pid it took
    // over; the one that launched it is the first of its parents.
    let actor = match kind.as_str() {
        "exec" => {
            let mut target = proc_of(&body["target"])?;
            target.name = str_at(body, "/script/path")
                .zip(interpreter(&target.path))
                .map(|(s, i)| format!("{i} {}", home(s).map(|(_, t)| t).unwrap_or(s.to_string())))
                .or_else(|| {
                    body.get("args")
                        .and_then(|a| script_name(&target.path, a, str_at(body, "/cwd/path")))
                });
            remember(&target);
            target
        }
        _ => {
            let mut a = actor;
            a.name = known(a.pid)
                .filter(|k| k.path == a.path)
                .and_then(|k| k.name);
            a
        }
    };

    let mut out = Map::new();
    out.insert("event".into(), json!(kind));
    out.insert("process".into(), actor.json());
    let above = parents(actor.ppid);
    if !above.is_empty() {
        out.insert("parents".into(), json!(above));
    }

    let mut n = Norm {
        actor: Some(actor.ident()),
        kind: Some(kind.clone()),
        principal: Some("none"), // a program, not a person the profile could know
        ..Norm::default()
    };
    let mut org = home(&actor.path).map(|(name, _)| name.to_string());

    // what the event touched, and whether that is worth a model's time
    let mut touched: Option<(String, Option<Group>)> = None;
    match kind.as_str() {
        "exec" => {
            let mut x = Map::new();
            if let Some(args) = body.get("args") {
                x.insert("args".into(), normkit::shrink(args, ARGS_BYTES));
            }
            let cwd = str_at(body, "/cwd/path");
            org = org.or_else(|| cwd.and_then(home).map(|(name, _)| name.to_string()));
            put(&mut x, "cwd", cwd);
            put(&mut x, "script", str_at(body, "/script/path"));
            let (env, count) = environment(body.get("env"));
            x.insert("env_count".into(), json!(count));
            if !env.is_empty() {
                n.fact(
                    "agent_env",
                    "the environment sets where an AI agent sends its API traffic, keeps its config or sends telemetry",
                );
                x.insert("env".into(), Value::Object(env));
            }
            if scratch(&actor.path) {
                n.fact(
                    "scratch_program",
                    "the program runs from a temporary, cache, download or node_modules directory",
                );
            }
            if actor.signer.is_none() && actor.team.is_none() && !actor.platform {
                n.fact("unsigned", "the program has no code signature");
            }
            out.insert("exec".into(), Value::Object(x));
        }
        "open" => {
            let file = str_at(body, "/file/path")?.to_string();
            let write = int_at(body, "/fflag").unwrap_or(0) & FWRITE != 0;
            let g = group(&file)?;
            if !write && !g.read_matters() {
                return None;
            }
            out.insert("access".into(), json!(if write { "write" } else { "read" }));
            touched = Some((file, Some(g)));
        }
        "close" => {
            if !body["modified"].as_bool().unwrap_or(false) {
                return None;
            }
            let file = str_at(body, "/target/path")?.to_string();
            let g = group(&file)?;
            out.insert("modified".into(), json!(true));
            touched = Some((file, Some(g)));
        }
        "create" => {
            let file = destination(body)?;
            let g = group(&file)?;
            touched = Some((file, Some(g)));
        }
        "unlink" => {
            let file = str_at(body, "/target/path")?.to_string();
            let g = group(&file)?;
            touched = Some((file, Some(g)));
        }
        "rename" => {
            let from = str_at(body, "/source/path")?.to_string();
            let to = destination(body)?;
            let g = group(&to).or_else(|| group(&from))?;
            if body.pointer("/destination/existing_file").is_some() {
                n.fact(
                    "rename_over",
                    "the file was replaced by renaming another over it",
                );
            }
            out.insert("from".into(), json!(from));
            touched = Some((to, Some(g)));
        }
        // rare and each its own story (btm_launch_item_add, tcc_modify, ...): as it came, less the stat blocks
        _ => {
            let mut detail = body.clone();
            if let Value::Object(m) = &mut detail {
                normkit::prune(m, &|k, _| k == "stat");
            }
            out.insert("detail".into(), normkit::shrink(&detail, ARGS_BYTES));
        }
    }

    if let Some((file, g)) = touched {
        if let Some(g) = g {
            n.fact(g.key(), g.note());
        }
        org = org.or_else(|| home(&file).map(|(name, _)| name.to_string()));
        n.target = Some(file.clone());
        out.insert("file".into(), json!(file));
    }
    n.org = org;
    Some((out, n))
}

impl Guest for Component {
    fn metadata() -> Meta {
        Meta {
            name: "macos".to_string(),
            version: "0.1.0".to_string(),
        }
    }

    /// An eslogger line: an `event` from a `process`.
    fn probe() -> Vec<Selector> {
        vec![Selector {
            any: Vec::new(),
            all: vec![
                Pred::Has("event".to_string()),
                Pred::Has("process.audit_token".to_string()),
            ],
            none: Vec::new(),
        }]
    }

    fn process_logs(input: Vec<Logview>) -> Result<Vec<u8>, String> {
        let kit = factskit::kit(lookup);
        let mut out = Vec::new();
        for lv in input {
            let read = normkit::record(&lv.log())?;
            if let Some((rec, n)) = finished(&read) {
                // eslogger's `time`, and no `source`: the stamp is built, not read
                let mut stamp = Stamp::of(&read);
                if let Some(t) = read.get("time").and_then(Value::as_str) {
                    stamp.ts = t.to_string();
                }
                stamp.source = "macos".into();
                kit.write(&mut out, stamp, rec, n)?;
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn name(path: &str, args: &[&str], cwd: &str) -> Option<String> {
        script_name(path, &json!(args), Some(cwd))
    }

    #[test]
    fn interpreters_are_named_by_their_script() {
        let node = "/opt/homebrew/Cellar/node/26.0.0/bin/node";
        assert_eq!(
            name(
                node,
                &["node", "scripts/postinstall.js"],
                "/Users/a/code/x/node_modules/p"
            ),
            Some("node ~/code/x/node_modules/p/scripts/postinstall.js".into())
        );
        assert_eq!(
            name(
                node,
                &["node", "/opt/homebrew/bin/npm", "install"],
                "/Users/a/code/x"
            ),
            Some("node /opt/homebrew/bin/npm".into())
        );
        assert_eq!(
            name(
                node,
                &["node", "--require", "./hook.js", "./server.js"],
                "/Users/a/s"
            ),
            Some("node ~/s/server.js".into())
        );
        assert_eq!(
            name(
                "/usr/bin/python3",
                &["python3", "-m", "pytest", "-q"],
                "/Users/a/x"
            ),
            Some("python -m pytest".into())
        );
        assert_eq!(name("/bin/sh", &["sh", "-c", "node x.js"], "/"), None);
        assert_eq!(name("/bin/zsh", &["-zsh"], "/Users/a"), None);
        assert_eq!(name("/usr/bin/git", &["git", "status"], "/Users/a"), None);
        // inline code in a cluster of flags
        assert_eq!(
            name(
                "/bin/zsh",
                &["/bin/zsh", "-lc", "ls src tests"],
                "/Users/a/x"
            ),
            None
        );
        assert_eq!(
            name("/usr/bin/python3", &["python3", "-Bc", "print(1)"], "/"),
            None
        );
        assert_eq!(name(node, &["node", "-pe", "1"], "/"), None);
        // a value-taking letter ends the cluster
        assert_eq!(
            name(
                "/usr/bin/python3",
                &["python3", "-Werror::DeprecationWarning", "t.py"],
                "/Users/a"
            ),
            Some("python ~/t.py".into())
        );
        assert_eq!(
            name(
                "/bin/bash",
                &["bash", "-o", "pipefail", "-e", "run.sh"],
                "/Users/a"
            ),
            Some("bash ~/run.sh".into())
        );
    }
}
