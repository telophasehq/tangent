//! Which files an event touched that a verdict could turn on: the files an
//! AI coding agent is steered, extended and read through (the groups of the
//! Endpoint AI Agent Abuse catalog), the files that hold credentials, and
//! the files a login or a shell runs. Everything else a Mac writes all day
//! is nobody's business here.

/// A group, and the note a model reads it by.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Group {
    /// an agent's settings and hooks, which run a command at its steps
    Settings,
    /// what an agent reads as instructions or remembers between sessions
    Instructions,
    /// which MCP servers an agent connects to
    Mcp,
    /// an agent's transcripts and session state
    State,
    /// plugins, skills and editor extensions: code the agent loads
    Extensions,
    /// cloud, registry, SSH and signing credentials
    Credentials,
    /// what runs at login, in every shell, on every git command, or on every push
    Persistence,
}

impl Group {
    pub fn key(self) -> &'static str {
        match self {
            Group::Settings => "agent_settings",
            Group::Instructions => "agent_instructions",
            Group::Mcp => "agent_mcp",
            Group::State => "agent_state",
            Group::Extensions => "agent_extensions",
            Group::Credentials => "credentials",
            Group::Persistence => "persistence",
        }
    }

    pub fn note(self) -> &'static str {
        match self {
            Group::Settings => "an AI agent's settings file, where its hooks are: a command there runs at the agent's steps",
            Group::Instructions => "an AI agent's instructions or memory: text the model reads as its own guidance",
            Group::Mcp => "an AI agent's MCP configuration: which servers it connects to and runs",
            Group::State => "an AI agent's transcripts or session state: every prompt, reply and command it ran",
            Group::Extensions => "an AI agent's plugins, skills or editor extensions: code it loads",
            Group::Credentials => "a credential file: cloud, registry, SSH, signing or API keys",
            Group::Persistence => "runs at login, in every shell, on every git command, or in CI on every push",
        }
    }

    /// Whether a read of a file in the group is worth a verdict. An agent
    /// reads its settings, MCP config and plugins all day; a read of its
    /// transcripts or of a credential is what theft looks like.
    pub fn read_matters(self) -> bool {
        matches!(self, Group::State | Group::Credentials)
    }
}

/// `/Users/<name>/...` as `~/...`; the name, for the record's org.
pub fn home(path: &str) -> Option<(&str, String)> {
    let rest = path.strip_prefix("/Users/")?;
    let (name, tail) = rest.split_once('/')?;
    if name.is_empty() || name == "Shared" {
        return None;
    }
    Some((name, format!("~/{tail}")))
}

fn tilde(path: &str) -> String {
    home(path)
        .map(|(_, t)| t)
        .unwrap_or_else(|| path.to_string())
}

struct Rule {
    group: Group,
    /// the path starts with this (`~/` for the user's home)
    prefix: &'static str,
    /// and holds this anywhere after the prefix
    within: &'static str,
    /// and ends with this
    suffix: &'static str,
}

const fn rule(
    group: Group,
    prefix: &'static str,
    within: &'static str,
    suffix: &'static str,
) -> Rule {
    Rule {
        group,
        prefix,
        within,
        suffix,
    }
}

/// In the order the catalog lists them; the first match names the group, so
/// `~/.codex/auth.json` is a credential before it is state.
const RULES: &[Rule] = &[
    // credentials
    rule(Group::Credentials, "~/.aws/", "", ""),
    rule(Group::Credentials, "~/.ssh/", "", ""),
    rule(Group::Credentials, "~/.gnupg/", "", ""),
    rule(Group::Credentials, "~/.npmrc", "", ""),
    rule(Group::Credentials, "~/.netrc", "", ""),
    rule(Group::Credentials, "~/.docker/config.json", "", ""),
    rule(Group::Credentials, "~/.kube/config", "", ""),
    rule(Group::Credentials, "~/.config/gh/", "", ""),
    rule(Group::Credentials, "~/.codex/auth.json", "", ""),
    rule(Group::Credentials, "~/Library/Keychains/", "", ""),
    rule(Group::Credentials, "", "", "/.env"),
    rule(Group::Credentials, "", "/.env.", ""),
    // settings and hooks (H)
    rule(Group::Settings, "", "", "/.claude/settings.json"),
    rule(Group::Settings, "", "", "/.claude/settings.local.json"),
    rule(Group::Settings, "", "", "/.gemini/settings.json"),
    rule(Group::Settings, "~/.codex/config.toml", "", ""),
    rule(Group::Settings, "", "", "/.vscode/tasks.json"),
    // instructions and memory (I)
    rule(Group::Instructions, "", "", "/CLAUDE.md"),
    rule(Group::Instructions, "", "", "/AGENTS.md"),
    rule(Group::Instructions, "", "", "/GEMINI.md"),
    rule(Group::Instructions, "~/.claude/projects/", "/memory/", ""),
    rule(Group::Instructions, "~/.codex/memories/", "", ""),
    rule(Group::Instructions, "", "/.cursor/rules/", ""),
    rule(
        Group::Instructions,
        "",
        "",
        "/.github/copilot-instructions.md",
    ),
    // MCP (M); `~/.claude.json` is written as `~/.claude.json.tmp.<pid>.<hex>` then renamed over
    rule(Group::Mcp, "~/.claude.json", "", ""),
    rule(Group::Mcp, "", "", "/.mcp.json"),
    rule(Group::Mcp, "", "", "/.cursor/mcp.json"),
    rule(Group::Mcp, "", "", "/.vscode/mcp.json"),
    rule(
        Group::Mcp,
        "~/Library/Application Support/Code/User/mcp.json",
        "",
        "",
    ),
    // transcripts and state (S)
    rule(Group::State, "~/.claude/projects/", "", ".jsonl"),
    rule(Group::State, "~/.claude/history.jsonl", "", ""),
    rule(Group::State, "~/.codex/sessions/", "", ""),
    rule(Group::State, "~/.codex/history.jsonl", "", ""),
    rule(Group::State, "~/.gemini/tmp/", "", ""),
    // plugins, skills, extensions (X)
    rule(Group::Extensions, "~/.claude/plugins/", "", ""),
    rule(Group::Extensions, "~/.claude/skills/", "", ""),
    rule(Group::Extensions, "~/.codex/skills/", "", ""),
    rule(Group::Extensions, "~/.gemini/extensions/", "", ""),
    rule(Group::Extensions, "~/.cursor/extensions/", "", ""),
    rule(Group::Extensions, "~/.vscode/extensions/", "", ""),
    // what runs on its own
    rule(Group::Persistence, "~/.zshrc", "", ""),
    rule(Group::Persistence, "~/.zshenv", "", ""),
    rule(Group::Persistence, "~/.zprofile", "", ""),
    rule(Group::Persistence, "~/.bashrc", "", ""),
    rule(Group::Persistence, "~/.bash_profile", "", ""),
    rule(Group::Persistence, "~/.profile", "", ""),
    rule(Group::Persistence, "~/Library/LaunchAgents/", "", ""),
    rule(Group::Persistence, "/Library/LaunchAgents/", "", ""),
    rule(Group::Persistence, "/Library/LaunchDaemons/", "", ""),
    rule(Group::Persistence, "", "/.git/hooks/", ""),
    rule(Group::Persistence, "", "", "/.git/config"),
    rule(Group::Persistence, "", "/.github/workflows/", ""),
    // the rest of a Codex home, after its state, memory and skills have their own
    rule(Group::Settings, "", "/.codex/", ""),
];

/// The group a path belongs to, if any.
pub fn group(path: &str) -> Option<Group> {
    let p = tilde(path);
    RULES
        .iter()
        .find(|r| {
            p.starts_with(r.prefix)
                && p[r.prefix.len()..].contains(r.within)
                && p.ends_with(r.suffix)
        })
        .map(|r| r.group)
}

/// Where a dropped or unpacked program would run from.
pub fn scratch(path: &str) -> bool {
    let p = tilde(path);
    p.starts_with("/tmp/")
        || p.starts_with("/private/tmp/")
        || p.starts_with("/var/folders/")
        || p.starts_with("/private/var/folders/")
        || p.starts_with("~/.cache/")
        || p.starts_with("~/Downloads/")
        || p.contains("/node_modules/")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn groups() {
        let g = |p: &str| group(p).map(Group::key);
        assert_eq!(g("/Users/a/.claude/settings.json"), Some("agent_settings"));
        assert_eq!(
            g("/Users/a/code/x/.claude/settings.local.json"),
            Some("agent_settings")
        );
        assert_eq!(g("/Users/a/code/x/CLAUDE.md"), Some("agent_instructions"));
        assert_eq!(
            g("/Users/a/.claude/projects/-Users-a-code-x/memory/MEMORY.md"),
            Some("agent_instructions")
        );
        assert_eq!(
            g("/Users/a/.claude/projects/-Users-a-code-x/abc.jsonl"),
            Some("agent_state")
        );
        assert_eq!(g("/Users/a/.claude.json"), Some("agent_mcp"));
        assert_eq!(g("/Users/a/.claude.json.tmp.123.ab"), Some("agent_mcp"));
        assert_eq!(
            g("/Users/a/.claude/plugins/installed_plugins.json"),
            Some("agent_extensions")
        );
        assert_eq!(g("/Users/a/.codex/auth.json"), Some("credentials"));
        assert_eq!(g("/Users/a/.codex/config.toml"), Some("agent_settings"));
        assert_eq!(
            g("/Users/a/.codex/sessions/2026/x.jsonl"),
            Some("agent_state")
        );
        assert_eq!(g("/Users/a/.codex/history.jsonl"), Some("agent_state"));
        assert_eq!(
            g("/Users/a/.codex/memories/x.md"),
            Some("agent_instructions")
        );
        assert_eq!(
            g("/Users/a/.codex/skills/x/SKILL.md"),
            Some("agent_extensions")
        );
        assert_eq!(
            g("/Users/a/.codex/log/codex-tui.log"),
            Some("agent_settings")
        );
        assert_eq!(g("/Users/a/code/x/.env"), Some("credentials"));
        assert_eq!(g("/Users/a/code/x/.env.local"), Some("credentials"));
        assert_eq!(
            g("/Users/a/code/x/.git/hooks/pre-commit"),
            Some("persistence")
        );
        assert_eq!(g("/Users/a/.zshrc"), Some("persistence"));
        assert_eq!(
            g("/Users/a/code/x/.github/workflows/ci.yml"),
            Some("persistence")
        );
        assert_eq!(g("/Users/a/.claude/shell-snapshots/snapshot.sh"), None);
        assert_eq!(g("/Users/a/.claude/statsig/x"), None);
        assert_eq!(g("/Users/a/code/x/src/main.rs"), None);
        assert_eq!(g("/Users/Shared/x/.env"), Some("credentials"));
        assert_eq!(g("/dev/ttys003"), None);
    }

    #[test]
    fn homes() {
        assert_eq!(home("/Users/a/x"), Some(("a", "~/x".to_string())));
        assert_eq!(home("/Users/Shared/x"), None);
        assert_eq!(home("/usr/bin/git"), None);
        assert!(scratch("/Users/a/code/x/node_modules/.bin/run"));
        assert!(scratch("/private/tmp/x"));
        assert!(!scratch("/usr/bin/curl"));
    }
}
