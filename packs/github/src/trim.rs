use normkit::pick;
use serde_json::{Map, Value};

pub fn github_audit(m: &Map<String, Value>) -> Map<String, Value> {
    let mut out = pick(
        m,
        &[
            "ts",
            "source",
            "action",
            "actor",
            "actor_is_bot",
            "actor_ip",
            "actor_location",
            "org",
            "repo",
            "user",
            "team",
            "visibility",
            "public_repo",
            "programmatic_access_type",
            "token_scopes",
            "integration",
            "operation_type",
            "permission",
            "old_permission",
            "name",
            "head_branch",
            "event",
            "conclusion",
            "runner_name",
            "runner_type",
            "pull_request_title",
            "pull_request_url",
            "topic",
            "ref",
            "key",
            "fingerprint",
            "read_only",
            "hook_id",
            "config",
            "target_login",
            "reason",
            "explanation",
            "token_id",
            "hashed_token",
        ],
    );
    // a hosted runner is named for its job's VM: `GitHub Actions 12`
    if let Some(n) = out.get("runner_name").and_then(Value::as_str) {
        let n = n.strip_prefix("GitHub Actions ").unwrap_or("");
        if !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()) {
            out.remove("runner_name");
        }
    }
    let action = m.get("action").and_then(Value::as_str).unwrap_or("");
    if action.starts_with("workflows.") {
        out.remove("topic");
    }
    out
}
