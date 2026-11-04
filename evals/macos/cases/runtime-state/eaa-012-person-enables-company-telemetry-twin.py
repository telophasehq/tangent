"""The twin of EAA-012: the person starts Claude Code with telemetry sent to
acme's own collector, as the monitoring documentation describes, and the
otelHeadersHelper their user settings name (acme's token script, installed
by the person) runs at start."""
import es

CASE = {
    "id": "eaa-012-person-enables-company-telemetry-twin",
    "title": "The person starts claude with OTEL_* set for acme's collector; acme's otelHeadersHelper runs under it",
    "flag": False,
    "eaa": ["EAA-012"],
    "group": "runtime-state",
    "grounding": {
        "catalog_case": None,
        "steps": [],
        "sources": ["SRC-020"],
    },
    "notes": (
        "Plainly benign: the monitoring documentation describes turning telemetry on with "
        "CLAUDE_CODE_ENABLE_TELEMETRY and the OTEL_* exporter variables, and otelHeadersHelper for dynamic "
        "headers; hunting.md's H7 calls custom collectors and header helpers common enterprise patterns. The "
        "person, at a Terminal tab with a tty, starts claude in their own repository with the variables pointing "
        "at acme's collector (made up, under example.com); the helper their ~/.claude/settings.json names, "
        "~/.local/bin/acme-otel-headers (an sh script, made up), runs under claude (as `/bin/sh -c`, in a session "
        "of its own with no terminal, as Claude Code's other children are seen here; not observed for helpers) "
        "and reads a token from the keychain with `security`. The helper inherits claude's environment, so its exec carries the OTEL_* names "
        "too. Key events: the claude exec and the helper's exec."
    ),
}

HELPER = es.Program("acme-otel-headers", f"{es.HOME}/.local/bin/acme-otel-headers", interp=("sh", []), size=412)
OTEL = {"CLAUDE_CODE_ENABLE_TELEMETRY": "1", "OTEL_METRICS_EXPORTER": "otlp", "OTEL_LOGS_EXPORTER": "otlp",
        "OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf", "OTEL_EXPORTER_OTLP_ENDPOINT": "https://otel.acme.example.com:4318"}


def build(mac):
    mac.exists(HELPER.path, days=60)
    sh = mac.terminal(cwd="~/code/acme-api")
    mac.wait(5)
    agent = es.claude(mac, sh, "~/code/acme-api", env_add=OTEL, key=True)
    mac.wait(1.1)
    h = mac.spawn(agent, "sh", ["/bin/sh", "-c", HELPER.path], fds="pipe", setsid=True)
    mac.exec(h, HELPER, [HELPER.path], key=True)
    mac.read_file(h, HELPER.path)
    mac.run(h, "security", ["security", "find-generic-password", "-s", "acme-otel", "-w"], wait=0.03, fds="pipe")
    mac.exit(h)
    mac.wait(10)
    es.claude_prompt(mac, agent)
    mac.read_file(agent, "~/code/acme-api/src/lib.rs")
    mac.wait(30)
    mac.write_file(agent, agent.session, append=True)
    mac.wait(30)
    mac.exit(agent)
    mac.logout(sh)
