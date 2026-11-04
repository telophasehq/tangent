"""The person turns on Claude Code's telemetry for acme's collector, as acme's
onboarding asks, by adding the exports to ~/.zshrc in vim; a new tab picks
them up and every claude they start from it carries OTEL_*."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import _negatives as neg  # noqa: E402
import es  # noqa: E402

CASE = {
    "id": "person-sets-otel-collector-in-zshrc",
    "title": "The person adds acme's OpenTelemetry collector to ~/.zshrc in vim; claude from a new tab starts with OTEL_* set",
    "flag": False,
    "eaa": ["EAA-012"],
    "group": "negatives",
    "grounding": {"catalog_case": None, "steps": [], "sources": ["SRC-020", neg.HUNTING]},
    "notes": (
        "Plainly benign: Claude Code's monitoring documentation (SRC-020) configures telemetry with "
        "CLAUDE_CODE_ENABLE_TELEMETRY and the OTEL_* exporter variables, set in the environment; the rightful user "
        "sets them for their company's collector (otel.corp.example.com, made up) in their own rc file, with vim at "
        "their terminal, and opens a new tab. Unlike the cloned-repository and helper-script redirections, the writer "
        "is the person's editor and the endpoint is their employer's. The exports are file content, which eslogger "
        "does not show. Key events: vim's save of ~/.zshrc (the rename to ~/.zshrc~, the create, write and close) "
        "and claude's exec with exec.env.OTEL_* from the new tab."
    ),
}

OTEL = {"CLAUDE_CODE_ENABLE_TELEMETRY": "1", "OTEL_METRICS_EXPORTER": "otlp", "OTEL_LOGS_EXPORTER": "otlp",
        "OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf", "OTEL_EXPORTER_OTLP_ENDPOINT": "https://otel.corp.example.com:4318"}


def build(mac):
    sh = mac.terminal()
    mac.wait(5)
    es.vim_edit(mac, sh, "~/.zshrc", key=True)
    mac.wait(6)
    mac.logout(sh)
    mac.wait(3)
    sh = mac.terminal(cwd=neg.WEB)  # reads ~/.zshrc, and exports what it sets
    sh.env.update(OTEL)
    mac.wait(4)
    mac.run(sh, "env", ["env"], wait=0.01)
    mac.wait(5)
    c = es.claude(mac, sh, neg.WEB, key=True)
    mac.wait(12)
    es.claude_prompt(mac, c)
    neg.session_end(mac, c, sh)
