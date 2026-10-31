# cases

What the judge has to catch in a product's own logs, and what it must leave
alone, written down as event sequences before arguing about designs. Every
design question is measured against these.

Each case is a directory:

```
cases/<category>/<name>/
  case.yaml        # scope, cadence, sources, expected verdict, what to check first
  events.ndjson    # the sequence, in the shape of the real source
```

Nothing in here is real: no customer, person, repo or account exists. Two
sources: `app`, a startup's own audit log, and `hiro`, the structlog of an AI
agent that runs plans for its customers. Both carry the customer (`org`,
`organization_id`), which is the partition. The cases built on a company's
own infrastructure logs (CloudTrail, GitHub, Okta, Workspace, CI) are in the
[cloud-infra pack](../../cloud-infra/cases/).

## Sources and their shapes

| `source` | shape |
|---|---|
| `app` | a product's own audit log: `event`, `actor`, `ip`, `org` |
| `hiro` | an AI agent's structlog: `service`, `event`, `organization_id` → `task_id` → `plan_id` → `step_id` |

Every event also carries `ts`, so mixed sources order.

## The 17 cases

12 should flag, 5 must not. `P1` is the first corpus to pass; `P2` is next.

Most positives are sequences of lines innocent on their own
(`alone_innocent: true` in `case.yaml`): a key created and first used
elsewhere, a promotion and an export, a secret read and a POST no plan named,
a tool result that reads like instructions and the step after it acting on
something else. That is why the judge reads an actor's records together, per
customer, not lines.

The negatives are the shapes a product emits all day that look like attacks:
a deploy's restart burst, the top-of-the-hour fan-out, scanner probes for PHP
files, a vendor's rate limit, an admin exporting their own report from the
usual place.

### Identity

| case | | sources | events | what the logs show |
|---|---|---|---|---|
| `identity/api-key-admin-then-new-ip` | flag P1 | `app` | 3 | An admin-scoped API key is created at the office and first used from a VPS |
| `identity/role-change-then-privileged-action` | flag P2 | `app` | 2 | A member is promoted to admin at night and exports the org minutes later |
| `identity/sso-bypass-password-login` | flag P1 | `app` | 2 | Password login succeeds in an org with SSO enforced |

### Data

| case | | sources | events | what the logs show |
|---|---|---|---|---|
| `data/bulk-pagination-export` | flag P1 | `app` | 40 | One API key pages through every finding in four minutes |
| `data/egress-new-destination` | flag P2 | `hiro` | 4 | A service that only talks to vendors posts to a file-sharing site |

### SaaS and integrations

| case | | sources | events | what the logs show |
|---|---|---|---|---|
| `saas/integration-token-stolen-bulk-read` | flag P1 | `app` | 41 | A SaaS integration's OAuth token is stolen in the vendor's breach and used to read everything (Salesloft Drift, Aug 2025) |
| `saas/provider-keys-stolen-from-vendor-console` | flag P2 | `app` | 40 | Keys saved in a third-party developer console are taken in its breach and run up usage from everywhere (OpenCode Console, Oct 2026) |

### AI agents

| case | | sources | events | what the logs show |
|---|---|---|---|---|
| `ai-agent/mcp-server-copies-every-message-out` | flag P1 | `hiro` | 11 | A malicious MCP server release sends a copy of everything it handles to its author (postmark-mcp 1.0.16, Sept 2025) |
| `ai-agent/repo-settings-redirect-model-credentials` | flag P1 | `hiro` | 6 | A cloned repository's assistant settings point the agent's model calls, API key included, at an attacker host (Claude Code CVE-2026-21852) |

### Agent plans

| case | | sources | events | what the logs show |
|---|---|---|---|---|
| `hiro/plan-retry-shifting-params` | flag P2 | `hiro` | 7 | A plan step fails and retries six times with a different role ARN each time |
| `hiro/plan-secret-then-unplanned-post` | flag P1 | `hiro` | 6 | A plan step reads a secret, a later step POSTs to a host not in the plan |
| `hiro/tool-output-injection-then-scope-change` | flag P1 | `hiro` | 5 | A tool result contains instructions; the next step acts on a different resource |

### Must not flag

| case | | sources | events | what the logs show |
|---|---|---|---|---|
| `negative/admin-usual-ip-then-export` | quiet P1 | `app` | 3 | An admin at the usual IP exports their own org's report |
| `negative/deploy-restart-burst` | quiet P1 | `hiro` | 42 | A deploy: new version, forty health checks, a few restarts |
| `negative/scanner-probes` | quiet P1 | `hiro` | 5 | Internet scanners probe the public API for PHP and WordPress files |
| `negative/top-of-hour-sync-fanout` | quiet P1 | `hiro` | 60 | The poller fans out to GitHub at the top of the hour |
| `negative/vendor-429-burst` | quiet P1 | `hiro` | 9 | Okta rate-limits the poller, which backs off and succeeds |

## Cases from disclosed incidents, 2025–2026

4 of the cases follow the paths of real compromises as the logs would show them. Hosts, names and addresses are synthetic; the sequence of events is taken from the public write-ups.

| case | incident |
|---|---|
| `ai-agent/mcp-server-copies-every-message-out` | postmark-mcp 1.0.16, September 2025 |
| `ai-agent/repo-settings-redirect-model-credentials` | Claude Code CVE-2026-21852, 2026 |
| `saas/integration-token-stolen-bulk-read` | Salesloft Drift, August 2025 |
| `saas/provider-keys-stolen-from-vendor-console` | OpenCode Console breach, October 2026 |

Sources: [Claude Code leak and CVE (NodeSource)](https://nodesource.com/blog/anthropic-claude-code-source-leak-bun-bug),
[OpenCode Console (BleepingComputer)](https://bleepingcomputer.com/news/security/opencode-hack-exposes-api-keys-amid-ai-distillation-attacks).
