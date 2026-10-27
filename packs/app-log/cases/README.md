# cases

What the judge has to catch, and what it must leave alone — written down as
event sequences before arguing about designs. Every design question (per
record or per window, which model, which cadence) gets measured against these.

Each case is a directory:

```
cases/<category>/<name>/
  case.yaml        # scope, cadence, sources, expected verdict, what to check first
  events.ndjson    # the sequence, in the shape of the real source
```

Nothing in here is real: no customer, person, repo or account exists. The
cases that use an application's audit log or an AI agent's logs live in the
`app-log` pack.

## Sources and their shapes

| `source` | shape | scope keys |
|---|---|---|
| `okta` | Okta System Log: `eventType`, `actor.alternateId`, `client.ipAddress`, `securityContext.asOrg` | actor |
| `github_audit` | GitHub org audit log: `action`, `actor`, `repo`, `actor_ip`, `programmatic_access_type` | actor, org |
| `cloudtrail` | CloudTrail record: `eventSource`, `eventName`, `userIdentity`, `sourceIPAddress`, `requestParameters` | actor, account |
| `google_workspace` | Reports API activity: `actor.email`, `events[].name` | actor |
| `app` | a startup's own audit log: `event`, `actor`, `ip`, `org` | actor, org |
| `gha_ci` / `runner_egress` | GitHub Actions job lines; egress from the build runner | run |
| `hiro` | Hiro structlog: `service`, `event`, `organization_id` → `task_id` → `plan_id` → `step_id` | org, plan |

Every event also carries `ts` so a harness can order mixed sources.

## What the cases show

Most of the positives are **pairs or sequences of lines that are innocent on
their own** (`alone_innocent: true`): a login and a token, a role change and
an export, a workflow push and a runner registration, a secret read and a POST.
That is the argument for judging windows per scope — actor, org, plan — rather
than lines. A few are single lines nobody should need a model for (root login,
CloudTrail stopped, bucket public, MFA off): those belong on a rule-based fast
lane that the model never gets to second-guess.

The negatives are the things that look alarming and are not: deploys, scanner
probes, vendor rate limits, the top-of-the-hour fan-out, Dependabot, the
on-call's 3 a.m. login from home, an admin exporting their own report. A judge
that flags these is a judge nobody reads.

`P1` is the first corpus to pass; `P2` is next.

## Cases from disclosed incidents, 2025–2026

Seventeen cases follow the paths of real compromises as acme's logs would show
them. Hosts, names and addresses are synthetic; the sequence of events is
taken from the public write-ups.

| case | incident | what the logs show |
|---|---|---|
| `supply-chain/ci-action-tags-retargeted` | TeamPCP: Trivy and KICS GitHub Actions, March 2026 | a retargeted action tag, runner egress to a look-alike host, the deploy key used from a datacenter minutes later |
| `supply-chain/pypi-package-pth-stealer` | TeamPCP: LiteLLM 1.82.7/1.82.8 on PyPI, March 2026 | a `.pth` stealer posting the environment on every Python start, then the deploy key abroad |
| `supply-chain/trojan-dependency-rat` | axios 1.14.1 / 0.30.4 maintainer hijack, March 2026 | a dependency the package never had, a postinstall, beacons, private repos cloned with a developer's token |
| `supply-chain/postinstall-ai-cli-inventory-then-repos-public` | nx "s1ngularity", August 2025 | a repo named `s1ngularity-repository-0` at install time; days later repos flipped public and renamed |
| `supply-chain/preinstall-worm-publishes-credentials-as-repos` | Shai-Hulud second wave, November 2025; "mini Shai-Hulud" with valid provenance, August 2026 | a preinstall fetching Bun, repos created from the runner with the worm's description |
| `supply-chain/action-prints-runner-memory` | tj-actions/changed-files, March 2025 | a base64 memory dump in the job log, the deploy key used abroad afterwards |
| `vendor/platform-breach-env-secrets-reused` | Railway environment-variable breach, October 2026 | every secret that lived in the vendor's env vars used from one address within a day |
| `saas/provider-keys-stolen-from-vendor-console` | OpenCode Console breach, October 2026 | a key that only ever called from one address calling from five countries in an hour |
| `saas/integration-token-stolen-bulk-read` | Salesloft Drift, August 2025 | an integration's token paging through everything from a host it never used |
| `ai-agent/repo-settings-redirect-model-credentials` | Claude Code CVE-2026-21852 | an agent's model calls going to the host a cloned repo's settings named |
| `ai-agent/agent-escapes-task-uses-found-credentials` | OpenAI's evaluation-sandbox escape, July 2026 | one credential, then a role, six secrets and three services in four minutes |
| `ai-agent/mcp-server-copies-every-message-out` | postmark-mcp 1.0.16, September 2025 | every notification also posted to a host no plan named |
| `vendor/ai-app-oauth-token-stolen-sso-pivot` | Vercel via Context.ai, April 2026 | an AI app's OAuth token reading an employee's Drive from a new host, then her SSO into the internal tools |

With four look-alikes that must not flag: a rotation the morning after a
vendor's notice, Dependabot bumping axios to a clean release, the scanner
action's ordinary run, and a repo made public as its README announced.

Sources: [TeamPCP (Unit 42)](https://unit42.paloaltonetworks.com/teampcp-supply-chain-attacks/),
[TeamPCP (Arctic Wolf)](https://arcticwolf.com/resources/blog/teampcp-supply-chain-attack-campaign-targets-trivy-checkmarx-kics-and-litellm-potential-downstream-impact-to-additional-projects/),
[axios (Elastic)](https://www.elastic.co/security-labs/how-we-caught-the-axios-supply-chain-attack),
[Railway (TechCrunch)](https://techcrunch.com/2026/10/22/developer-platform-railway-urges-all-users-to-revoke-secret-keys-after-cyberattack/),
[OpenCode Console (BleepingComputer)](https://bleepingcomputer.com/news/security/opencode-hack-exposes-api-keys-amid-ai-distillation-attacks),
[OpenAI incident (OpenAI)](https://openai.com/index/hugging-face-incident-and-the-road-ahead/),
[Vercel via Context.ai (TechCrunch)](https://techcrunch.com/2026/04/20/app-host-vercel-confirms-security-incident-says-customer-data-was-stolen-via-breach-at-context-ai/),
[Claude Code leak and CVE (NodeSource)](https://nodesource.com/blog/anthropic-claude-code-source-leak-bun-bug),
[mini Shai-Hulud (Socket)](https://socket.dev/blog/openapi-react-query-codegen-npm-compromise),
[npm compromise advisory (CISA)](https://www.cisa.gov/news-events/alerts/2025/09/23/widespread-supply-chain-compromise-impacting-npm-ecosystem).
