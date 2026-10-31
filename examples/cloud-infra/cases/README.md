# cases

What the judge has to catch, and what it must leave alone, written down as
event sequences before arguing about designs. Every design question (per
record or per window, which model, which cadence) is measured against these.
`make test` runs eight of them through norm and facts as fixtures;
the full set is replayed inside weeks of ordinary traffic for the numbers in
the pack's README.

Each case is a directory:

```
cases/<category>/<name>/
  case.yaml        # scope, cadence, sources, expected verdict, what to check first
  events.ndjson    # the sequence, in the shape of the real source
```

Nothing in here is real: no customer, person, repo or account exists. The
cases built on a product's own audit log or an AI agent's logs are in the
[app-log pack](../../app-log/cases/).

## Sources and their shapes

| `source` | shape |
|---|---|
| `cloudtrail` | CloudTrail record: `eventSource`, `eventName`, `userIdentity`, `sourceIPAddress`, `requestParameters` |
| `github_audit` | GitHub org audit log: `action`, `actor`, `repo`, `actor_ip`, `programmatic_access_type` |
| `okta` | Okta System Log: `eventType`, `actor.alternateId`, `client.ipAddress`, `securityContext.asOrg` |
| `google_workspace` | Workspace Reports API activity: `actor.email`, `ipAddress`, `events[].name` |
| `gha_ci` | GitHub Actions job lines: `repo`, `run_id`, `step`, `line` |
| `runner_egress` | egress from the build runner: `repo`, `run_id`, `dst_host`, `bytes_out` |
| `app` | a product's own audit log: `event`, `actor`, `ip`, `org` |
| `hiro` | an AI agent's structlog: `service`, `event`, `organization_id` → `task_id` → `plan_id` → `step_id` |

Every event also carries `ts`, so mixed sources order.

## The 34 cases

28 should flag, 6 must not. `P1` is the first corpus to pass; `P2` is next.

Most positives are sequences of lines innocent on their own
(`alone_innocent: true` in `case.yaml`): a login and a token, a role change
and an export, a workflow push and a runner registration, a secret read and a
POST. That is why the judge reads an actor's records together, not lines.
A few are single lines (root login, CloudTrail stopped, a bucket made public,
MFA off); the judge reads those like any other, with the profile's `watch`
and `root` facts beside them.

The negatives are what looks alarming and is not: Dependabot's weekly batch,
the scanner action's ordinary run, the on-call's 3 a.m. login from home, a
rotation the morning after a vendor's notice, a repo made public as its README
announced. A judge that flags these is a judge nobody reads.

### Cloud (CloudTrail)

| case | | sources | events | what the logs show |
|---|---|---|---|---|
| `cloud/assume-role-new-principal-then-reads` | flag P2 | `cloudtrail` | 21 | An external account assumes the read-only role and reads customer exports |
| `cloud/cloudtrail-stop-logging` | flag P1 | `cloudtrail` | 1 | CloudTrail logging stopped |
| `cloud/iam-key-create-attach-star-then-use` | flag P1 | `cloudtrail` | 5 | A new IAM user gets an access key and AdministratorAccess, then reads buckets from a new IP |
| `cloud/root-console-login` | flag P1 | `cloudtrail` | 1 | Root account console login without MFA |
| `cloud/s3-bucket-public` | flag P1 | `cloudtrail` | 2 | Public access block removed and a bucket policy grants everyone read |
| `cloud/sg-open-world` | flag P1 | `cloudtrail` | 1 | A security group opens Postgres to the internet |

### Identity

| case | | sources | events | what the logs show |
|---|---|---|---|---|
| `identity/admin-login-new-asn` | flag P1 | `okta` | 5 | Admin console login from a hosting-provider ASN in a new country |
| `identity/impossible-travel` | flag P1 | `okta` | 2 | Two successful logins, two continents, 22 minutes apart |
| `identity/mfa-disabled` | flag P1 | `okta` | 2 | Admin deactivates their own MFA factor, then signs in without it |

### GitHub

| case | | sources | events | what the logs show |
|---|---|---|---|---|
| `github/branch-protection-off-force-push` | flag P1 | `github_audit` | 3 | Branch protection removed, main force-pushed, protection restored |
| `github/new-owner-and-deploy-key` | flag P2 | `github_audit` | 2 | A contractor becomes org owner and adds a read-write deploy key |
| `github/pat-mass-push` | flag P1 | `github_audit` | 12 | One personal access token pushes to a dozen repos in three minutes |
| `github/pwn-request-workflow-from-fork` | flag P2 | `github_audit` | 2 | A PR from a fork edits a workflow that runs with secrets |
| `github/shai-hulud-repo-migration` | flag P1 | `github_audit` | 4 | Shai-Hulud: a repo named Shai-Hulud appears, a private repo goes public as -migration |
| `github/shai-hulud-workflow-runner` | flag P1 | `github_audit`, `runner_egress` | 4 | Shai-Hulud: a workflow file pushed straight to main, a runner registered, egress to webhook.site |

### Data

| case | | sources | events | what the logs show |
|---|---|---|---|---|
| `data/gmail-forwarding-rule` | flag P1 | `google_workspace` | 1 | Finance user's mailbox starts forwarding to an outside Gmail address |
| `data/snapshot-shared-external` | flag P1 | `cloudtrail` | 1 | An RDS snapshot is shared with another AWS account |

### Supply chain (CI, dependencies)

| case | | sources | events | what the logs show |
|---|---|---|---|---|
| `supply-chain/action-prints-runner-memory` | flag P1 | `gha_ci`, `cloudtrail` | 6 | A popular Action's tags are moved to a commit that prints the runner's memory, secrets included, into the job log (tj-actions/changed-files, March 2025) |
| `supply-chain/ci-action-tags-retargeted` | flag P1 | `gha_ci`, `runner_egress`, `cloudtrail` | 10 | A security scanner's Action has its version tags force-pushed to a stealer (TeamPCP: Trivy, KICS; March 2026) |
| `supply-chain/ci-postinstall-egress` | flag P1 | `gha_ci`, `runner_egress` | 6 | npm install runs a postinstall that talks to webhook.site from the build runner |
| `supply-chain/new-dependency-typosquat` | flag P2 | `github_audit`, `gha_ci` | 2 | A PR adds a two-day-old package whose name is one letter off a popular one |
| `supply-chain/postinstall-ai-cli-inventory-then-repos-public` | flag P1 | `github_audit` | 18 | nx/s1ngularity: a postinstall drives the developer's AI CLIs to inventory secrets; days later the stolen token flips repos public (Aug 2025) |
| `supply-chain/preinstall-worm-publishes-credentials-as-repos` | flag P1 | `gha_ci`, `runner_egress`, `github_audit` | 10 | Shai-Hulud, second wave: a preinstall fetches Bun, hunts credentials and publishes them as repos with the runner's token (Nov 2025; again with valid provenance, Aug 2026) |
| `supply-chain/pypi-package-pth-stealer` | flag P1 | `gha_ci`, `runner_egress`, `cloudtrail` | 10 | A backdoored PyPI release runs on every Python start and ships the runner's credentials (LiteLLM 1.82.8, March 2026) |
| `supply-chain/trojan-dependency-rat` | flag P1 | `gha_ci`, `runner_egress`, `github_audit` | 14 | A hijacked maintainer account publishes axios with a new dependency whose postinstall drops a RAT (March 2026) |

### A vendor's breach

| case | | sources | events | what the logs show |
|---|---|---|---|---|
| `vendor/ai-app-oauth-token-stolen-sso-pivot` | flag P1 | `google_workspace`, `okta`, `app` | 18 | An AI productivity app an employee connected to her Google account is breached; its OAuth token reads her Drive, then her SSO opens the internal tools (Vercel via Context.ai, April 2026) |
| `vendor/platform-breach-env-secrets-reused` | flag P1 | `cloudtrail`, `github_audit`, `app` | 24 | A hosting vendor's breach exposes stored environment variables; what was in them is used from a datacenter within a day (Railway, Oct 2026) |

### AI agents

| case | | sources | events | what the logs show |
|---|---|---|---|---|
| `ai-agent/agent-escapes-task-uses-found-credentials` | flag P1 | `hiro`, `cloudtrail` | 14 | An agent given one credential finds more and uses them across services in minutes (OpenAI's evaluation-sandbox escape, July 2026) |

### Must not flag

| case | | sources | events | what the logs show |
|---|---|---|---|---|
| `negative/dependabot-bumps-axios` | quiet P1 | `github_audit`, `gha_ci`, `runner_egress` | 5 | Dependabot bumps axios to a clean release and CI runs it |
| `negative/dependabot-prs` | quiet P1 | `github_audit` | 10 | Dependabot opens its weekly batch of PRs and CI runs them |
| `negative/oncall-3am-login` | quiet P1 | `okta`, `app` | 5 | The on-call engineer signs in at 3 a.m. from home, as every time they are paged |
| `negative/repo-made-public-as-planned` | quiet P1 | `github_audit` | 2 | An admin makes the docs repo public, as the README it pushed first announces |
| `negative/scanner-action-usual-run` | quiet P1 | `gha_ci`, `runner_egress` | 5 | The image scan step runs as it does on every build |
| `negative/secret-rotation-after-vendor-notice` | quiet P1 | `cloudtrail`, `app` | 10 | Admins rotate keys the morning after a vendor's breach notice |

## Cases from disclosed incidents, 2025–2026

11 of the cases follow the paths of real compromises as the logs would show them. Hosts, names and addresses are synthetic; the sequence of events is taken from the public write-ups.

| case | incident |
|---|---|
| `supply-chain/ci-action-tags-retargeted` | TeamPCP: Trivy and KICS GitHub Actions, March 2026 |
| `supply-chain/pypi-package-pth-stealer` | TeamPCP: LiteLLM 1.82.7/1.82.8 on PyPI, March 2026 |
| `supply-chain/trojan-dependency-rat` | axios 1.14.1 / 0.30.4 maintainer hijack, March 2026 |
| `supply-chain/postinstall-ai-cli-inventory-then-repos-public` | nx "s1ngularity", August 2025 |
| `github/shai-hulud-repo-migration` | Shai-Hulud, first wave, September 2025 |
| `github/shai-hulud-workflow-runner` | Shai-Hulud, first wave, September 2025 |
| `supply-chain/preinstall-worm-publishes-credentials-as-repos` | Shai-Hulud second wave, November 2025; "mini Shai-Hulud" with valid provenance, August 2026 |
| `supply-chain/action-prints-runner-memory` | tj-actions/changed-files, March 2025 |
| `vendor/platform-breach-env-secrets-reused` | Railway environment-variable breach, October 2026 |
| `vendor/ai-app-oauth-token-stolen-sso-pivot` | Vercel via Context.ai, April 2026 |
| `ai-agent/agent-escapes-task-uses-found-credentials` | OpenAI's evaluation-sandbox escape, July 2026 |

Sources: [TeamPCP (Unit 42)](https://unit42.paloaltonetworks.com/teampcp-supply-chain-attacks/),
[TeamPCP (Arctic Wolf)](https://arcticwolf.com/resources/blog/teampcp-supply-chain-attack-campaign-targets-trivy-checkmarx-kics-and-litellm-potential-downstream-impact-to-additional-projects/),
[axios (Elastic)](https://www.elastic.co/security-labs/how-we-caught-the-axios-supply-chain-attack),
[npm compromise advisory (CISA)](https://www.cisa.gov/news-events/alerts/2025/09/23/widespread-supply-chain-compromise-impacting-npm-ecosystem),
[mini Shai-Hulud (Socket)](https://socket.dev/blog/openapi-react-query-codegen-npm-compromise),
[Railway (TechCrunch)](https://techcrunch.com/2026/10/22/developer-platform-railway-urges-all-users-to-revoke-secret-keys-after-cyberattack/),
[Vercel via Context.ai (TechCrunch)](https://techcrunch.com/2026/04/20/app-host-vercel-confirms-security-incident-says-customer-data-was-stolen-via-breach-at-context-ai/),
[OpenAI incident (OpenAI)](https://openai.com/index/hugging-face-incident-and-the-road-ahead/).
