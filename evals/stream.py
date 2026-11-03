#!/usr/bin/env python3
"""Turn the case corpus into one stream, the way a pipeline would see it.

    python3 -I evals/stream.py         # writes evals/runs/stream/{stream.ndjson,truth.json}
    python3 -I evals/stream.py v3      # gen.py's history cases and their history: {stream,truth}-v3.*

Months of ordinary traffic for one startup (acme) across every source the
cases use, with every case on a day of its own (two that share an actor ten
days apart or more), each with its own context events shifted along, so no
case's history holds another case. Every event is synthetic; nothing is
real.

The point of the noise is to give every scope a history: the window node's
`new` (first sightings) and `usual` (the rate before the window) only mean
something against days of ordinary behavior, and a judge that only ever sees
the interesting hour never learns what boring looks like.

`truth.json` says, per case, which keys (the normalized actor and org, as the
packs compute `_norm.actor` and `_norm.org`) and which two hours a flag must
touch to count for that case, and whether it should flag.

The Stratus cases come from stratus/cases.json (splice.py's output, committed).
Everything goes to evals/runs/, which git ignores.
"""
import json
import math
import os
import random
import sys
from collections import Counter
from datetime import datetime, timedelta, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import gen  # noqa: E402  (builds gen.CASES on import)

OUT = os.path.join(HERE, "runs", "stream")
START = datetime(2026, 9, 20, 0, 0, tzinfo=timezone.utc)   # history begins
LIVE = datetime(2026, 10, 20, 13, 0, tzinfo=timezone.utc)   # first case lands
END = datetime(2026, 10, 28, 0, 0, tzinfo=timezone.utc)
SHARED_GAP_DAYS = 10
STRATUS = os.path.join(HERE, "stratus", "cases.json")
rng = random.Random(7)

SF = gen.SF
HOME = dict(ip="67.180.22.4", country="US", city="Oakland", asn=7922, as_org="Comcast Cable")
USERS = ["alice", "bob", "carol", "dave", "erin", "grace", "oncall-frank", "admin", "cfo"]
DEVS = {"dev-sam": ["acme/web", "acme/api"], "dev-mallory": ["acme/web"], "eve": ["acme/infra"],
        "dave": ["acme/api"], "grace": ["acme/docs"]}
REPOS = ["acme/web", "acme/api", "acme/infra", "acme/docs"]


def iso(t):
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def parse(s):
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)


def jitter(minutes):
    return timedelta(seconds=rng.uniform(0, minutes * 60))


def weekdays():
    d = START
    while d < END:
        if d.weekday() < 5:
            yield d
        d += timedelta(days=1)


def days():
    d = START
    while d < END:
        yield d
        d += timedelta(days=1)


# ---- the normalizer's view, mirrored for truth.json ---------------------------

# The company's own infrastructure is one partition, the company; a product's
# logs partition by customer (the profile's `org` and `own_sources`).
OWN_ORG = "acme"
OWN_SOURCES = {"cloudtrail", "github_audit", "okta", "google_workspace", "gha_ci", "runner_egress"}


def norm_keys(e):
    """(actor, org) as the packs compute `_norm.actor` and `_norm.org`."""
    actor, org = raw_keys(e)
    if e.get("source") in OWN_SOURCES:
        return actor, OWN_ORG
    return actor, org


def cloudtrail_actor(e):
    """Who acted, as the aws pack's `cloudtrail_actor` says: an IAM
    user by name; a role session by its role, except an SSO role, by the
    person; AWS acting on its own, `aws:<service>`; another account,
    `account:<id>`."""
    service = aws_acting(e)
    if service:
        return f"aws:{service}"
    ui = e.get("userIdentity") or {}
    kind = ui.get("type") or ""
    if kind == "AssumedRole":
        issuer = (ui.get("sessionContext") or {}).get("sessionIssuer") or {}
        role = issuer.get("userName")
        if not role:
            return ui.get("userName") or ui.get("arn")
        if role.startswith("AWSReservedSSO_"):
            session = (ui.get("arn") or "").rsplit("/", 1)[-1]
            return session or role
        return role
    if kind == "AWSAccount":
        return f"account:{ui['accountId']}" if ui.get("accountId") else None
    if kind == "Root":
        return ui.get("userName") or "root"
    return ui.get("userName") or ui.get("arn")


def aws_acting(e):
    """As the aws pack's `aws_acting`: the service when AWS acted on its own (a
    service-linked role's session, by its path; an `AWSService` call; a call
    only `invokedBy` names), else None."""
    ui = e.get("userIdentity") or {}
    kind = ui.get("type") or ""
    if kind == "AssumedRole":
        issuer = (ui.get("sessionContext") or {}).get("sessionIssuer") or {}
        return linked_service(issuer.get("arn"))
    if kind == "AWSService" or (not kind and ui.get("invokedBy")):
        return invoker(e)
    return None


def linked_service(arn):
    """As the plugin's `linked_service`: the service of a role whose path
    starts with `aws-service-role/`, else None (`role/x/aws-service-role/foo`
    is anyone's)."""
    _, sep, path = (arn or "").partition(":role/")
    if not sep or not path.startswith("aws-service-role/"):
        return None
    return named(path.removeprefix("aws-service-role/").split("/")[0]) or "service"


def invoker(e):
    """As the plugin's `invoker`: `invokedBy`, else an `*.amazonaws.com`
    address, else the service-linked role assumed, else the event's service."""
    ui = e.get("userIdentity") or {}
    ip = e.get("sourceIPAddress") or ""
    return (named(ui.get("invokedBy"))
            or named(ip if ip.endswith(".amazonaws.com") else None)
            or linked_service((e.get("requestParameters") or {}).get("roleArn"))
            or named(e.get("eventSource"))
            or "service")


def named(s):
    """As the plugin's `named`: a service without `.amazonaws.com`; `AWS
    Internal` (or nothing) names none."""
    s = (s or "").removesuffix(".amazonaws.com")
    return s if s and s != "AWS Internal" else None


def raw_keys(e):
    s = e.get("source")
    if s == "okta":
        return e["actor"]["alternateId"], None
    if s == "github_audit":
        return e.get("actor"), e.get("org")
    if s == "cloudtrail":
        return cloudtrail_actor(e), (e.get("userIdentity") or {}).get("accountId")
    if s == "google_workspace":
        return e["actor"]["email"], None
    if s == "app":
        return e.get("actor"), e.get("org")
    if s in ("gha_ci", "runner_egress"):
        repo = e.get("repo")
        return repo, repo.split("/")[0] if repo else None
    if s == "agent":
        return None, e.get("organization_id")
    return e.get("actor"), e.get("org")


# ---- ordinary days -----------------------------------------------------------

def noise():
    ev = []
    # Okta: everyone signs in on weekdays from the office; the on-call signs in
    # at night from home a few times a week.
    for d in weekdays():
        for u in USERS:
            t = d + timedelta(hours=15, minutes=30) + jitter(120)
            ev.append(gen.okta(iso(t), "user.session.start", f"{u}@acme.io", **SF))
            if rng.random() < 0.5:
                ev.append(gen.okta(iso(d + timedelta(hours=20) + jitter(90)), "user.session.start",
                                   f"{u}@acme.io", **SF))
        if rng.random() < 0.45:
            ev.append(gen.okta(iso(d + timedelta(hours=9) + jitter(120)), "user.session.start",
                               "oncall-frank@acme.io", **HOME))
    # The app's own audit log: logins and page views from the office; a service
    # key reading findings through business hours; the on-call acking at night.
    for d in weekdays():
        for u in USERS:
            t = d + timedelta(hours=15, minutes=40) + jitter(120)
            ev.append(gen.app(iso(t), "auth.login", f"{u}@acme.io", SF["ip"], method="sso", mfa=True,
                              result="success"))
            for _ in range(rng.randint(1, 4)):
                ev.append(gen.app(iso(t + jitter(300)), "page.view", f"{u}@acme.io", SF["ip"],
                                  path=rng.choice(["/findings", "/plans", "/settings", "/reports"])))
        for h in range(14, 24):
            for _ in range(rng.randint(2, 5)):
                ev.append(gen.app(iso(d + timedelta(hours=h) + jitter(60)), "api_request", "ak_1a1",
                                  "165.227.44.19", method="GET", path="/v1/findings?limit=50", status=200,
                                  bytes=rng.randint(8000, 40000)))
        if rng.random() < 0.45:
            t = d + timedelta(hours=9) + jitter(120)
            ev.append(gen.app(iso(t), "auth.login", "oncall-frank@acme.io", HOME["ip"], method="sso",
                              mfa=True, result="success"))
            ev.append(gen.app(iso(t + jitter(10)), "incident.ack", "oncall-frank@acme.io", HOME["ip"],
                              incident=f"INC-{rng.randint(400, 499)}"))
    # GitHub: developers push and open pull requests; Dependabot once a week.
    for d in weekdays():
        for dev, repos in DEVS.items():
            for _ in range(rng.randint(2, 6)):
                t = d + timedelta(hours=16) + jitter(480)
                repo = rng.choice(repos).split("/")[1]
                ev.append(gen.gh(iso(t), "git.push", dev, repo=repo, ref=f"refs/heads/{dev}/work"))
            if rng.random() < 0.7:
                t = d + timedelta(hours=17) + jitter(420)
                ev.append(gen.gh(iso(t), "pull_request.create", dev, repo=rng.choice(repos).split("/")[1],
                                 title=rng.choice(["Fix flaky test", "Bump deps", "Refactor poller", "Docs"])))
        if rng.random() < 0.6:
            ev.append(gen.gh(iso(d + timedelta(hours=18) + jitter(240)), "pull_request.merge", "dave",
                             repo=rng.choice(REPOS).split("/")[1]))
        if d.weekday() == 0:
            for i in range(2):
                ev.append(gen.gh(iso(d + timedelta(hours=7, minutes=i)), "pull_request.create", "dependabot[bot]",
                                 repo="web", token="github app", title=f"Bump dep-{rng.randint(1, 60)}"))
    # CloudTrail: the deploy role assumes and describes; a read-only role reads
    # a few objects; dave signs in to the console with MFA twice a week; the
    # backup user snapshots the database every morning.
    for d in days():
        for _ in range(rng.randint(4, 8)):
            t = d + timedelta(hours=14) + jitter(540)
            ev.append(gen.ct(iso(t), "sts.amazonaws.com", "AssumeRole", "deploy-bot",
                             request={"roleArn": "arn:aws:iam::111122223333:role/deploy", "roleSessionName": "deploy"}))
            ev.append(gen.ct(iso(t + jitter(5)), "ec2.amazonaws.com", "DescribeInstances", "deploy-bot"))
        for _ in range(rng.randint(2, 6)):
            ev.append(gen.ct(iso(d + timedelta(hours=13) + jitter(600)), "s3.amazonaws.com", "GetObject",
                             "prod-readonly", request={"bucketName": "acme-prod-reports",
                                                       "key": f"daily/{d.strftime('%Y%m%d')}.json"}))
        ev.append(gen.ct(iso(d + timedelta(hours=6) + jitter(10)), "rds.amazonaws.com", "CreateDBSnapshot",
                         "svc-backup2", request={"dBInstanceIdentifier": "prod-db",
                                                 "dBSnapshotIdentifier": f"prod-db-{d.strftime('%Y%m%d')}"},
                         agent="aws-sdk-go/1.44"))
        if d.weekday() in (0, 3):
            ev.append(gen.ct(iso(d + timedelta(hours=16) + jitter(120)), "signin.amazonaws.com", "ConsoleLogin",
                             "dave", ip=SF["ip"], agent="Mozilla/5.0", mfa="Yes",
                             response={"ConsoleLogin": "Success"}))
    # Google Workspace: logins.
    for d in weekdays():
        for u in ("cfo", "alice", "bob"):
            t = d + timedelta(hours=15) + jitter(180)
            ev.append({"ts": iso(t), "source": "google_workspace",
                       "id": {"time": iso(t), "applicationName": "login"},
                       "actor": {"email": f"{u}@acme.io", "callerType": "USER"}, "ipAddress": SF["ip"],
                       "events": [{"type": "login", "name": "login_success",
                                   "parameters": [{"name": "login_type", "value": "google_password"}]}]})
    # The AI agent's own log, org_acme: health checks every ten minutes, the
    # poller fanning out on the hour, two plans a day that read a secret and
    # talk to the hosts the plan is about.
    plan_no = 100
    for d in days():
        for m in range(0, 24 * 60, 10):
            t = d + timedelta(minutes=m) + timedelta(seconds=rng.randint(0, 9))
            ev.append(gen.agent(iso(t), "agent-api", "http_request", method="GET", path="/health",
                               status=200, client_ip="10.0.1.10", user_agent="ELB-HealthChecker/2.0",
                               duration_ms=rng.randint(1, 4)))
        for h in range(24):
            t0 = d + timedelta(hours=h)
            for i in range(6):
                ev.append(gen.agent(iso(t0 + timedelta(seconds=i * 2)), "agent-poller", "outbound_request",
                                   destination=f"https://api.github.com/repos/acme/repo-{i:02d}/actions",
                                   method="GET", status=200))
            ev.append(gen.agent(iso(t0 + timedelta(seconds=20)), "agent-poller",
                               "integration_sync_completed", vendor="github", objects=rng.randint(40, 120)))
        for _ in range(2):
            plan_no += 1
            p, task = f"p-{plan_no}", f"t-{6000 + plan_no}"
            t = d + timedelta(hours=rng.randint(14, 22), minutes=rng.randint(0, 59))
            kind = rng.choice(["rotate", "patch"])
            steps = ["rotate_iam_key", "update_vault", "notify"] if kind == "rotate" else \
                    ["patch_dependency", "open_pr", "notify"]
            hosts = ["sts.amazonaws.com", "vault.acme.internal"] if kind == "rotate" else \
                    ["api.github.com", "registry.npmjs.org"]
            W = "agent-plan-worker"
            ev.append(gen.agent(iso(t), W, "plan_started", task=task, plan=p, steps=steps))
            for i, (step, host) in enumerate(zip(steps[:2], hosts), start=1):
                s = f"s-{i}"
                t += timedelta(seconds=rng.randint(5, 20))
                ev.append(gen.agent(iso(t), W, "step_started", task=task, plan=p, step=s, step_name=step))
                if i == 1:
                    t += timedelta(seconds=rng.randint(2, 8))
                    ev.append(gen.agent(iso(t), W, "secret_read", task=task, plan=p, step=s,
                                       path="aws/prod/deploy-bot" if kind == "rotate" else "github/acme/bot"))
                t += timedelta(seconds=rng.randint(2, 8))
                ev.append(gen.agent(iso(t), W, "outbound_request", task=task, plan=p, step=s,
                                   destination=f"https://{host}/", method="POST", status=200,
                                   bytes=rng.randint(300, 2000)))
                t += timedelta(seconds=rng.randint(2, 8))
                ev.append(gen.agent(iso(t), W, "step_completed", task=task, plan=p, step=s, step_name=step,
                                   duration_ms=rng.randint(400, 3000)))
            t += timedelta(seconds=rng.randint(2, 8))
            ev.append(gen.agent(iso(t), W, "step_completed", task=task, plan=p, step="s-3", step_name="notify",
                               duration_ms=rng.randint(100, 500)))
            t += timedelta(seconds=2)
            ev.append(gen.agent(iso(t), W, "plan_completed", task=task, plan=p, steps=len(steps)))
    # CI: builds of web and api a few times a day, each talking to the registry
    # and back to GitHub.
    run = 50000
    for d in weekdays():
        for _ in range(rng.randint(2, 4)):
            run += 1
            repo = rng.choice(["acme/web", "acme/api"])
            t = d + timedelta(hours=16) + jitter(480)
            ev.append(gen.ci(iso(t), repo, run, "npm ci", f"added {rng.randint(900, 1300)} packages in {rng.randint(30, 60)}s"))
            ev.append(gen.egress(iso(t + timedelta(seconds=5)), repo, run, "registry.npmjs.org", bytes_out=rng.randint(2000, 9000)))
            ev.append(gen.ci(iso(t + timedelta(seconds=50)), repo, run, "npm run build", f"built in {rng.randint(8, 30)}s"))
            ev.append(gen.egress(iso(t + timedelta(seconds=70)), repo, run, "api.github.com", bytes_out=rng.randint(500, 1500),
                                 method="POST", path=f"/repos/{repo}/statuses/{rng.randrange(16**8):08x}"))
    return ev


# ---- the cases, three hours apart ----------------------------------------------

def case_actors(c):
    return {a for a, _ in map(norm_keys, c["events"]) if a}


def quiet_before(d):
    """No case on Sunday or Monday: the weekend is near silent, so the org's
    latest records before them reach back to the case before."""
    return (LIVE + timedelta(days=d)).weekday() in (6, 0)


def actor_gaps(ordinary):
    """Days between two cases that share an actor: long enough for 30 of its
    ordinary records to come between, so the earlier case has left its
    latest records; SHARED_GAP_DAYS at least."""
    days = max((END - START).days, 1)
    per_day = Counter(a for a, _ in map(norm_keys, ordinary) if a)
    return lambda a: max(SHARED_GAP_DAYS, math.ceil(30 / per_day[a] * days) if per_day[a] else 120)


def schedule(cases, gap):
    """A day for each case, counted from LIVE. A case occupies every calendar
    day its events touch (a case's context can begin days before it, or run
    past midnight); no two cases share a day, none begins on a quiet day
    (`quiet_before`), and two that share an actor are `gap(actor)` days
    apart, so a working evening lies between two cases and neither's history
    holds the other. History is each scope's latest records however long
    ago, so nothing shorter keeps cases apart."""
    taken, spans_of, days = set(), {}, []
    for c in cases:
        actors = case_actors(c)
        offs = [(parse(e["ts"]).date() - LIVE.date()).days for e in c["events"]]
        lo, hi = min(offs), max(offs)
        d = -lo
        while (any(x in taken for x in range(d + lo, d + hi + 1)) or quiet_before(d + lo)
               or any(d + lo - b < gap(a) and a_ - (d + hi) < gap(a)
                      for a in actors for a_, b in spans_of.get(a, ()))):
            d += 1
        taken.update(range(d + lo, d + hi + 1))
        days.append(d)
        for a in actors:
            spans_of.setdefault(a, []).append((d + lo, d + hi))
    return days


def place(cases, deltas):
    """Each case shifted by its delta from where gen.py and splice.py put it."""
    out, truth = [], []
    for c, delta in zip(cases, deltas):
        events = []
        for e in c["events"]:
            e = json.loads(json.dumps(e))
            t = parse(e["ts"]) + delta
            e["ts"] = iso(t)
            for f in ("published", "@timestamp", "eventTime", "timestamp"):
                if f in e:
                    e[f] = e["ts"]
            if "id" in e and isinstance(e["id"], dict) and "time" in e["id"]:
                e["id"]["time"] = e["ts"]
            # No marker on the event: the judge must not be told which case it is.
            events.append(e)
        last = max(parse(e["ts"]) for e in events)
        live_from = last - timedelta(hours=2)
        live = [e for e in events if parse(e["ts"]) >= live_from]
        actors = Counter(a for a, _ in map(norm_keys, live) if a)
        orgs = Counter(o for _, o in map(norm_keys, live) if o)
        truth.append({
            "id": c["id"], "flag": c["expected"]["flag"], "priority": c["priority"], "scope": c["scope"],
            "one_line": c["expected"]["one_line"],
            "actor_key": actors.most_common(1)[0][0] if actors else None,
            "org_key": orgs.most_common(1)[0][0] if orgs else None,
            # Every principal and org the case touches in its live window: a
            # case that moves from a build runner to a stolen deploy key is
            # caught whichever of them reaches the judge.
            "actor_keys": sorted(actors), "org_keys": sorted(orgs),
            "live_from": iso(live_from), "live_to": iso(last), "events": len(events),
            # When each of its events happened: the fast eval judges the
            # buffers that hold them.
            "times": sorted(e["ts"] for e in events),
        })
        out.extend(events)
    return out, truth


def placed_cases(v3=False):
    """Our cases, Stratus Red Team's recorded techniques and their harmless
    twins (stratus/cases.json, from stratus/splice.py), taken in turn so no
    suite gets all the early days, each on its own day (`schedule`). v3's
    cases come last, so every other case keeps its day."""
    ours = sorted(gen.CASES[:gen.V3_FROM], key=lambda c: c["id"])
    if v3:
        ours = [dict(c, events=usual_signins(c["events"])) for c in ours]
    if not os.path.exists(STRATUS):  # without it the corpus is 140 cases short, and nothing would say so
        sys.exit(f"{STRATUS} is missing: it is committed; restore it, or run python3 -I evals/stratus/splice.py")
    extra = json.load(open(STRATUS))
    suites = [ours, [c for c in extra if c["id"].startswith("stratus/")], [c for c in extra if c["id"].startswith("control/")]]
    order = [suite[i] for i in range(max(map(len, suites))) for suite in suites if i < len(suite)]
    if v3:
        order += sorted(gen.CASES[gen.V3_FROM:], key=lambda c: c["id"])
    # The ordinary days exactly as main() makes them (same seed, its own Random).
    ordinary = noise_between(START, END, seed=7)
    gap = actor_gaps(ordinary)
    if v3:  # an actor only v3 has (an IdC username) by v3's days; the rest as before, so no v2 case moves
        known, more = {a for a, _ in map(norm_keys, ordinary) if a}, actor_gaps(v3_noise(ordinary, START, END, seed=13))
        gap = (lambda g2: lambda a: g2(a) if a in known else more(a))(gap)
    return place(order, [timedelta(days=d) for d in schedule(order, gap)])


def noise_between(start, end, seed):
    """Ordinary days from `start` to `end`, from their own seed, so the days
    before stay exactly as they were."""
    global START, END, rng
    saved = START, END, rng
    START, END, rng = start, end, random.Random(seed)
    try:
        return noise()
    finally:
        START, END, rng = saved


# ---- v3: the history a person's header reads ---------------------------------

MEMBERS = list(gen.SETS)
POOL = (["sts:GetCallerIdentity"] * 4 + ["s3:ListBuckets", "ec2:DescribeInstances"] * 3
        + ["logs:DescribeLogGroups", "logs:FilterLogEvents"] * 2
        + ["ec2:DescribeSecurityGroups", "ec2:DescribeVolumes", "ec2:DescribeRegions", "ssm:DescribeInstanceInformation",
           "ssm:DescribeParameters", "ssm:ListCommands", "secretsmanager:ListSecrets", "secretsmanager:DescribeSecret",
           "secretsmanager:ListSecretVersionIds", "iam:GetAccountSummary", "iam:ListRoles", "iam:GetUser",
           "cloudtrail:DescribeTrails", "cloudtrail:GetTrailStatus", "lambda:ListFunctions20150331",
           "lambda:GetFunction20150331v2", "rds:DescribeDBInstances", "bedrock:ListFoundationModels",
           "bedrock:GetFoundationModel", "organizations:DescribeOrganization", "rolesanywhere:ListTrustAnchors",
           "route53resolver:ListResolverQueryLogConfigs", "ecs:ListClusters", "ecs:DescribeServices"])
CONSOLE = (["ec2:DescribeInstances", "monitoring:GetMetricData"] * 2
           + ["monitoring:DescribeAlarms", "logs:DescribeLogGroups", "logs:StartQuery", "s3:ListBuckets", "s3:GetBucketLocation",
              "rds:DescribeDBInstances", "ecs:ListClusters", "ecs:DescribeServices", "health:DescribeEventAggregates"])
COSTS = ["sts:GetCallerIdentity", "ce:GetCostAndUsage", "ce:GetCostAndUsage", "ce:GetCostForecast", "budgets:DescribeBudgets"]
PACKAGES = ["eslint", "vite", "typescript", "zod", "react", "prettier", "vitest", "date-fns"]


def params(r, call):
    """The parameters a read names, when it names one."""
    pick = {"secretsmanager:DescribeSecret": lambda: {"secretId": r.choice(["prod/db", "prod/stripe", "staging/db"])},
            "secretsmanager:ListSecretVersionIds": lambda: {"secretId": r.choice(["prod/db", "prod/stripe", "staging/db"])},
            "lambda:GetFunction20150331v2": lambda: {"functionName": r.choice(["acme-api-handler", "acme-cron"])},
            "logs:FilterLogEvents": lambda: {"logGroupName": r.choice(["/ecs/acme-api", "/ecs/acme-web"])},
            "cloudtrail:GetTrailStatus": lambda: {"name": "acme-trail"},
            "bedrock:GetFoundationModel": lambda: {"modelIdentifier": "anthropic.claude-haiku-5-5"},
            "ecs:DescribeServices": lambda: {"cluster": "acme-prod", "services": ["api"]}}.get(call)
    return pick() if pick else None


def every(start, end, days, who, salt):
    """`who`'s dates every `days` days from an offset of their own, moved off weekends."""
    d = start + timedelta(days=int(gen.h(who, salt), 16) % days)
    while d < end:
        yield d + timedelta(days=(7 - d.weekday()) % 7 if d.weekday() >= 5 else 0)
        d += timedelta(days=days)


def net_of(e):
    c, sc = e["client"], e["securityContext"]
    return dict(ip=c["ipAddress"], country=c["geographicalContext"]["country"], city=c["geographicalContext"]["city"],
                asn=sc["asNumber"], as_org=sc["asOrg"])


def usual_signins(events):
    """A case's sign-in from where its person always signs in (the office, the
    on-call's home), written as v3's ordinary days write one; the rest as it was."""
    out = []
    for e in events:
        if (e["source"] == "okta" and e["eventType"] == "user.session.start" and e["outcome"]["result"] == "SUCCESS"
                and e["client"]["ipAddress"] in (SF["ip"], HOME["ip"]) and "authenticationContext" not in e):
            out += gen.signin(gen.at(e["ts"], -3), e["actor"]["alternateId"], net_of(e), gen.device(e["actor"]["alternateId"]))
        else:
            out.append(e)
    return out


def v3_noise(ordinary, start, end, seed):
    """What v3 adds to the ordinary days. Okta sign-ins as Okta writes them
    (policy, password, push, the laptop's device id), now and then a mistyped
    password, a password change, a helpdesk MFA reset and the new factor. Each
    member's own AWS work: aws-cli reads with their IAM user's key, console
    sessions through Okta and IAM Identity Center, a password change and a new
    key every 90 days. GitHub token ids and clones; Workspace's login
    challenge. And the routine four look-alike cases claim: the scanner step in
    every api build, CI for Dependabot's PRs, admin's monthly key rotation,
    grace making an example repo public."""
    r = random.Random(seed)
    jit = lambda minutes: timedelta(seconds=r.uniform(0, minutes * 60))
    out, run = [], 70000
    for e in ordinary:
        s = e["source"]
        if s == "okta" and e["eventType"] == "user.session.start":
            who, net = e["actor"]["alternateId"], net_of(e)
            t0 = gen.at(e["ts"], -3)
            if r.random() < 0.04:
                out += gen.signin(gen.at(t0, -r.randint(20, 50)), who, net, gen.device(who), fail=True)
            out += gen.signin(t0, who, net, gen.device(who))
            continue
        e = json.loads(json.dumps(e))
        if s == "google_workspace" and e["events"][0]["name"] == "login_success":
            e["events"][0]["parameters"] += [{"name": "login_challenge_method", "multiValue": ["password", "google_prompt"]},
                                             {"name": "is_suspicious", "boolValue": False}]
        if s == "github_audit" and e["actor"] in DEVS and e.get("programmatic_access_type") == "personal access token":
            e.update(gen.token(e["actor"]))
        if s == "github_audit" and e["actor"] == "dependabot[bot]" and e["action"] == "pull_request.create":
            pkg = PACKAGES[int(gen.h(e["ts"]), 16) % len(PACKAGES)]
            major, minor = int(gen.h(pkg), 16) % 9 + 1, int(gen.h(e["ts"], 1), 16) % 20
            e.update(title=f"build(deps): bump {pkg} from {major}.{minor}.0 to {major}.{minor + 1}.0",
                     files_changed=["package.json", "package-lock.json"])
            run += 1
            out += [e, gen.gh(gen.at(e["ts"], 12), "workflows.created_workflow_run", "dependabot[bot]", repo="web", workflow="ci.yml",
                              event="pull_request", token="github app"),
                    gen.ci(gen.at(e["ts"], 60), "acme/web", run, "npm ci", f"added {1100 + run % 200} packages in 41s"),
                    gen.egress(gen.at(e["ts"], 61), "acme/web", run, "registry.npmjs.org", method="GET",
                               path=f"/{pkg}/-/{pkg}-{major}.{minor + 1}.0.tgz", bytes_out=500),
                    gen.ci(gen.at(e["ts"], 120), "acme/web", run, "npm run build", "build complete in 38s")]
            continue
        out.append(e)
        if s == "gha_ci" and e["repo"] == "acme/api" and e["step"] == "npm run build":
            step, n = "Run aquasecurity/trivy-action@0.28.0", e["run_id"]
            out += [gen.ci(gen.at(e["ts"], 5), "acme/api", n, step, "Downloading action archive"),
                    gen.egress(gen.at(e["ts"], 6), "acme/api", n, "github.com", method="GET",
                               path="/aquasecurity/trivy-action/archive/refs/tags/0.28.0.tar.gz", bytes_out=300),
                    gen.ci(gen.at(e["ts"], 12), "acme/api", n, step, "Trivy DB updated"),
                    gen.egress(gen.at(e["ts"], 12), "acme/api", n, "ghcr.io", method="GET",
                               path="/v2/aquasecurity/trivy-db/manifests/2", bytes_out=600),
                    gen.ci(gen.at(e["ts"], 30), "acme/api", n, step, f"{n % 4} vulnerabilities found (LOW)")]

    office = gen.OFFICE["ip"]
    rotations = {who: [d + timedelta(hours=17) + jit(180) for d in every(start, end, 90, who, "key")] for who in MEMBERS}
    for who, ts_ in rotations.items():
        for n, t in enumerate(ts_):
            login, sess, user = iso(t), "ASIA" + gen.h(who, t)[:16].upper(), who
            out += [gen.aws(login, "signin:ConsoleLogin", who, office, None, agent=gen.BROWSER, response={"ConsoleLogin": "Success"}),
                    gen.aws(iso(t + timedelta(minutes=2)), "iam:ChangePassword", who, office, sess, agent="console.amazonaws.com", console=login),
                    gen.aws(iso(t + timedelta(minutes=4)), "iam:CreateAccessKey", who, office, sess, agent="console.amazonaws.com",
                            console=login, request={"userName": user},
                            response={"accessKey": {"accessKeyId": gen.key(who, n + 1), "status": "Active", "userName": user,
                                                    "createDate": iso(t + timedelta(minutes=4))}}),
                    gen.aws(iso(t + timedelta(days=1) + jit(60)), "iam:DeleteAccessKey", who, office, gen.key(who, n + 1),
                            request={"userName": user, "accessKeyId": gen.key(who, n)})]
    for who in MEMBERS:
        for d in every(start, end, 90, who, "password"):
            out.append(gen.okta_on(iso(d + timedelta(hours=16, minutes=30) + jit(120)), "user.account.update_password", who,
                                   gen.OFFICE, gen.device(who), target=[{"type": "User", "alternateId": who}],
                                   display="User update password for Okta"))
        for d in every(start, end, 365, who, "phone"):  # a new phone: the helpdesk (admin) resets, the new factor is enrolled
            t = d + timedelta(hours=16) + jit(240)
            out += ([gen.reset(iso(t), who)] if who != "admin@acme.io" else []) + gen.signin(
                iso(t + timedelta(minutes=15) + jit(30)), who, HOME if who.startswith("oncall") else gen.OFFICE, gen.device(who), enroll=True)

    d = start
    while d < end:
        workday = d.weekday() < 5
        for who in MEMBERS:
            n = sum(t <= d for t in rotations[who])
            cfo = who.startswith("cfo")
            if workday and r.random() < (0.15 if cfo else 0.5):
                t = d + timedelta(hours=16) + jit(420)
                for _ in range(r.randint(3, 9)):
                    t += timedelta(seconds=r.uniform(5, 90))
                    c = r.choice(COSTS if cfo else POOL)
                    out.append(gen.aws(iso(t), c, who, office, gen.key(who, n), request=params(r, c)))
            if who.startswith("oncall") and r.random() < 0.2:
                t = d + timedelta(hours=9) + jit(120)
                for _ in range(r.randint(2, 6)):
                    t += timedelta(seconds=r.uniform(5, 60))
                    c = r.choice(POOL)
                    out.append(gen.aws(iso(t), c, who, HOME["ip"], gen.key(who, n), request=params(r, c)))
            if workday and r.random() < 0.3:
                t = d + timedelta(hours=17) + jit(300)
                since = iso(t + timedelta(seconds=20))
                out += [gen.okta_on(iso(t), "user.authentication.sso", who, gen.OFFICE, gen.device(who),
                                    target=[{"type": "AppInstance", "displayName": "AWS IAM Identity Center"}]),
                        gen.saml(since, who, office)]
                for _ in range(r.randint(3, 8)):
                    t += timedelta(seconds=r.uniform(20, 120))
                    c = r.choice(COSTS[1:] if cfo else CONSOLE)
                    out.append(gen.sso(iso(t), c, who, office, since))
        if workday:
            for dev, repos in DEVS.items():
                if r.random() < 0.6:
                    for _ in range(r.randint(1, 3)):
                        out.append(gen.gh(iso(d + timedelta(hours=16) + jit(480)), r.choice(["git.fetch"] * 4 + ["git.clone"]), dev,
                                          repo=r.choice(repos).split("/")[1], **gen.token(dev)))
        d += timedelta(days=1)

    for k, d in enumerate(every(start, end, 28, "admin@acme.io", "rotate")):
        t = d + timedelta(hours=16) + jit(60)
        out.append(gen.aws(iso(t), "signin:ConsoleLogin", "admin@acme.io", office, None, agent=gen.BROWSER,
                           response={"ConsoleLogin": "Success"}))
        for i, u in enumerate(r.sample(["deploy-bot", "railway-deploy", "svc-backup2"], r.randint(1, 2))):
            out += [gen.ct(iso(t + timedelta(minutes=2 + i * 4)), "iam.amazonaws.com", "CreateAccessKey", "admin", ip=office,
                           agent="console.amazonaws.com", mfa="Yes", request={"userName": u}),
                    gen.ct(iso(t + timedelta(minutes=3 + i * 4)), "iam.amazonaws.com", "DeleteAccessKey", "admin", ip=office,
                           agent="console.amazonaws.com", mfa="Yes", request={"userName": u, "accessKeyId": gen.key(u, k)})]
        out += [gen.app(iso(t + timedelta(minutes=12)), "api_key.create", "admin@acme.io", office, key_id=f"ak_{gen.h('ak', k)[:3]}", scope="read"),
                gen.app(iso(t + timedelta(minutes=13)), "api_key.revoke", "admin@acme.io", office, key_id=f"ak_{gen.h('ak', k - 1)[:3]}")]
    for k, d in enumerate(every(start, end, 35, "grace", "publish")):
        t, repo, tok = d + timedelta(hours=18) + jit(180), f"example-{k + 1}", gen.token("grace")
        out += [gen.gh(iso(t), "repo.create", "grace", repo=repo, visibility="private", **tok),
                gen.gh(iso(t + timedelta(minutes=5)), "git.push", "grace", repo=repo, ref="refs/heads/main", files=["README.md"],
                       message="docs: example, going public", **tok),
                gen.gh(iso(t + timedelta(minutes=15)), "repo.access", "grace", repo=repo, visibility="public", **tok)]
    return out


def main():
    """`stream.py` writes stream.ndjson and truth.json as they always were;
    `stream.py v3` adds gen.py's history cases and the history they need
    (`v3_noise`) and writes stream-v3.ndjson and truth-v3.json, whose cases
    also carry `rows` (line in the stream, ts, org, actor of each event) and
    `twin`."""
    v3 = sys.argv[1:] == ["v3"]
    os.makedirs(OUT, exist_ok=True)
    cases, truth = placed_cases(v3)
    new = len(gen.CASES) - gen.V3_FROM if v3 else 0
    events = noise()
    last = max(parse(e["ts"]) for e in cases[:sum(t["events"] for t in truth[:len(truth) - new])])
    end = END
    if last + timedelta(hours=12) > END:
        end = (last + timedelta(days=1)).replace(hour=0, minute=0, second=0)
        events += noise_between(END, end, seed=11)
    if v3:
        last = max(parse(e["ts"]) for e in cases)
        if last + timedelta(hours=12) > end:
            more = (last + timedelta(days=1)).replace(hour=0, minute=0, second=0)
            events += noise_between(end, more, seed=12)
            end = more
        events = v3_noise(events, START, end, seed=13)
    owner = {id(e): t["id"] for t, e in zip([t for t in truth for _ in range(t["events"])], cases)}
    events += cases
    events.sort(key=lambda e: e["ts"])
    if v3:
        by = {t["id"]: t for t in truth}
        for t in truth:
            t["rows"] = []
            if t["id"] in gen.TWINS:
                t["twin"] = gen.TWINS[t["id"]]
        for i, e in enumerate(events):
            if id(e) in owner:
                actor, org = norm_keys(e)
                by[owner[id(e)]]["rows"].append([i, e["ts"], org, actor])
    name = "-v3" if v3 else ""
    with open(os.path.join(OUT, f"stream{name}.ndjson"), "w") as f:
        for e in events:
            f.write(json.dumps(e, separators=(",", ":")) + "\n")
    with open(os.path.join(OUT, f"truth{name}.json"), "w") as f:
        json.dump(truth, f, indent=1)
    by_source = Counter(e["source"] for e in events)
    print(f"{len(events)} events from {events[0]['ts']} to {events[-1]['ts']}, {len(cases)} of them in {len(truth)} cases")
    print("  per source:", dict(by_source.most_common()))
    print(f"  cases land from {truth[0]['live_to']} to {truth[-1]['live_to']}")
    if v3:
        for c in gen.CASES[gen.V3_FROM:]:
            t = by[c["id"]]
            print(f"  {t['id']}: {t['events']} events, live {t['live_from']} to {t['live_to']}, actors {', '.join(t['actor_keys'])}")


if __name__ == "__main__":
    main()
