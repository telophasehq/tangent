"""Stratus Red Team's recorded detonations as cases for the replay.

    python3 -I evals/stratus/splice.py       # writes evals/stratus/cases.json; then python3 -I evals/stream.py v3

Each technique with recorded CloudTrail records becomes two cases, run by a
member of acme whose credentials were taken:

- `away`: from a hosting address acme has never used;
- `usual`: from the office, where the member always works.

The first measures what the pipeline catches with everything it knows; the
second, what the records alone give away, which is the technique.

The records are made to look like acme's: the technique's account becomes
acme's (another account stays another account), the scrambled regions become
real ones, the attacking IAM user becomes the member (IAM users here are
named by email, so their AWS history and their Okta and app history are
one actor's), and nothing says Stratus or Grimoire (Datadog's capture tool): their user agent becomes the AWS CLI's,
and every resource it named (`stratus-red-team-malicious-iam-role`) gets a
plain name by kind (`acme-role-1`)."""
import json
import pathlib
import re
import collections
import hashlib
from collections import Counter
from datetime import datetime, timedelta, timezone

HERE = pathlib.Path(__file__).resolve().parent
ACCOUNT = "111122223333"
REGION = "us-west-2"
GLOBAL = {"iam.amazonaws.com", "signin.amazonaws.com", "organizations.amazonaws.com"}  # recorded in us-east-1
MEMBERS = ["alice@acme.io", "bob@acme.io", "carol@acme.io", "cfo@acme.io", "dave@acme.io",
           "erin@acme.io", "grace@acme.io", "oncall-frank@acme.io", "admin@acme.io"]
AWAY = ["159.89.214.31", "5.161.92.17", "45.77.65.211", "51.89.160.12", "139.162.130.8"]  # DigitalOcean, Hetzner, Vultr, OVH, Linode
USUAL = "73.162.14.9"  # the office
CLI = "aws-cli/2.17.0"
BASE = datetime(2026, 10, 20, 13, 0, tzinfo=timezone.utc)  # stream.py's LIVE: placement shifts from here
REAL_REGION = re.compile(r"^[a-z]{2}(-gov)?-[a-z]+-\d$")
KINDS = [("anchor", "anchor"), ("profile", "profile"), ("role", "role"), ("user", "user"), ("bucket", "bucket"),
         ("trail", "trail"), ("layer", "layer"), ("func", "fn"), ("lambda", "fn"), ("snapshot", "snap"),
         ("ami", "image"), ("image", "image"), ("security-group", "sg"), ("sg", "sg"), ("keypair", "key"),
         ("key", "key"), ("flow", "flowlog"), ("vpc", "vpc"), ("secret", "secret"), ("param", "param"),
         ("log", "logs"), ("rds", "db"), ("db", "db"), ("instance", "instance")]


def iso(t):
    return t.strftime("%Y-%m-%dT%H:%M:%SZ")


def neutral_names(text, tag):
    """Every name Stratus gave a resource, a plain one of the same kind, unique
    to the case (`tag`): a role one case sets up is not the next case's, or
    its day of history would hold the other case."""
    names, counts = {}, Counter()
    for tok in sorted(set(re.findall(r"stratus-red-team[-_][A-Za-z0-9._-]*[A-Za-z0-9]", text)), key=len, reverse=True):
        rest = tok[len("stratus-red-team") + 1:].lower()
        if re.fullmatch(r"[0-9a-f]{8}(-[0-9a-f]{4}){3}-[0-9a-f]{12}", rest):
            kind = "session"
        else:
            kind = next((k for w, k in KINDS if w in rest), "res")
        counts[kind] += 1
        names[tok] = f"acme-{kind}-{tag}{counts[kind]}"
    return names


def member_identity(member, key):
    """The member's IAM user, as CloudTrail writes it."""
    return {"type": "IAMUser", "principalId": "AIDA" + key[4:], "arn": f"arn:aws:iam::{ACCOUNT}:user/{member}",
            "accountId": ACCOUNT, "accessKeyId": key, "userName": member}


def attacker_session(r):
    """A role session that is the attacker, not a role the technique set up:
    Stratus ran some techniques as an SSO session or a role of its own
    (`test-role`); the roles its warm-up made are all named stratus-red-team-*."""
    ui = r.get("userIdentity") or {}
    if ui.get("type") != "AssumedRole":
        return False
    issuer = ((ui.get("sessionContext") or {}).get("sessionIssuer") or {}).get("userName") or ui.get("arn") or ""
    return "stratus-red-team" not in issuer


def rewrite(records, member, ip, tag):
    records = [dict(r, userAgent=CLI) if re.search(r"stratus|grimoire", r.get("userAgent") or "", re.I) else r for r in records]
    key = "AKIA" + hashlib.sha1(f"{member}{tag}".encode()).hexdigest()[:16].upper()
    records = [dict(r, userIdentity=member_identity(member, key)) if attacker_session(r) else r for r in records]
    text = json.dumps(records)
    primary = Counter(r.get("recipientAccountId") for r in records).most_common(1)[0][0]
    attacker = Counter((r["userIdentity"].get("arn"), r["userIdentity"].get("userName"))
                       for r in records if r.get("userIdentity", {}).get("type") == "IAMUser").most_common(1)
    for scrambled in {r.get("awsRegion") for r in records if r.get("awsRegion") and not REAL_REGION.match(r["awsRegion"])}:
        text = text.replace(scrambled, REGION)
    text = text.replace(primary, ACCOUNT)
    if attacker:
        user = attacker[0][0][1]
        text = re.sub(rf'(?<=[":/]){re.escape(user)}(?=["/])', member, text)
    for old, new in neutral_names(text, tag).items():
        text = text.replace(old, new)
    # What is left: a path segment (`/credentials/stratus-red-team/...`) and
    # the tag its resources carry (`StratusRedTeam`).
    text = text.replace("stratus-red-team", "acme").replace("stratus_red_team", f"acme_ops_{tag}").replace("StratusRedTeam", "Project")
    out = json.loads(text)
    for r in out:
        src = r.get("sourceIPAddress") or ""
        if re.fullmatch(r"[\d.]+", src):
            r["sourceIPAddress"] = ip
        if r.get("eventSource") in GLOBAL:
            r["awsRegion"] = "us-east-1"
    assert not re.search(r"stratus|grimoire", json.dumps(out), re.I), "a Stratus or Grimoire marker survived"
    return out


# The splice control: each technique's records with every call made a
# harmless read in the same service, everything else as the attack had it
# (who, from where, when, how many, with what client). A judge that flags
# these from the office is reacting to the splice, not the attack.
READS = {
    "ssm.amazonaws.com": ["DescribeInstanceInformation", "DescribeParameters", "ListCommands"],
    "ec2.amazonaws.com": ["DescribeInstances", "DescribeSecurityGroups", "DescribeVolumes", "DescribeRegions"],
    "secretsmanager.amazonaws.com": ["DescribeSecret", "ListSecretVersionIds"],
    "sts.amazonaws.com": ["GetCallerIdentity"],
    "bedrock.amazonaws.com": ["ListFoundationModels", "GetFoundationModel"],
    "iam.amazonaws.com": ["GetAccountSummary", "ListRoles", "GetUser"],
    "cloudtrail.amazonaws.com": ["DescribeTrails", "GetTrailStatus"],
    "lambda.amazonaws.com": ["ListFunctions20150331", "GetFunction20150331v2"],
    "rolesanywhere.amazonaws.com": ["ListTrustAnchors"],
    "route53resolver.amazonaws.com": ["ListResolverQueryLogConfigs"],
    "organizations.amazonaws.com": ["DescribeOrganization"],
    "rds.amazonaws.com": ["DescribeDBInstances"],
}
# A call these services do not answer with a read becomes one EC2 answers.
ELSEWHERE = {"ec2-instance-connect.amazonaws.com": "ec2.amazonaws.com"}
CALL_FIELDS = ("requestParameters", "responseElements", "resources", "errorCode", "errorMessage",
               "additionalEventData", "serviceEventDetails", "sharedEventID", "apiVersion")


def benign(records, member):
    """`member`'s own identity stands in for one the attack created (the
    federated user GetFederationToken returns), which a read never would."""
    own = next((r["userIdentity"] for r in records if (r.get("userIdentity") or {}).get("userName") == member), None)
    out = []
    for k, r in enumerate(records):
        r = dict(r)
        if (r.get("userIdentity") or {}).get("type") == "FederatedUser" and own:
            r["userIdentity"] = own
        if r.get("eventSource") == "signin.amazonaws.com":
            # A sign-in with MFA, as every day.
            r.pop("errorCode", None)
            r.pop("errorMessage", None)
            r["additionalEventData"] = {**(r.get("additionalEventData") or {}), "MFAUsed": "Yes"}
            r["responseElements"] = {"ConsoleLogin": "Success"}
            out.append(r)
            continue
        source = ELSEWHERE.get(r.get("eventSource"), r.get("eventSource"))
        names = READS.get(source) or READS["sts.amazonaws.com"]
        if source not in READS:
            source = "sts.amazonaws.com"
        for f in CALL_FIELDS:
            r.pop(f, None)
        r.update(eventSource=source, eventName=names[k % len(names)], eventType="AwsApiCall", readOnly=True,
                 managementEvent=True, eventCategory="Management", requestParameters=None, responseElements=None)
        out.append(r)
    return out


def main():
    cat = {t["id"]: t for t in json.load(open(HERE / "techniques.json"))["techniques"]}
    recorded = [tid for tid in sorted(cat) if json.load(open(HERE / "logs" / f"{tid}.json"))]
    cases, slot = [], 0
    for kind in ("stratus", "control"):
        for tid in recorded:
            raw = sorted(json.load(open(HERE / "logs" / f"{tid}.json")), key=lambda r: r["eventTime"])
            t0 = datetime.strptime(raw[0]["eventTime"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
            for variant in ("away", "usual"):
                member = MEMBERS[slot % len(MEMBERS)]
                ip = AWAY[slot % len(AWAY)] if variant == "away" else USUAL
                tag = hashlib.sha1(f"{kind}/{variant}/{tid}".encode()).hexdigest()[:4]
                records = rewrite(raw, member, ip, tag)
                if kind == "control":
                    records = benign(records, member)
                events = []
                for r in records:
                    t = BASE + (datetime.strptime(r["eventTime"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc) - t0)
                    r["eventTime"] = iso(t)
                    events.append({"ts": iso(t), "source": "cloudtrail", **{k: v for k, v in r.items() if k not in ("ts", "source")}})
                attack = kind == "stratus"
                cases.append({
                    "id": f"{kind}/{variant}/{tid}", "title": cat[tid]["title"] if attack else f"Reads shaped like {cat[tid]['title']}",
                    "category": kind, "priority": "P2", "scope": "actor", "cadence": "minutes", "alone_innocent": not attack,
                    "sources": ["cloudtrail"],
                    "expected": {"flag": attack,
                                 "one_line": f"{member} ({variant}): " + (cat[tid]["summary"][:140] if attack else "harmless reads, spliced like the attack"),
                                 "check_first": cat[tid]["detection"][:200] if attack else "nothing"},
                    "events": events,
                })
                slot += 1
    json.dump(cases, open(HERE / "cases.json", "w"), indent=1)
    n = collections.Counter(c["category"] for c in cases)
    print(f"{n['stratus']} attack cases and {n['control']} controls from {len(recorded)} techniques, {sum(len(c['events']) for c in cases)} records")


if __name__ == "__main__":
    main()
