"""The grounding: Stratus Red Team's AWS techniques (Datadog, Apache-2.0),
pinned to one release, each with what it does, how its docs say to detect
it, and the CloudTrail records Datadog captured when it was detonated.

    python3 -I evals/stratus/fetch.py        # writes techniques.json and logs/<id>.json

The docs' records are anonymized by Stratus (accounts, regions, addresses
and names scrambled); they are labeled attack traffic to splice into a
replay, not anything from our accounts. Nothing here runs an attack.

One change to Datadog's records: the STS session tokens their AssumeRole and
GetFederationToken responses carry (long expired, published in the docs) are
replaced by REDACTED, so no credential is kept here (`redacted`). The aws
pack drops issued credentials before a record is stored, so the judge reads
the same records either way."""
import json
import pathlib
import re
import sys
import urllib.request

TAG = "v2.37.0"  # 2026-09-30; pinned, at least two weeks old
REPO = "DataDog/stratus-red-team"
HERE = pathlib.Path(__file__).resolve().parent


def get(url):
    with urllib.request.urlopen(url, timeout=30) as r:
        return r.read().decode("utf-8")


def redacted(v):
    """`v` with the value of every `sessionToken` replaced by REDACTED."""
    if isinstance(v, dict):
        return {k: "REDACTED" if k == "sessionToken" and isinstance(x, str) else redacted(x) for k, x in v.items()}
    if isinstance(v, list):
        return [redacted(x) for x in v]
    return v


def section(doc, name):
    m = re.search(rf"^## {re.escape(name)}\b.*?$(.*?)(?=^## |\Z)", doc, re.M | re.S)
    return m.group(1).strip() if m else ""


def bullets(text):
    return [re.sub(r"<[^>]+>", "", b).strip() for b in re.findall(r"^\s*- (.+)$", text, re.M)]


def plain(text):
    text = re.sub(r"<code>(.*?)</code>", r"`\1`", text)
    text = re.sub(r"<[^>]+>", "", text)
    return re.sub(r"\n{3,}", "\n\n", text).strip()


def parse(tid, doc):
    title = re.search(r"^title:\s*(.+)$", doc, re.M).group(1).strip()
    mappings = section(doc, "Mappings")
    tactics = []
    m = re.search(r"- MITRE ATT&CK\n((?:\s{4,}- .+\n?)+)", mappings)
    if m:
        tactics = [t.strip()[2:] for t in m.group(1).strip().splitlines()]
    catalog = re.findall(r"\[([^\]]+)\]\([^)]*threat-technique-catalog[^)]*\)\s*\((T[\d.]+)\)", mappings)
    desc = section(doc, "Description")
    parts = re.split(r"<span[^>]*>(Warm-up|Detonation|Revert)</span>:", desc)
    summary = plain(parts[0])
    steps = {parts[i].lower(): bullets(parts[i + 1]) for i in range(1, len(parts) - 1, 2)}
    logs_md = section(doc, "Detonation logs")
    events = re.findall(r"^- `([a-z0-9-]+:[A-Za-z0-9]+)`", logs_md, re.M)
    raw = []
    fence = re.search(r"```json[^\n]*\n(.*?)\n\s*```", logs_md, re.S)
    if fence:
        body = "\n".join(l[4:] if l.startswith("    ") else l.lstrip("\t") for l in fence.group(1).splitlines())
        try:
            raw = json.loads(body)
        except json.JSONDecodeError as e:
            print(f"  {tid}: raw logs did not parse ({e})", file=sys.stderr)
    return {
        "id": tid,
        "title": title,
        "tactics": tactics,
        "aws_catalog": [{"name": n, "id": i} for n, i in catalog],
        "summary": summary,
        "warm_up": steps.get("warm-up", []),
        "detonation": steps.get("detonation", []),
        "detection": plain(section(doc, "Detection")),
        "events": events,
    }, raw


def main():
    listing = json.loads(get(f"https://api.github.com/repos/{REPO}/contents/docs/attack-techniques/AWS?ref={TAG}"))
    ids = sorted(f["name"][:-3] for f in listing if f["name"].startswith("aws.") and f["name"].endswith(".md"))
    (HERE / "logs").mkdir(exist_ok=True)
    out = []
    for tid in ids:
        doc = get(f"https://raw.githubusercontent.com/{REPO}/{TAG}/docs/attack-techniques/AWS/{tid}.md")
        t, raw = parse(tid, doc)
        t["logged_events"] = len(raw)
        raw = redacted(raw)
        json.dump(raw, open(HERE / "logs" / f"{tid}.json", "w"), indent=1)
        out.append(t)
        print(f"{tid}: {len(t['events'])} kinds, {len(raw)} records", file=sys.stderr)
    json.dump({"source": f"https://github.com/{REPO}/tree/{TAG}", "license": "Apache-2.0", "techniques": out},
              open(HERE / "techniques.json", "w"), indent=1)
    print(f"{len(out)} techniques", file=sys.stderr)


if __name__ == "__main__":
    main()
