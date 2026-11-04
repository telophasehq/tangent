# Grounding: what we should catch in AWS

[Stratus Red Team](https://github.com/DataDog/stratus-red-team) (Datadog, Apache-2.0) emulates 45 AWS attack techniques. Its docs give each one's steps, how to detect it, and, for 35 of them, the CloudTrail records Datadog captured when it ran. That makes it a list of things to catch and labeled attack traffic in one, without running anything in our accounts. [NOTICE](NOTICE) and [LICENSE](LICENSE) are Stratus Red Team's; NOTICE also says what here is theirs and what changed.

- `fetch.py` pulls the catalog at a pinned release (v2.37.0) into `techniques.json`, and each technique's recorded records into `logs/<id>.json`, with the session tokens Datadog's responses carry replaced by `REDACTED`. Stratus scrambled their accounts, regions, addresses and names.
- `grounding.yaml` says, per technique, what a judge needs to tell it from ordinary work, and which of our cases exercise the same calls.
- `splice.py` makes the cases, into `evals/runs/stratus/cases.json` (`stream.py` runs it when the file is missing): each of the 35 recorded techniques twice, as a member of acme whose credentials were taken, `away` (from a hosting address) and `usual` (from the office), 70 attack cases; and a control for each, the same records with every call made a harmless read in the same service, 70 must-not-flag cases. Each technique runs at an hour of its own in the members' working day, the same for its four cases. The roles the techniques' warm-up created are acme's own (`WARM_UP`: `ec2-support`, `org-admin`, `web-instance`, `lambda-exec`, `fleet-anywhere`), in the profile's `services` and at work on `stream.py`'s ordinary days; the role a detonation creates keeps a new name. `stream.py` places the cases.

The output is the same on every run: the plain names `splice.py` gives resources are numbered longest first, then in name order. The runs recorded before November 2026 replayed an earlier output: equal-length names were numbered in the order of a Python set, which follows the process's hash seed, so two cases (`secretsmanager-retrieve-secrets` away and usual) number 16 secret names differently; and its controls had the two faults fixed below.

| decides | techniques | covered by our cases | with recorded logs |
|---|---|---|---|
| **event**: one record and its parameters | 23 | 6 | 21 |
| **actor**: routine for someone; who, from where, and whether they usually do | 13 | 0 | 10 |
| **volume**: many calls in minutes | 6 | 1 | 4 |
| **data**: needs S3 data events | 3 | 0 | 0 |

What it says about the pipeline:

- **Event techniques test knowledge.** Stopping a trail, sharing a snapshot with another account, a trust policy that admits a stranger: the record alone decides, so these measure whether a model knows AWS, not how much history it gets. The CloudTrail plugin keeps every defining request parameter in the 35 recorded techniques (it drops response fields, mostly issued credentials, on eleven, and the certificate source of a Roles Anywhere trust anchor).
- **Actor techniques need a longer memory than a day.** SSM sessions, Lambda deploys, a second access key, an unusual instance type are someone's normal work; whether they are this principal's is in weeks of history, not the last 24 hours. None is in our corpus.
- **Volume techniques need counts.** Twenty `GetSecretValue` calls in four minutes is the signal; the history shows an actor's last 20 records and a total, so a burst that spans buffers is easy to undercount.
- **Data techniques need the trail to log S3 data events.** Ransomware by deletion or re-encryption is invisible to `LookupEvents` and to a trail that logs management events only, which is the default.

Stratus covers AWS, Azure, GCP, Entra ID and Kubernetes; it says nothing about Okta, GitHub or Google Workspace, which still rely on our own cases.

## Not citable yet

An audit on 2026-10-28 found that the splice could be told from acme's own traffic by things that have nothing to do with the attack, and every number measured on these cases so far is withdrawn until the runs are repeated. All six tells are fixed; no run has been scored since.

Fixed in November 2026:

- every control was on a different member from its attack: the member and address came from the case's position in the output, and 70 attacks is not a multiple of nine members. They now come from the technique and the variant, so attack and control share both.
- the controls carried `requestParameters` on none of their 620 records, the attacks on 600. Each read now carries the request CloudTrail records for it, shaped as the service writes it and naming what the attack named (the same instances, secret, trail, function or role). Null remains only where the service records null (`GetCallerIdentity`, `GetAccountSummary`, `ListFoundationModels`, `DescribeOrganization`): 56 control records to the attacks' 20.

- the IAM user name in Datadog's recording survived in 20 attack records across 6 cases, where it stood before a space or a hyphen rather than a quote or a slash: the `is not authorized` message of a denied call, and SSM session ids (`christophe-bkqs75...`). The name is now replaced wherever it stands on its own, and the splice asserts it is gone.
- all 140 spliced cases started at 13:00Z, an hour in which members' own IAM-user records never appear (their AWS work runs 16:00Z to 23:00Z). Each technique now begins at an hour of its own between 16:00Z and 21:00Z, the same for its attack and control from either address.
- all four spliced console sign-ins carried the CLI's user agent, since every user agent that named Stratus became the CLI's. A console sign-in now carries the browser's, as the members' own sign-ins do.
- none of the roles the techniques' warm-up created was in the profile: each case named them afresh (`acme-role-cbb41`), so every role session was a principal nobody knew. They are now acme's own roles (`WARM_UP` in `splice.py`), in the profile's `services` and `about`, and at work on the ordinary days of both streams: the web tier's instance checks in with SSM from the VPC, the nightly org inventory lists the accounts, and members look at instances as `ec2-support` (in v3, after assuming it from their own IAM user). A role the detonation itself creates keeps a new name, as an attacker's would.

In every run before the fixes, the controls were flagged about as often as the attacks (from away, all 35; from the office, 18 to 29 of 35), so on Stratus the corpus measured what gets escalated, not whether the judge tells an attack from its harmless twin. No run has been scored on the fixed cases yet. Stratus has been public since 2022 and documents its detections, so a model may also know these techniques.

Not yet looked at: the recorded `roleSessionName` (`aws-go-sdk-<nanoseconds>`, the Go SDK's default) and the recorded access key and principal ids on IAM-user records, which no pack reads.

`recipientAccountId` is not one of the tells. The spliced records carry it, and so did every ordinary CloudTrail record of the members they are spliced onto (their own AWS work, 7,675 records in the v3 stream), before and after the generator began writing it on every CloudTrail record, as the cases have carried it since #48. The records that gained it belong to other principals (the deploy, read-only and backup service users, and the IAM users `dave` and `admin`) and to our cases.
