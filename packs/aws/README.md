# aws

CloudTrail into finished records, by one plugin, `aws`. It takes the records
whose `source` is `cloudtrail`: what tangent's `cloudtrail` source reads from
a trail's files (every event, data events included, every account and region
of an organization trail), or the same records sent as NDJSON.

## What it writes

`_norm` ([the record](../README.md#the-record) has the contract):

| field | from |
|---|---|
| `actor` | an assumed role's role (`sessionContext.sessionIssuer.userName`), an SSO session's person (the session name of an `AWSReservedSSO_` role), `account:<id>` for another account, the root user's name or `root`, else `userIdentity.userName` or `arn`; `aws:<service>` when AWS acted on its own, named only from fields AWS writes (`invokedBy`, a service-linked role's path, an `.amazonaws.com` source, `eventSource`) |
| `org` | `userIdentity.accountId`; your org when `cloudtrail` is in the profile's `own_sources` |
| `kind` | `eventName` |
| `ip` | `sourceIPAddress`, unless an AWS service, `AWS Internal`, or the SAML or web identity broker made the call |
| `target` | `requestParameters.bucketName`, else `roleArn`, `userName` or `groupId` |

`_facts`, beside what the profile and the tables say:

| key | when | note |
|---|---|---|
| `principal: aws` | AWS acted on its own | |
| `root` | `userIdentity.type` is `Root` | AWS root credentials used |
| `world_open` | the call's parameters hold a CIDR with a `/0` prefix, every address (`0.0.0.0/0`, `::/0`, or any address before the `/0`) | opened to every address (`<cidr>`) |

Your config routes each record to its sink and to
[behavior-baseline](../behavior-baseline), which folds it into its actor's
baseline: the APIs and resources it touched, its clients and addresses,
sign-ins, credential changes, and who assumed each role session. That is what
the judge's `_baseline` says of the actor.

## What it drops

Kept: `ts`, `source`, `eventTime`, `eventSource`, `eventName`, `awsRegion`,
`recipientAccountId`, `sourceIPAddress`, `userAgent`, `errorCode`,
`errorMessage`, `readOnly`, `resources`; `userIdentity`'s `type`, `userName`,
`arn`, `accountId` and `invokedBy`, with the session's `role` and
`mfaAuthenticated`; `additionalEventData`'s `MFAUsed`, `LoginTo`,
`SamlProviderArn` and `CredentialType`.

- `requestParameters` past 1500 bytes keeps its top-level values and the names
  of the rest.
- `responseElements` only for a call that grants (`ConsoleLogin`,
  `AssumeRole*`, `Create*`, `GetSessionToken`, `GetFederationToken`) or one
  under 300 bytes, cut the same way past 1000; issued credentials never.
- At any depth: pagination, client and idempotency tokens, request and upload
  ids, session tokens and secret keys, signatures, hashes and ETags, image
  manifests and digests, schema versions.
- Everything else: `eventID`, `requestID`, `eventVersion`, `eventType`,
  `tlsDetails`, `sessionCredentialFromConsole`, `managementEvent`, ...

## What it enriches

`sourceIPAddress`, as `_norm.ip`: its country, network and owner, hosting,
Tor and VPN, and whether it is one of yours (`_facts.ip`). Next, through
`Norm::address`:

- the ranges a security group or network ACL rule opens
  (`requestParameters.ipPermissions.items[].ipRanges.items[].cidrIp`,
  `cidrIpv6`, `cidrBlock`): a `/32` is an address as it is; a wider range
  wants its first address, or a lookup that takes ranges;
- the addresses a WAF IP set is given (`requestParameters.addresses`).

## Use it

In your config, beside [judge](../judge), which brings enrich-ip and
behavior-baseline:

```yaml
include:
  - ../tangent/packs/judge/ingest.yaml
  - ../tangent/packs/aws/ingest.yaml

enrichment_tables:                  # your company: packs/README.md#the-profile
  profile: { type: document, path: profile/profile.yaml, required: true }

sinks:
  lake:                          # every finished record, kept: Parquet files, a directory an hour; delete old hours to trim it
    type: rolling_file
    path: lake/logs/%Y/%m/%d/%H
    wal_path: lake/wal
    max_file_age_seconds: 3600
    compression: { type: zstd, level: 3 }
    encoding:
      type: parquet
      record_column: record      # each record whole, for the fields the schema leaves out
      schema:
        ts: timestamp
        source: string
        _norm: { org: string, actor: string, kind: string, ip: string }

sources:
  cloudtrail:
    type: cloudtrail
    region: ${AWS_REGION}                   # the queue's; the SDK's default chain when unset
    profile: ${AWS_PROFILE}
    queue_url: ${CLOUDTRAIL_QUEUE_URL}      # e.g. https://sqs.us-east-1.amazonaws.com/111122223333/trail-files
    bucket_role: ${CLOUDTRAIL_BUCKET_ROLE}  # an organization trail's log archive role; unset otherwise

dag:
  - from: cloudtrail
    to: [aws]
  - from: aws   # kept, learned, judged
    to: [lake, baseline_builder, baseline_renderer]
```

and `cloudtrail` in your profile's `own_sources`, so every account is your
org. [Judge's ingest.yaml](../judge/ingest.yaml) has the row saying how the
judge writes CloudTrail records as logfmt.

### Credentials

The source reads the trail through an SQS queue told of each file the trail
writes, with the AWS credential chain (`AWS_PROFILE`, or the SDK's default).
Left without `queue_url`, it says so once and stays idle. A message is
deleted once its file's records are kept; one whose file cannot be read stays
on the queue and comes back in five minutes, so give the queue a redrive
policy to set aside what never succeeds. When the queue cannot be read (an
expired session, a permission), the source warns once and retries, at most a
minute apart.

The source's identity needs `sqs:ReceiveMessage` and `sqs:DeleteMessage` on
the queue, and `sts:AssumeRole` on the bucket role. The bucket role, or the
identity itself when the bucket is its own account's, needs `s3:GetObject` on
the trail's prefix, and `kms:Decrypt` if the trail encrypts its files.

### An organization trail

One trail can record every account and region in the organization into one
bucket. Either of these notifications works:

- the trail's own SNS notifications, with the queue subscribed to the topic;
- S3 event notifications on the bucket, straight to the queue or through SNS.

With Control Tower, the trail already notifies the audit account's
`aws-controltower-AllConfigNotifications` topic, and its bucket is the log
archive account's. Subscribe a queue in the audit account to the topic, and
set `CLOUDTRAIL_BUCKET_ROLE` to a role in the log archive account that can
read the bucket. AWS Config's messages share the topic and are skipped, as
are digests and the bucket's other files.

Reading the trail makes few calls of its own, but some land back in it: the
bucket role's `AssumeRole`, about once an hour; a KMS `Decrypt` for each file
when the trail encrypts its files with KMS (fewer with an S3 Bucket Key); and,
when the trail logs S3 data events for its own bucket, a `GetObject` for each
file, each one an event in a later file. Leave the trail's bucket out of its
data-event selectors. With a bucket role, these calls carry the session name
`tangent`.

### Replay

`tangent replay --config <file> --from 1d` reads the config's `cloudtrail`
source's past from CloudTrail's Event history through LookupEvents, so it
needs no trail, queue or bucket. It uses the source's `region` and `profile`
credentials and reads the account they belong to, in every commercial region.
An opt-in region the account hasn't enabled is skipped, and so, with a
warning, is an opt-in region whose endpoint can't be reached. An organization
trail's other accounts are not included. The identity needs
`cloudtrail:LookupEvents`. Event history keeps 90 days of management events
only: a replay gets no data events, and a `--from` more than 90 days back is
refused. LookupEvents allows two calls a second per region and returns 50
events a call, so an hour with 5,000 events in one region takes about 50
seconds.

Each LookupEvents call is a management event too, so the range ends when the
replay starts. Its own calls that land in the range anyway (as they do when
the machine's clock is ahead of AWS's) are left out, and so are earlier
replays' calls from the same machine. Each replay's calls carry
`tangent-replay-` and a random 64-bit id of its own in their `userAgent`, but
the tag alone is not trusted: anyone can set a user agent, and earlier
replays' tags are in Event history for anyone who can read it. A record is
left out only when it is a LookupEvents call with a replay's tag, signed with
an access key that replay's calls were signed with, at a time it ran (a
minute either side, for the clocks). Each replay records its id, its access
keys and when it started and finished in a ledger of the machine's replays,
`$XDG_STATE_HOME/tangent/replays.jsonl` (else
`~/.local/state/tangent/replays.jsonl`). An access key ID can't sign a call
without its secret, so only a caller holding that replay's credentials, or
one who can write the ledger, could have a call left out: the ledger is
trusted as your config is. A replay finishes when its range is read, or
when it fails or is stopped with Ctrl-C. One killed before then (a closed terminal, SIGKILL, a crash) counts
as running until its temporary credentials expired, but for 36 hours at most
(the longest an STS session lasts), or for a day with an IAM user's keys.
Another machine's replays are in that machine's ledger, so their calls stay
in. If the ledger can't be read, earlier replays' calls stay in; if it can't
be written, later replays will read this one's. Either is warned of once,
and the replay goes on.

### Worth watching

Kinds to list under the profile's `watch`: `StopLogging`, `DeleteTrail`,
`PutBucketPolicy`, `PutBucketPublicAccessBlock`,
`AuthorizeSecurityGroupIngress`, `ModifyDBSnapshotAttribute`, `CreateUser`,
`CreateAccessKey`, `AttachUserPolicy`.

## Test it

[tests/input.json](tests/input.json) holds CloudTrail records from the cases,
one carrying the keys only the pipeline writes (which must not come through)
and one opening a security group to the world. [tests/expected.json](tests/expected.json)
is what the plugin makes of them against the tables in `tests/`, which
`ingest.yaml` names for the test in any config: [profile.yaml](tests/profile.yaml),
the company; [addresses.yaml](tests/addresses.yaml), GeoLite2's answers for
their addresses; [networks.csv](tests/networks.csv), those addresses' labels.

```bash
make -C packs test-aws          # compile, then the fixtures
make -C packs fixtures-aws      # expected.json again, after a change; read the diff
```

Cases in [tangent-eval's cases](https://github.com/EthanBlackburn/tangent-eval/tree/main/cases) with CloudTrail
records, 20: `cloud/*` (root console login, CloudTrail stopped, a bucket made
public, a security group opened to the world, a role assumed by a new
principal, a key created with `*`, a password change and a new key whose
first calls come from a new host), `data-exfil/snapshot-shared-external`,
`agents/agent-escapes-task-uses-found-credentials`, three of
`supply-chain/`, two of `identity/` (`mfa-reset-then-new-factor-new-asn`,
`okta-new-country-then-aws`), four of `benign/`
(`mfa-reset-then-new-factor-office`, `okta-usual-country-then-aws`,
`password-change-new-key-usual-ip`, `secret-rotation-after-vendor-notice`),
`prompt-injection/cli-user-agent-answers` and
`vendor/platform-breach-env-secrets-reused`.
