# okta

Okta's System Log into finished records, by one plugin, `okta`. It takes the
records whose `source` is `okta`: what tangent's `okta` source reads from the
System Log API, or the same events sent as NDJSON. The smallest pack: the one
to copy for a new provider.

## What it writes

`_norm` ([enrich](../enrich#the-record) has the contract):

| field | from |
|---|---|
| `actor` | `actor.alternateId` |
| `org` | none of its own: your org when `okta` is in the profile's `own_sources` |
| `kind` | `eventType` |
| `ip` | `client.ipAddress` |
| `country` | `client.geographicalContext.country` |
| `target` | the first target's `displayName` |

`_facts` is what the profile and the tables say; Okta adds no facts of its
own.

Your config routes each record to its sink and to
[behavior-baseline](../behavior-baseline), which folds it into its actor's
baseline: event types, applications, browsers and devices, addresses, sign-ins,
failed sign-ins, MFA and password changes and grants, Okta's positive behaviors
on a sign-in. That is what the judge's `_baseline` says of the actor.

## What it drops

Kept: `ts`, `source`, `published`, `eventType`, `displayMessage`, `severity`,
`outcome`, `device` (the registered device, whose `id` behavior-baseline keeps as
the actor's devices); the actor's `type`, `alternateId` and `displayName`; the
client's `ipAddress`, `zone`, `device`, the user agent's `os` and `browser`,
and the city, state and country; `securityContext`'s `asNumber`, `asOrg`,
`isp` and `isProxy`; each target's `type`, `alternateId` and `displayName`;
`authenticationContext`'s provider and credential type; and Okta's own risk
signals in `debugContext.debugData` (`threatSuspected`, `risk`, `behaviors`,
`logOnlySecurityData`, `factor`), as `debugData`. Nulls go, and a `zone` of
`"null"`.

Dropped: `uuid`, `version`, `legacyEventType`, `transaction`, `request` (its
`ipChain`), the rest of `debugContext`, ids, the raw user agent, the
coordinates and postal code.

## What it enriches

`client.ipAddress`, as `_norm.ip`: its country, network and owner, hosting,
Tor and VPN, and whether it is one of yours (`_facts.ip`). Next, through
`Norm::address`: the hops in `request.ipChain[].ip`, the proxies a request
came through, read before the trim drops them.

## Use it

In your config, beside [judge](../judge), which brings enrich and
behavior-baseline:

```yaml
include:
  - ../tangent/packs/judge/ingest.yaml
  - ../tangent/packs/okta/ingest.yaml

enrichment_tables:                  # your company: packs/enrich/README.md#the-profile
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
  okta_audit:
    type: okta
    org_url: ${OKTA_ORG_URL}          # https://example.okta.com
    token: ${OKTA_API_TOKEN}

dag:
  - from: okta_audit
    to: [okta]
  - from: okta   # kept, learned, judged
    to: [lake, baseline_builder, baseline_renderer]
```

and `okta` in your profile's `own_sources`, so its records are your org's.

`OKTA_API_TOKEN` comes from Security → API → Tokens, made by a read-only
administrator. The source keeps its place in the runtime's state store, so a
restart resumes where it stopped; on first start it reads the last hour
(`since`). Left without a token, it says so once and stays idle.

Kinds worth the profile's `watch`: `user.mfa.factor.deactivate`,
`system.api_token.create`.

## Test it

[tests/input.json](tests/input.json): System Log events from the cases, one carrying the
keys only the pipeline writes. [tests/expected.json](tests/expected.json) is
what the plugin makes of them against the tables in `tests/` (the company,
GeoLite2's answers for their addresses, their labels), which `ingest.yaml`
names for the test in any config.

```bash
make -C packs test-okta
make -C packs fixtures-okta       # after a change; read the diff
```

Cases in [tangent-eval's cases](https://github.com/EthanBlackburn/tangent-eval/tree/main/cases) with Okta
records, 14: seven of `identity/` (`admin-login-new-asn`, `impossible-travel`,
`mfa-disabled`, `mfa-reset-then-new-factor-new-asn`,
`okta-failed-burst-then-success`, `okta-new-country-then-aws`,
`okta-new-country-then-github`), five of `benign/`
(`mfa-reset-then-new-factor-office`, `okta-typo-then-success`,
`okta-usual-country-then-aws`, `okta-usual-country-then-github`,
`oncall-3am-login`), `prompt-injection/okta-token-name-vouches` and
`vendor/ai-app-oauth-token-stolen-sso-pivot`.
