# inference-bedrock

Claude on Amazon Bedrock reads every record; only what it flags goes on.

Same pipeline as [../inference-local](../inference-local), with the `bedrock`
backend: the `inference` node calls the Converse API with the AWS
credential chain (environment, profile, SSO, instance or task role), so there is
no key to configure.

```yaml
inference:
  judge:
    backend: bedrock
    model: us.anthropic.claude-haiku-4-5-20251001-v1:0
    region: us-west-2
    prompt_file: judge.md
    output: _judge
    forward_if: flag
    concurrency: 8
    timeout_ms: 30000
```

## Credentials

Anything the AWS SDK accepts. On a laptop, `aws sso login --profile <p>` and
either `profile: <p>` in the config or `AWS_PROFILE=<p>` in the environment; in
ECS or EC2, the task or instance role. Two things that bite:

- `AWS_BEARER_TOKEN_BEDROCK` in the environment takes precedence over SigV4 for
  Bedrock. A stale one fails every request with a generic authentication
  error; unset it unless you mean it.
- A stale SSO *refresh* token fails inside the SDK (`InvalidGrantException`)
  even while the CLI still works on its cached access token. `aws sso login`
  again fixes both.

## Run

```bash
tangent run --config tangent.yaml
```

```bash
nc localhost 9000 < events.ndjson
cat flagged.ndjson
```

## Throughput

Measured with 300 synthetic events like [events.ndjson](events.ndjson), Haiku 4.5
through this exact node (one account, us-west-2, late 2026):

| concurrency | events/s | p50 | p95 | errors |
|---|---|---|---|---|
| 8 | 5.75 | 1.37 s | 1.79 s | 0 |
| 32 | 17.9 | 1.38 s | 3.97 s | 0 |
| 64 | 21.2 | 1.39 s | 3.23 s | 0 |

A verdict takes about 1.4 s whatever the load, so throughput is
`concurrency / 1.4 s` until an account quota binds: this account allows
10,000 requests and 5,000,000 tokens per minute for Haiku 4.5, and with the
whole record in the prompt (~1,000 tokens) tokens bind first, at roughly 80
events/s. Trim what the model sees with `fields:` to raise that. All 300 runs
flagged exactly the two intended event types and nothing else.

At 21 events/s Haiku costs about $0.03 per second, which is why a judge like this
sits behind a novelty filter rather than reading a raw stream.
