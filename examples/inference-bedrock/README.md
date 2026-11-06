# inference-bedrock

Claude on Amazon Bedrock reads every record; a plugin keeps what it flagged.

Same pipeline as [../inference-local](../inference-local), with the `bedrock`
backend: the `judge` node calls the Converse API with the AWS credential chain
(environment, profile, SSO, instance or task role), so there is no key to
configure. The filter is the same [flagged](../inference-local/flagged/src/lib.rs)
plugin, referenced by path.

```yaml
inference:
  judge:
    backend: bedrock
    model: us.anthropic.claude-haiku-4-5-20251001-v1:0
    aws: { region: us-west-2 }
    prompt_file: judge.md
    batch_bytes: 4000
    max_tokens: 300
    concurrency: 8
    timeout_ms: 60000
plugins:
  flagged:
    module_type: rust
    path: ../inference-local/flagged
```

## Credentials

Anything the AWS SDK accepts. On a laptop, `aws sso login --profile <p>` and
either `aws: { profile: <p> }` in the config or `AWS_PROFILE=<p>` in the environment; in
ECS or EC2, the task or instance role. Two things that bite:

- `AWS_BEARER_TOKEN_BEDROCK` in the environment takes precedence over SigV4 for
  Bedrock. A stale one fails every request with a generic authentication
  error; unset it unless you mean it.
- A stale SSO *refresh* token fails inside the SDK (`InvalidGrantException`)
  even while the CLI still works on its cached access token. `aws sso login`
  again fixes both.

A request that fails is retried twice, then each of its records goes on with
`{"error": ...}` as `judge` and tangent logs `inference failed`; the plugin
keeps those, so a credential problem shows up in `flagged.ndjson` as every
record. When the request cannot be signed or sent (no credentials, no route
to Bedrock) the error is the same generic `HttpError: Http client error: An
unexpected error occurred. Verify Internet connection or AWS keys`, whichever
it was.

## Run

In this directory:

```bash
tangent plugin compile --config tangent.yaml    # builds ../inference-local/flagged into plugins/
tangent run --config tangent.yaml
```

In another shell:

```bash
nc -w 1 localhost 9000 < events.ndjson
cat flagged.ndjson
```

The events and the prompt are inference-local's; what to expect is in
[its README](../inference-local/README.md#run). The file sink appends:
`rm flagged.ndjson` before running again.
