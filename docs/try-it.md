# Try it

Your AWS account's last day of CloudTrail, judged by Claude Haiku 5.5 on Bedrock.

1. Install tangent, and gh, which checks the plugins tangent fetches (no login needed):

```bash
curl --proto '=https' --tlsv1.2 -LsSf https://github.com/EthanBlackburn/tangent/releases/latest/download/tangent-cli-installer.sh | sh
brew install gh    # or see https://cli.github.com
```

2. Write the config:

```bash
tangent init
```

It uses `$AWS_PROFILE`, or your default AWS profile when that is unset. To use another, open tangent.yaml in any editor and set it on the line marked EDIT.

3. Replay the last day:

```bash
tangent replay --config tangent.yaml --from 1d
```

The judge's Bedrock calls are billed to your account: replay prints about how many records the range holds before it judges any, and Ctrl-C stops it. The README's [Volume and cost](../README.md#volume-and-cost) prices a day.

What it flagged is in alerts.ndjson, every verdict in verdicts.ndjson. The first run fetches the packs' plugins for this version of tangent. It warns that four enrichment files are missing and runs without them; tangent.yaml says where they come from. Your AWS identity needs cloudtrail:LookupEvents and Bedrock access to Claude Haiku 5.5 in us-west-2.
