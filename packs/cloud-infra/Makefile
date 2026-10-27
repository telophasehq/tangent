TANGENT ?= tangent

# The judge: any OpenAI-compatible endpoint. These defaults are a local model
# (mlx_lm.server or llama-server on :8080); for a hosted one, for example
#   JUDGE_URL=https://api.openai.com/v1 JUDGE_MODEL=gpt-5.1 JUDGE_API_KEY=$OPENAI_API_KEY make run
JUDGE_URL ?= http://localhost:8080/v1
JUDGE_MODEL ?= mlx-community/GLM-4.7-Flash-4bit
JUDGE_API_KEY ?= none
export JUDGE_URL JUDGE_MODEL JUDGE_API_KEY

# The sources. Each reads its API directly and wakes up when its credential is
# set; left empty, it says so once and stays idle. The README says what each
# credential needs.
AWS_REGION ?=                   # CloudTrail: the SDK's default chain when unset
AWS_PROFILE ?=
CLOUDTRAIL_QUEUE_URL ?=         # set: a trail's delivery through its SQS notifications instead of LookupEvents
GITHUB_ORG ?=                   # the organization's login
GITHUB_TOKEN ?=                 # a token with read:audit_log
OKTA_ORG_URL ?=                 # https://acme.okta.com
OKTA_API_TOKEN ?=
GOOGLE_APPLICATION_CREDENTIALS ?=   # a service account key file with domain-wide delegation
GOOGLE_ADMIN_EMAIL ?=           # the admin the service account acts as
export AWS_REGION AWS_PROFILE CLOUDTRAIL_QUEUE_URL GITHUB_ORG GITHUB_TOKEN OKTA_ORG_URL OKTA_API_TOKEN GOOGLE_APPLICATION_CREDENTIALS GOOGLE_ADMIN_EMAIL

setup:
	./setup.sh

compile: setup
	$(TANGENT) plugin compile --config tangent.yaml

test: compile
	$(TANGENT) plugin test --config tangent.yaml

run: compile
	$(TANGENT) run --config tangent.yaml

.PHONY: setup compile test run
