TANGENT ?= tangent

# The judge: any OpenAI-compatible endpoint. These defaults are a local model
# on :8080. Use llama-server: it turns the verdict schema into a grammar, so
# replies are always well-formed JSON and their reasons stay short; mlx_lm.server
# ignores the schema and samples greedily, and about one reply in five came back
# unreadable. With the GGUF build of the model:
#   llama-server -m GLM-4.7-Flash-Q4_K.gguf --port 8080 -np 1 -c 32768 -ngl 99 -fa on --jinja --temp 0.7 --top-p 0.95
# For a hosted one, for example
#   JUDGE_URL=https://api.openai.com/v1 JUDGE_MODEL=gpt-5.1 JUDGE_API_KEY=$OPENAI_API_KEY make run
JUDGE_URL ?= http://localhost:8080/v1
JUDGE_MODEL ?= glm-4.7-flash
JUDGE_API_KEY ?= none
export JUDGE_URL JUDGE_MODEL JUDGE_API_KEY

# The sources. Each reads its API directly and wakes up when its credential is
# set; left empty, it says so once and stays idle. The README says what each
# credential needs.
AWS_REGION ?=                   # CloudTrail: the SDK's default chain when unset
AWS_REGIONS ?=                  # several, comma-separated (us-east-1,us-west-2); include us-east-1, where IAM, STS and sign-ins are recorded
AWS_PROFILE ?=
CLOUDTRAIL_QUEUE_URL ?=         # set: a trail's delivery through its SQS notifications instead of LookupEvents
CLOUDTRAIL_BUCKET_ROLE ?=       # a role to read the trail's bucket as, when another account owns it (an organization trail's log archive)
GITHUB_ORG ?=                   # the organization's login
GITHUB_TOKEN ?=                 # a token with read:audit_log
OKTA_ORG_URL ?=                 # https://acme.okta.com
OKTA_API_TOKEN ?=
GOOGLE_APPLICATION_CREDENTIALS ?=   # a service account key file with domain-wide delegation
GOOGLE_ADMIN_EMAIL ?=           # the admin the service account acts as
export AWS_REGION AWS_REGIONS AWS_PROFILE CLOUDTRAIL_QUEUE_URL CLOUDTRAIL_BUCKET_ROLE GITHUB_ORG GITHUB_TOKEN OKTA_ORG_URL OKTA_API_TOKEN GOOGLE_APPLICATION_CREDENTIALS GOOGLE_ADMIN_EMAIL

setup:
	./setup.sh

compile: setup
	$(TANGENT) plugin compile --config tangent.yaml

test: compile
	$(TANGENT) plugin test --config tangent.yaml

run: compile
	$(TANGENT) run --config tangent.yaml

.PHONY: setup compile test run
