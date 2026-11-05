TANGENT ?= tangent

# The judge: any OpenAI-compatible endpoint, a local model on :8080 by default
# (packs/core/judge.yaml has the defaults). A hosted one:
#   JUDGE_URL=https://api.openai.com/v1 JUDGE_MODEL=gpt-5.1 JUDGE_API_KEY=$OPENAI_API_KEY make run

# The sources. Each reads its API directly and wakes up when its credential is
# set; left empty, it says so once and stays idle. The README says what each
# credential needs.
AWS_REGION ?=                   # CloudTrail: the queue's region; the SDK's default chain when unset
AWS_PROFILE ?=
CLOUDTRAIL_QUEUE_URL ?=         # the SQS queue a trail's delivery notifications land in
CLOUDTRAIL_BUCKET_ROLE ?=       # a role to read the trail's bucket as, when another account owns it (an organization trail's log archive)
GITHUB_ORG ?=                   # the organization's login
GITHUB_TOKEN ?=                 # a token with read:audit_log
OKTA_ORG_URL ?=                 # https://acme.okta.com
OKTA_API_TOKEN ?=
GOOGLE_APPLICATION_CREDENTIALS ?=   # a service account key file with domain-wide delegation
GOOGLE_ADMIN_EMAIL ?=           # the admin the service account acts as
export AWS_REGION AWS_PROFILE CLOUDTRAIL_QUEUE_URL CLOUDTRAIL_BUCKET_ROLE GITHUB_ORG GITHUB_TOKEN OKTA_ORG_URL OKTA_API_TOKEN GOOGLE_APPLICATION_CREDENTIALS GOOGLE_ADMIN_EMAIL

# The packs' plugins, one of two ways. `fetch`: prebuilt, from the tangent
# release of this tangent's version, each checked against GitHub's attestation
# of its build before it is kept (needs gh, no login; nothing to compile).
# `compile`: built here from source, with the Rust WASM toolchain, for hacking
# on a pack or a tangent built from a checkout:  make run PLUGINS=compile
PLUGINS ?= fetch

setup:
	./setup.sh

fetch: setup
	$(TANGENT) plugin fetch --config ingest.yaml
	$(TANGENT) plugin fetch --config judge.yaml

compile: setup
	./setup.sh toolchain
	$(TANGENT) plugin compile --config ingest.yaml
	$(TANGENT) plugin compile --config judge.yaml

test: $(PLUGINS)
	$(TANGENT) plugin test --config ingest.yaml

# Two processes over one logs.sqlite, so ingest never waits on the model. The
# judge reads ingest's state.sqlite, each actor's history, and refuses to start
# without it, so ingest starts first; the judge waits for the file, and does not
# start once ingest has exited (a config error, say).
run: $(PLUGINS)
	$(TANGENT) run --config ingest.yaml & i=$$!; \
	while kill -0 $$i 2>/dev/null && [ ! -s state.sqlite ]; do sleep 1; done; sleep 1; \
	kill -0 $$i 2>/dev/null || { echo "ingest exited; not starting the judge" >&2; exit 1; }; \
	$(TANGENT) run --config judge.yaml

# A past range of the sources through ingest and the judge (README.md#replay):
# the sources whose past replay reads (CloudTrail, from its Event history),
# nothing to Slack.
# Each range is a directory of its own under replays/, with its own
# logs.sqlite, state.sqlite and alerts.ndjson:
#   make replay FROM=2026-10-01 TO=2026-10-02
FROM ?=                         # a date (midnight UTC) or an RFC 3339 time
TO ?=
REPLAY = replays/$(FROM)_$(TO)

replay: $(PLUGINS)
	@[ -n "$(FROM)" ] && [ -n "$(TO)" ] || \
	  { echo "usage: make replay FROM=2026-10-01 TO=2026-10-02" >&2; exit 1; }
	mkdir -p $(REPLAY)
	printf 'include: [../../ingest.yaml]\nruntime: { plugins_path: ../../plugins/ }\n' > $(REPLAY)/ingest.yaml
	printf 'include: [../../judge.yaml]\nruntime: { plugins_path: ../../plugins/ }\n' > $(REPLAY)/judge.yaml
	cd $(REPLAY) && $(TANGENT) replay --ingest ingest.yaml --judge judge.yaml --from $(FROM) --to $(TO)

.PHONY: setup fetch compile test run replay
