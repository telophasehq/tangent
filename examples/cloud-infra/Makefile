TANGENT ?= tangent

# The judge: any OpenAI-compatible endpoint, a local model on :8080 by default
# (packs/judge/ingest.yaml has the defaults). A hosted one:
#   JUDGE_URL=https://api.openai.com/v1 JUDGE_MODEL=gpt-5.1 JUDGE_API_KEY=$OPENAI_API_KEY make run

# The sources. Each reads its API directly and wakes up when its credential is
# set, in the environment or on the command line (make run GITHUB_ORG=acme
# GITHUB_TOKEN=...); left unset, it says so once and stays idle. The README says
# what each credential needs.
#   AWS_REGION                      CloudTrail: the queue's region; the SDK's default chain when unset
#   AWS_PROFILE
#   CLOUDTRAIL_QUEUE_URL            the SQS queue a trail's delivery notifications land in
#   CLOUDTRAIL_BUCKET_ROLE          a role to read the trail's bucket as, when another account owns it (an organization trail's log archive)
#   GITHUB_ORG                      the organization's login
#   GITHUB_TOKEN                    a token with read:audit_log
#   OKTA_ORG_URL                    https://acme.okta.com
#   OKTA_API_TOKEN
#   GOOGLE_APPLICATION_CREDENTIALS  a service account key file with domain-wide delegation
#   GOOGLE_ADMIN_EMAIL              the admin the service account acts as
# None is given a default here: an empty AWS_REGION or AWS_PROFILE in the
# environment is not an unset one to the AWS SDK, and would hide the region and
# profile in ~/.aws/config.

# The packs' plugins, one of two ways. `fetch`: prebuilt, from the tangent
# release of this tangent's version, each checked against GitHub's attestation
# of its build before it is kept (needs gh, no login; nothing to compile).
# `compile`: built here from source, with the Rust WASM toolchain, for hacking
# on a pack or a tangent built from a checkout:  make run PLUGINS=compile
PLUGINS ?= fetch

setup:
	./setup.sh

fetch: setup
	$(TANGENT) plugin fetch --config tangent.yaml

# setup.sh toolchain fetches the data too
compile:
	./setup.sh toolchain
	$(TANGENT) plugin compile --config tangent.yaml

test: $(PLUGINS)
	$(TANGENT) plugin test --config tangent.yaml

run: $(PLUGINS)
	$(TANGENT) run --config tangent.yaml

.PHONY: setup fetch compile test run
