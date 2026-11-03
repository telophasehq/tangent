TANGENT ?= tangent

# The judge: any OpenAI-compatible endpoint, a local model on :8080 by default
# (packs/core/judge.yaml has the defaults). A hosted one:
#   JUDGE_URL=https://api.openai.com/v1 JUDGE_MODEL=gpt-5.1 JUDGE_API_KEY=$OPENAI_API_KEY make run

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

.PHONY: setup fetch compile test run
