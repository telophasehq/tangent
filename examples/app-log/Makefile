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
	$(TANGENT) plugin fetch --config tangent.yaml

compile: setup
	./setup.sh toolchain
	$(TANGENT) plugin compile --config tangent.yaml

test: $(PLUGINS)
	$(TANGENT) plugin test --config tangent.yaml

run: $(PLUGINS)
	$(TANGENT) run --config tangent.yaml

.PHONY: setup fetch compile test run
