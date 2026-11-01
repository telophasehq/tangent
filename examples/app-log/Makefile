TANGENT ?= tangent

# The judge: any OpenAI-compatible endpoint, a local model on :8080 by default
# (packs/core/judge.yaml has the defaults). A hosted one:
#   JUDGE_URL=https://api.openai.com/v1 JUDGE_MODEL=gpt-5.1 JUDGE_API_KEY=$OPENAI_API_KEY make run

setup:
	./setup.sh

compile: setup
	$(TANGENT) plugin compile --config ingest.yaml
	$(TANGENT) plugin compile --config judge.yaml

test: compile
	$(TANGENT) plugin test --config ingest.yaml

# Two processes over one logs.sqlite, so ingest never waits on the model.
run: compile
	$(TANGENT) run --config judge.yaml & $(TANGENT) run --config ingest.yaml

.PHONY: setup compile test run
