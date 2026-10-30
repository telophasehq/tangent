TANGENT ?= tangent

# The judge: any OpenAI-compatible endpoint, a local model on :8080 by default. A hosted one:
#   JUDGE_URL=https://api.openai.com/v1 JUDGE_MODEL=gpt-5.1 JUDGE_API_KEY=$OPENAI_API_KEY make run
JUDGE_URL ?= http://localhost:8080/v1
JUDGE_MODEL ?= glm-4.7-flash
JUDGE_API_KEY ?= none
export JUDGE_URL JUDGE_MODEL JUDGE_API_KEY

setup:
	./setup.sh

compile: setup
	$(TANGENT) plugin compile --config tangent.yaml

test: compile
	$(TANGENT) plugin test --config tangent.yaml

run: compile
	$(TANGENT) run --config tangent.yaml

.PHONY: setup compile test run
