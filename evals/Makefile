# The evals in one command, from the repository root:
#
#   make -C evals                  Claude Haiku 5.5 on Bedrock, signed in as AWS_PROFILE: about $1 a run
#   make -C evals JUDGE=local      packs/core's judge at JUDGE_URL (README.md#run-it): free, well over an hour
#   make -C evals corpus           the corpus alone, no model
#
# It makes what is missing or older than what makes it: the corpus (gen.py,
# stream.py, past.py, windows.py), tangent, the packs' plugins, and the
# enrichment data once (make -C examples/cloud-infra setup refreshes it). Then
# it replays the corpus through examples/cloud-infra in SHARDS replays at once,
# in a run directory of its own (the sinks append), and scores the verdicts
# into score.txt there. RUN names the directory, relative to evals/:
# runs/<judge>-<time> by default. TANGENT uses a tangent you built.

ROOT := $(abspath ..)
PY := python3 -I
JUDGE ?= bedrock
SHARDS ?= 10
TANGENT ?= $(ROOT)/target/release/tangent
ifndef RUN
RUN := runs/$(JUDGE)-$(shell date +%Y%m%d-%H%M%S)
endif
WORK := $(abspath $(RUN))
# replay.py reads JUDGE as the judge's config file
unexport JUDGE
ifeq ($(JUDGE),bedrock)
WAIT := AWS_WAIT_PROFILE=$${AWS_WAIT_PROFILE:-$${AWS_PROFILE:-}}
endif

STREAM := $(CURDIR)/runs/stream
TRUTH := $(STREAM)/truth-past-v3.json
WINDOWS := $(CURDIR)/runs/windows-v3.json
DATA := $(ROOT)/examples/enrich/data/networks.csv

.DELETE_ON_ERROR:

run: new corpus plugins
	$(PY) harness/maincfg.py isolated $(WORK) --judge $(JUDGE)
	@echo "replaying in $(SHARDS) shards; progress in $(RUN)/s*/run.log"
	echo $$$$ > $(WORK)/pid; TANGENT=$(TANGENT) TRUTH=$(TRUTH) $(WAIT) \
	  $(PY) harness/shard.py $(WORK) $(STREAM)/stream-past-v3.ndjson $(WINDOWS) $(SHARDS) > $(WORK)/shard.out 2>&1 \
	  || { tail -n 3 $(WORK)/s*/run.log >&2; tail -n 1 $(WORK)/shard.out >&2; exit 1; }
	$(PY) harness/score_v2.py $(TRUTH) $(WORK) > $(WORK)/score.txt && cat $(WORK)/score.txt

new: idle
	@[ ! -e $(WORK) ] || { echo "$(RUN) exists: a run needs a directory of its own (RUN=...)" >&2; exit 1; }

# A run reads the corpus, tangent and the plugins until it ends: none is
# rebuilt under it.
idle:
	@for p in runs/*/pid; do [ -e "$$p" ] && kill -0 $$(cat "$$p") 2>/dev/null \
	  && { echo "$${p%/pid} is still running and reads what this would rebuild: wait for it" >&2; exit 1; }; done; true

corpus: idle $(TRUTH) $(WINDOWS)

# Each step depends on the file its script writes last.
runs/stratus/cases.json: stratus/splice.py stratus/techniques.json $(wildcard stratus/logs/*.json)
	$(PY) stratus/splice.py

$(STREAM)/truth-v3.json: gen.py stream.py runs/stratus/cases.json
	$(PY) gen.py
	$(PY) stream.py v3

$(TRUTH): $(STREAM)/truth-v3.json harness/past.py
	$(PY) harness/past.py v3

$(WINDOWS): $(TRUTH) harness/windows.py
	$(PY) harness/windows.py $(TRUTH) $(WINDOWS)

# The WASM toolchain and the enrichment data, if there is none yet.
$(DATA):
	$(ROOT)/examples/cloud-infra/setup.sh

# Cargo and plugin compile do only what changed; a plugin compiled by another
# tangent build does not load, so both run every time.
plugins: idle $(DATA)
ifeq ($(TANGENT),$(ROOT)/target/release/tangent)
	cd $(ROOT) && CARGO_TARGET_DIR=$(ROOT)/target cargo build --release -p tangent-cli
else
	@echo "using $(TANGENT)"
endif
	cd $(ROOT)/examples/cloud-infra && $(TANGENT) plugin compile --config ingest.yaml \
	  && $(TANGENT) plugin compile --config judge.yaml

.PHONY: run new idle corpus plugins
