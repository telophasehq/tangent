TANGENT ?= tangent

# The packs' plugins, as in examples/cloud-infra: `fetch` takes them prebuilt
# from the release, `compile` builds them here with the Rust WASM toolchain.
# Until a release carries them (v0.1.9 does not): make PLUGINS=compile
PLUGINS ?= fetch

# The judge, as packs/core/judge.yaml has it: a local model on :8080. A hosted
# one takes hosted.yaml, which leaves out the local model's sampling settings:
#   JUDGE_URL=https://api.openai.com/v1 JUDGE_MODEL=gpt-5.1 JUDGE_API_KEY=$OPENAI_API_KEY make CONFIG=hosted.yaml
CONFIG ?= tangent.yaml

demo: $(PLUGINS) ../enrich/data/GeoLite2-ASN.mmdb
	rm -f state.sqlite state.sqlite.lock alerts.ndjson verdicts.ndjson
	@echo "==> 1/2 the history: 10,132 ordinary records folded into state.sqlite, no model"
	$(TANGENT) run --once --config learn.yaml > learn.log 2>&1 || { tail -20 learn.log; exit 1; }
	@echo "==> 2/2 the case: 2 records judged against that history"
	$(TANGENT) run --once --config $(CONFIG) > judge.log 2>&1 || { tail -20 judge.log; exit 1; }
	@echo "==> $$(wc -l < verdicts.ndjson | tr -d ' ') verdicts in verdicts.ndjson, $$(wc -l < alerts.ndjson | tr -d ' ') of them in alerts.ndjson:"
	@cat alerts.ndjson

# GeoLite2, the cloud ranges and the Tor exit list, which core's tables read
../enrich/data/GeoLite2-ASN.mmdb:
	../enrich/setup.sh data

fetch:
	$(TANGENT) plugin fetch --config tangent.yaml

compile:
	../enrich/setup.sh
	$(TANGENT) plugin compile --config tangent.yaml

clean:
	rm -f state.sqlite state.sqlite.lock alerts.ndjson verdicts.ndjson learn.log judge.log
	rm -fr lake plugins

.PHONY: demo fetch compile clean
