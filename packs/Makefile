TANGENT ?= tangent
export TANGENT

# Every pack: a directory with an ingest.yaml and a plugin (core and macos
# have none).
PACKS := $(sort $(patsubst %/Cargo.toml,%,$(wildcard */Cargo.toml)))
PACKS := $(filter $(PACKS),$(patsubst %/ingest.yaml,%,$(wildcard */ingest.yaml)))

# Each pack alone: its plugin compiled and its fixtures run from its own
# ingest.yaml, against the tables in its tests/, so a pack that leans on
# another file fails here.
test: $(addprefix test-,$(PACKS)) test-baseline test-shared

test-%:
	cd $* && $(TANGENT) plugin compile --config ingest.yaml && $(TANGENT) plugin test --config ingest.yaml

# Core's baseline plugin: its unit tests, then a sign-in story through it in
# `mode: both`, from core/baseline/test.yaml.
test-baseline:
	cd core/baseline && cargo test && $(TANGENT) plugin compile --config test.yaml && $(TANGENT) plugin test --config test.yaml

# The shared libraries' own tests.
test-shared:
	for lib in normkit enrichkit factskit; do (cd shared/$$lib && cargo test) || exit 1; done

# tests/expected.json from the plugin's output; read the diff before committing.
fixtures-%:
	./fixtures.sh $*

fixtures-baseline:
	CONFIG=test.yaml ./fixtures.sh core/baseline

.PHONY: test test-baseline test-shared fixtures-baseline
