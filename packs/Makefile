TANGENT ?= tangent
export TANGENT

# Every pack: a directory with an ingest.yaml and a plugin (core and macos
# have none).
PACKS := $(sort $(patsubst %/Cargo.toml,%,$(wildcard */Cargo.toml)))
PACKS := $(filter $(PACKS),$(patsubst %/ingest.yaml,%,$(wildcard */ingest.yaml)))

# Each pack alone: its plugin compiled and its fixtures run from its own
# ingest.yaml, against the tables in its tests/, so a pack that leans on
# another file fails here.
test: $(addprefix test-,$(PACKS)) test-history test-shared

test-%:
	cd $* && $(TANGENT) plugin compile --config ingest.yaml && $(TANGENT) plugin test --config ingest.yaml

# Core's history plugin: its unit tests, then a sign-in story through it in
# `mode: both`, from core/history/test.yaml.
test-history:
	cd core/history && cargo test && $(TANGENT) plugin compile --config test.yaml && $(TANGENT) plugin test --config test.yaml

# The shared libraries' own tests.
test-shared:
	for lib in normkit enrichkit factskit; do (cd shared/$$lib && cargo test) || exit 1; done

# tests/expected.json from the plugin's output; read the diff before committing.
fixtures-%:
	./fixtures.sh $*

fixtures-history:
	CONFIG=test.yaml ./fixtures.sh core/history

.PHONY: test test-history test-shared fixtures-history
