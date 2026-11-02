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

# Core's baseline: the unit tests of its kit, builder and renderer, then a
# sign-in story through the renderer and the builder over one state
# (core/baseline/story.sh).
test-baseline:
	for c in kit builder renderer; do (cd core/baseline/$$c && cargo test) || exit 1; done
	core/baseline/story.sh

# The shared libraries' own tests.
test-shared:
	for lib in normkit enrichkit identitykit factskit; do (cd shared/$$lib && cargo test) || exit 1; done

# tests/expected.json from the plugin's output; read the diff before committing.
fixtures-%:
	./fixtures.sh $*

fixtures-baseline:
	core/baseline/story.sh write

.PHONY: test test-baseline test-shared fixtures-baseline
