TANGENT ?= tangent
export TANGENT

# Every pack: a directory with an ingest.yaml and a plugin (enrich-ip,
# behavior-baseline, judge and macos have none).
PACKS := $(sort $(patsubst %/Cargo.toml,%,$(wildcard */Cargo.toml)))
PACKS := $(filter $(PACKS),$(patsubst %/ingest.yaml,%,$(wildcard */ingest.yaml)))

# Each pack alone: its plugin compiled and its fixtures run from its own
# ingest.yaml, against the tables in its tests/, so a pack that leans on
# another file fails here.
test: $(addprefix test-,$(PACKS)) test-baseline test-shared

test-%:
	cd $* && $(TANGENT) plugin compile --config ingest.yaml && $(TANGENT) plugin test --config ingest.yaml

# behavior-baseline: a sign-in story through the renderer and the builder over
# one state (behavior-baseline/story.sh), then the unit tests of its kit, builder
# and renderer. The story goes first: its `tangent plugin compile` writes the
# renderer's .tangent/wit, which the renderer's `cargo test` needs, and a
# fresh clone has none.
test-baseline:
	behavior-baseline/story.sh
	for c in kit builder renderer; do (cd behavior-baseline/$$c && cargo test) || exit 1; done

# The shared libraries' own tests.
test-shared:
	for lib in normkit enrichkit identitykit factskit; do (cd shared/$$lib && cargo test) || exit 1; done

# What a release ships: every pack's plugin as a component, in
# ../target/packs (dist.sh), which `tangent plugin fetch` downloads from the
# release and checks GitHub's attestation of.
dist:
	./dist.sh

# tests/expected.json from the plugin's output; read the diff before committing.
fixtures-%:
	./fixtures.sh $*

fixtures-baseline:
	behavior-baseline/story.sh write

.PHONY: test test-baseline test-shared dist fixtures-baseline
