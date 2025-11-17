build:
./setup.sh
tangent plugin compile --config tangent.yaml

test: build
tangent plugin test --config tangent.yaml

run: build
tangent run --config tangent.yaml

.PHONY: build test
