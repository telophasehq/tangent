build:
	./setup.sh
	tangent plugin compile --config tangent.yaml

data:
	./setup.sh data

test: build
	tangent plugin test --config tangent.yaml

run: build
	tangent run --config tangent.yaml

.PHONY: build data test run
