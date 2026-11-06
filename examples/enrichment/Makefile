build:
	./setup.sh
	tangent plugin compile --config tangent.yaml

# The fixture's countries come from ipinfo.io, so the test makes its HTTP
# calls: `tangent plugin test` answers them with an empty 204 otherwise.
test: build
	tangent plugin test --config tangent.yaml --enable-http

run: build
	tangent run --config tangent.yaml

.PHONY: build test run
