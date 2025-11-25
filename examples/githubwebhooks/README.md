# githubwebhooks

Go component for Tangent.

## Setup
```bash
./setup.sh
```

## Compile
```bash
tangent plugin compile --config tangent.yaml
```

## Test
```bash
tangent plugin test --config tangent.yaml
```

## Run server
```bash
tangent run --config tangent.yaml
```

## Benchmark performance
```bash
tangent run --config tangent.yaml
tangent bench --config tangent.yaml --seconds 30 --payload tests/input.json
```


## Using Makefile
```bash
# build and test
make test

# build and run
make run
```

