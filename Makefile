.PHONY: build test bench clean demo

# Build both the server and CLI binaries
build:
	go build -o bin/dkv-server ./cmd/server
	go build -o bin/dkv-cli ./cmd/cli

# Run all unit tests
test:
	go test ./pkg/... -v -count=1

# Run benchmarks for the storage engine
bench:
	go test ./pkg/storage/... -bench=. -benchmem

# Wipe build artifacts and any test data directories
clean:
	rm -rf bin/
	rm -rf /tmp/dkv-*

# Spin up a 3-node cluster locally
run-cluster:
	./start-cluster.sh
