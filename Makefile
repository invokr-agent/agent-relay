.PHONY: test lint verify build

# Built from tools/customlint (a separate module) with plain `go build`; it
# adds the goclocbudget plugin to golangci-lint, as in invokr.
custom-golangci-lint: tools/customlint/main.go tools/customlint/go.mod tools/customlint/go.sum
	go build -C tools/customlint -o "$(CURDIR)/custom-golangci-lint" .

test:
	go test -race ./...

lint: custom-golangci-lint
	./custom-golangci-lint run

verify: test lint

build:
	go build -o bin/agent-relay ./cmd/agent-relay
