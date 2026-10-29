.PHONY: test lint verify build e2e

# Built from tools/customlint (a separate module) with plain `go build`; it
# adds the goclocbudget plugin to golangci-lint.
custom-golangci-lint: tools/customlint/main.go tools/customlint/go.mod tools/customlint/go.sum
	go build -C tools/customlint -o "$(CURDIR)/custom-golangci-lint" .

test:
	go test -race ./...

lint: custom-golangci-lint
	./custom-golangci-lint run

verify: test lint

build:
	go build -o bin/agent-relay ./cmd/agent-relay

# Needs docker: runs agent-relay against a throwaway Matrix homeserver.
e2e:
	AGENT_RELAY_E2E_MATRIX=1 go test -tags e2e -count=1 -run TestEndToEndMatrix ./cmd/agent-relay
