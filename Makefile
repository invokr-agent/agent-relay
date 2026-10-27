.PHONY: test lint build

test:
	go test -race ./...

lint:
	go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.15.0 run ./...

build:
	go build -o bin/messaging-kit ./cmd/messaging-kit
