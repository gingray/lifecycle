.PHONY: test test-ci lint vet

test:
	go test -race ./... -json | sift

test-ci:
	go test -race ./...

lint:
	golangci-lint run ./...

vet:
	go vet ./...
