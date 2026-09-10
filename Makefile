.PHONY: test lint vet

test:
	go test ./... -json | sift

lint:
	golangci-lint run ./...

vet:
	go vet ./...
