.PHONY: test

test:
	go test ./... -json | sift
