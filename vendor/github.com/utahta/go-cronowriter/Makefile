.PHONY: fmt test

install:
	@dep ensure

fmt:
	@goimports -w .

test:
	@go test -v -race

test-coverage:
	@go test -v -race -coverprofile=coverage.txt -covermode=atomic

bench:
	@go test -bench .
