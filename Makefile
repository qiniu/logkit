all:
	go generate
	CGO_ENABLED=1 go install -v
	mv ${GOPATH}/bin/logkit .

install: all
	@echo

test:
	go generate; CGO_ENABLED=1 go test -race ./...

clean:
	go clean -i ./...

style:
	@$(QCHECKSTYLE) logkit

gofmt:
	find . -name '*.go' | xargs -l1 go fmt
