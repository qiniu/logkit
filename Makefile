all:
	go generate; CGO_ENABLED=1 go build -v -o logkit

install: all
	@echo

test:
	go generate; CGO_ENABLED=1 go test -race -cover ./...

clean:
	go clean -i ./...

style:
	@$(QCHECKSTYLE) logkit

gofmt:
	find . -name '*.go' | xargs -l1 go fmt
