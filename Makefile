all:
	go get ./...; cd main;CGO_ENABLED=0 go build -v -o logkit

install: all
	@echo

test:
	govendor sync; go get ./...;  CGO_ENABLED=1 go test -cover ./...

clean:
	go clean -i ./...

style:
	@$(QCHECKSTYLE) logkit

gofmt:
	find . -name '*.go' | xargs -l1 go fmt
