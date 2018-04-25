# mysqllog [![GoDoc](https://godoc.org/github.com/Preetam/mysqllog?status.svg)](https://godoc.org/github.com/Preetam/mysqllog) [![CircleCI](https://circleci.com/gh/Preetam/mysqllog.svg?style=svg)](https://circleci.com/gh/Preetam/mysqllog)

This package provides a simple MySQL slow query log parser.

## Example: Parse a log from stdin and print events as JSON

```go
package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"

	"github.com/Preetam/mysqllog"
)

func main() {
	p := &mysqllog.Parser{}
	reader := bufio.NewReader(os.Stdin)
	for line, err := reader.ReadString('\n'); err == nil; line, err = reader.ReadString('\n') {
		event := p.ConsumeLine(line)
		if event != nil {
			b, _ := json.Marshal(event)
			fmt.Printf("%s\n", b)
		}
	}
}
```

License
---

MIT (see LICENSE)
