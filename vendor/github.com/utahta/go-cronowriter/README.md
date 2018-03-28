# cronowriter

[![GoDoc Reference](https://godoc.org/github.com/utahta/go-cronowriter?status.svg)](http://godoc.org/github.com/utahta/go-cronowriter)
[![CircleCI](https://circleci.com/gh/utahta/go-cronowriter.svg?style=svg)](https://circleci.com/gh/utahta/go-cronowriter)
[![codecov](https://codecov.io/gh/utahta/go-cronowriter/branch/master/graph/badge.svg)](https://codecov.io/gh/utahta/go-cronowriter)
[![Go Report Card](https://goreportcard.com/badge/github.com/utahta/go-cronowriter)](https://goreportcard.com/report/github.com/utahta/go-cronowriter)
[![GitHub release](https://img.shields.io/github/release/utahta/go-cronowriter.svg)](https://github.com/utahta/go-cronowriter/releases)

This is a simple file writer that it writes message to the specified format path.

The file path is constructed based on current date and time, like cronolog.

This project follows the [Semantic Versioning](https://semver.org/).

## Installation

```
$ go get -u github.com/utahta/go-cronowriter
```

## Documentation

API documentation can be found [here](http://godoc.org/github.com/utahta/go-cronowriter).

The format specifications can be found [here](https://github.com/lestrrat-go/strftime#supported-conversion-specifications).

## Examples

```go
import "github.com/utahta/go-cronowriter"
```

```go
w := cronowriter.MustNew("/path/to/example.log.%Y%m%d")
w.Write([]byte("test"))

// output file
// /path/to/example.log.20170204
```

You can specify the directory as below
```go
w := cronowriter.MustNew("/path/to/%Y/%m/%d/example.log")
w.Write([]byte("test"))

// output file
// /path/to/2017/02/04/example.log
```

with Location
```go
w := cronowriter.MustNew("/path/to/example.log.%Z", writer.WithLocation(time.UTC))
w.Write([]byte("test"))

// output file
// /path/to/example.log.UTC
```

with Symlink
```go
w := cronowriter.MustNew("/path/to/example.log.%Y%m%d", writer.WithSymlink("/path/to/example.log"))
w.Write([]byte("test"))

// output file
// /path/to/example.log.20170204
// /path/to/example.log -> /path/to/example.log.20170204
```

with Mutex
```go
w := cronowriter.MustNew("/path/to/example.log.%Y%m%d", writer.WithMutex())
```

no use Mutex
```go
w := cronowriter.MustNew("/path/to/example.log.%Y%m%d", writer.WithNopMutex())
```

with Debug (stdout and stderr)
```go
w := cronowriter.MustNew("/path/to/example.log.%Y%m%d", writer.WithDebug())
w.Write([]byte("test"))

// output file, stdout and stderr
// /path/to/example.log.20170204
```

with Init
```go
w := cronowriter.MustNew("/path/to/example.log.%Y%m%d", writer.WithInit())

// open the file when New() method is called
// /path/to/example.log.20170204
```

## Example using with zap

### [uber-go/zap](https://github.com/uber-go/zap)

```go
package main

import (
	"github.com/uber-go/zap"
	"github.com/utahta/go-cronowriter"
)

func main() {
	w1 := cronowriter.MustNew("/tmp/example.log.%Y%m%d")
	w2 := cronowriter.MustNew("/tmp/internal_error.log.%Y%m%d")
	l := zap.New(
		zap.NewJSONEncoder(),
		zap.Output(zap.AddSync(w1)),
		zap.ErrorOutput(zap.AddSync(w2)),
	)
	l.Info("test")
}

// output
// /tmp/example.log.20170204
// {"level":"info","ts":1486198722.1201255,"msg":"test"}
```

## Contributing

1. Fork it ( https://github.com/utahta/go-cronowriter/fork )
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

