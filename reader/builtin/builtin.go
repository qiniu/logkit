// Package builtin does nothing but import all builtin readers to execute their init functions.
package builtin

import (
	_ "github.com/qiniu/logkit/reader/autofile"
	_ "github.com/qiniu/logkit/reader/cloudtrail"
	_ "github.com/qiniu/logkit/reader/cloudwatch"
	_ "github.com/qiniu/logkit/reader/dirx"
	_ "github.com/qiniu/logkit/reader/elastic"
	_ "github.com/qiniu/logkit/reader/http"
	_ "github.com/qiniu/logkit/reader/kafka"
	_ "github.com/qiniu/logkit/reader/mockreader"
	_ "github.com/qiniu/logkit/reader/mongo"
	_ "github.com/qiniu/logkit/reader/redis"
	_ "github.com/qiniu/logkit/reader/script"
	_ "github.com/qiniu/logkit/reader/snmp"
	_ "github.com/qiniu/logkit/reader/socket"
	_ "github.com/qiniu/logkit/reader/sql"
	_ "github.com/qiniu/logkit/reader/tailx"
)
