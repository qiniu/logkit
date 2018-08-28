// Package builtin does nothing but import all builtin parsers to execute their init functions.
package builtin

import (
	_ "github.com/qiniu/logkit/parser/csv"
	_ "github.com/qiniu/logkit/parser/empty"
	_ "github.com/qiniu/logkit/parser/grok"
	_ "github.com/qiniu/logkit/parser/json"
	_ "github.com/qiniu/logkit/parser/kafkarest"
	_ "github.com/qiniu/logkit/parser/logfmt"
	_ "github.com/qiniu/logkit/parser/mysql"
	_ "github.com/qiniu/logkit/parser/nginx"
	_ "github.com/qiniu/logkit/parser/qiniu"
	_ "github.com/qiniu/logkit/parser/raw"
	_ "github.com/qiniu/logkit/parser/syslog"
)
