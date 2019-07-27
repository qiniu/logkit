package builtin

import (
	_ "github.com/qiniu/logkit/sender/csv"
	_ "github.com/qiniu/logkit/sender/discard"
	_ "github.com/qiniu/logkit/sender/elasticsearch"
	_ "github.com/qiniu/logkit/sender/file"
	_ "github.com/qiniu/logkit/sender/http"
	_ "github.com/qiniu/logkit/sender/influxdb"
	_ "github.com/qiniu/logkit/sender/kafka"
	_ "github.com/qiniu/logkit/sender/mock"
	_ "github.com/qiniu/logkit/sender/mongodb"
	_ "github.com/qiniu/logkit/sender/mysql"
	_ "github.com/qiniu/logkit/sender/open_falcon"
	_ "github.com/qiniu/logkit/sender/pandora"
	_ "github.com/qiniu/logkit/sender/sqlfile"
)
