package builtin

import (
	_ "github.com/qiniu/logkit/metric/curl"
	_ "github.com/qiniu/logkit/metric/system"
	_ "github.com/qiniu/logkit/metric/telegraf"
	_ "github.com/qiniu/logkit/metric/telegraf/docker"
	_ "github.com/qiniu/logkit/metric/telegraf/elasticsearch"
	_ "github.com/qiniu/logkit/metric/telegraf/http_response"
	_ "github.com/qiniu/logkit/metric/telegraf/memcached"
)
