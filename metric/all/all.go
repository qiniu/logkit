package all

import (
	_ "github.com/qiniu/logkit/metric/curl"
	_ "github.com/qiniu/logkit/metric/system"
	_ "github.com/qiniu/logkit/metric/telegraf"
	_ "github.com/qiniu/logkit/metric/telegraf/memcached"
)
