package memcached

import (
	"errors"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/qiniu/log"

	"github.com/influxdata/telegraf/plugins/inputs/memcached"
	"github.com/qiniu/logkit/metric"
	"github.com/qiniu/logkit/metric/telegraf"
	. "github.com/qiniu/logkit/utils/models"
)

const MetricName = "memcached"

func init() {
	telegraf.AddUsage(MetricName, "Memcached(memcached)")
	telegraf.AddConfig(MetricName, map[string]interface{}{
		metric.OptionString: []Option{
			{
				KeyName:      "servers",
				ChooseOnly:   false,
				Default:      `localhost:11211`,
				DefaultNoUse: true,
				Description:  "服务器连接地址(逗号分隔多个)",
				Type:         metric.ConsifTypeString,
			},
		},
		metric.AttributesString: KeyValueSlice{
			{"memcached_get_hits", "get命中次数", ""},
			{"memcached_get_misses", "get未命中次数", ""},
			{"memcached_evictions", "驱逐次数", ""},
			{"memcached_limit_maxbytes", "限制大字节数", ""},
			{"memcached_bytes", "字节数", ""},
			{"memcached_uptime", "运行时间", ""},
			{"memcached_curr_items", "当前数量", ""},
			{"memcached_total_items", "总数量", ""},
			{"memcached_curr_connections", "当前连接数", ""},
			{"memcached_total_connections", "总连接数", ""},
			{"memcached_connection_structures", "连接结构数", ""},
			{"memcached_cmd_get", "get请求数", ""},
			{"memcached_cmd_set", "set请求数", ""},
			{"memcached_delete_hits", "delete命中次数", ""},
			{"memcached_delete_misses", "delete未命中次数", ""},
			{"memcached_incr_hits", "incr命中次数", ""},
			{"memcached_incr_misses", "incr未命中次数", ""},
			{"memcached_decr_hits", "decr命中次数", ""},
			{"memcached_decr_misses", "decr未命中次数", ""},
			{"memcached_cas_hits", "cas命中次数", ""},
			{"memcached_cas_misses", "cas未命中次数", ""},
			{"memcached_bytes_read", "读取字节数", ""},
			{"memcached_bytes_written", "响应字节数", ""},
			{"memcached_threads", "线程数", ""},
			{"memcached_conn_yields", "连接退让次数", ""},
		},
	})
}

type collector struct {
	*telegraf.Collector
}

func (c *collector) SyncConfig(data map[string]interface{}) error {
	mc, ok := c.Input.(*memcached.Memcached)
	if !ok {
		return errors.New("unexpected telegraf type, want '*memcached.Memcached'")
	}

	servers, ok := data["servers"].(string)
	if ok {
		mc.Servers = strings.Split(servers, ",")
	}

	return nil
}

// NewCollector creates a new Memcached collector.
func NewCollector() metric.Collector {
	input := inputs.Inputs[MetricName]()

	if _, err := toml.Decode(input.SampleConfig(), input); err != nil {
		log.Errorf("metric: failed to decode sample config of memcached: %v", err)
	}
	return &collector{telegraf.NewCollector(MetricName, input)}
}

func init() {
	metric.Add(MetricName, NewCollector)
}
