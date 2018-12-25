package elasticsearch

import (
	"errors"
	"fmt"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/inputs/elasticsearch"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/metric"
	"github.com/qiniu/logkit/metric/telegraf"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/utils/models"
)

const MetricName = "elasticsearch"

var (
	ConfigServers            = "servers"
	ConfigLocal              = "local"
	ConfigClusterHealth      = "cluster_health"
	ConfigClusterHealthLevel = "cluster_health_level"
	ConfigClusterStats       = "cluster_stats"
	ConfigInsecureSkipVerify = "insecure_skip_verify"
	ConfigTLSCA              = "tls_ca"
	ConfigTLSCert            = "tls_cert"
	ConfigTLSKey             = "tls_key"

	StatsIndices    = "indices"
	StatsOS         = "os"
	StatsProcess    = "process"
	StatsJVM        = "jvm"
	StatsThreadPool = "thread_pool"
	StatsFS         = "fs"
	StatsTransport  = "transport"
	StatsHttp       = "http"
	StatsBreaker    = "breaker"
)

func init() {
	telegraf.AddUsage(MetricName, "ES(elasticsearch)")
	telegraf.AddConfig(MetricName, map[string]interface{}{
		metric.OptionString: []Option{
			{
				KeyName:      ConfigServers,
				ChooseOnly:   false,
				Default:      `http://localhost:9200`,
				DefaultNoUse: true,
				Description:  "服务器连接地址(逗号分隔多个)",
				Type:         metric.ConfigTypeString,
			},
			{
				KeyName:       ConfigLocal,
				ChooseOnly:    true,
				ChooseOptions: []interface{}{"true", "false"},
				Default:       true,
				DefaultNoUse:  false,
				Description:   "只读取本节点的状态信息",
				Type:          metric.ConfigTypeBool,
			},
			{
				KeyName:       ConfigClusterHealth,
				ChooseOnly:    true,
				ChooseOptions: []interface{}{"true", "false"},
				Default:       false,
				DefaultNoUse:  false,
				Description:   "只获取状态健康的集群信息",
				Type:          metric.ConfigTypeBool,
			},
			{
				KeyName:       ConfigClusterHealthLevel,
				ChooseOnly:    true,
				ChooseOptions: []interface{}{"indices", "cluster"},
				Default:       "indices",
				DefaultNoUse:  false,
				Description:   "获取信息的健康等级",
			},
			{
				KeyName:       ConfigClusterStats,
				ChooseOnly:    true,
				ChooseOptions: []interface{}{"true", "false"},
				Default:       false,
				DefaultNoUse:  false,
				Description:   "只从master节点获取信息",
				Type:          metric.ConfigTypeBool,
			},
			{
				KeyName:       ConfigInsecureSkipVerify,
				ChooseOnly:    true,
				ChooseOptions: []interface{}{"true", "false"},
				Default:       true,
				DefaultNoUse:  false,
				Description:   "跳过校验SSL证书",
				Type:          metric.ConfigTypeBool,
			},
			{
				KeyName:            ConfigTLSCA,
				ChooseOnly:         false,
				Default:            "",
				Required:           false,
				Placeholder:        "证书授权的地址.ca",
				DefaultNoUse:       true,
				AdvanceDepend:      "insecure_skip_verify",
				AdvanceDependValue: false,
				Description:        "证书授权地址(tls_ca)",
				ToolTip:            `证书授权地址`,
			},
			{
				KeyName:            ConfigTLSCert,
				ChooseOnly:         false,
				Default:            "",
				Required:           false,
				Placeholder:        "证书的地址.cert",
				DefaultNoUse:       true,
				AdvanceDepend:      "insecure_skip_verify",
				AdvanceDependValue: false,
				Description:        "证书地址(tls_cert)",
				ToolTip:            `证书地址`,
			},
			{
				KeyName:            ConfigTLSKey,
				ChooseOnly:         false,
				Default:            "",
				Required:           false,
				Placeholder:        "秘钥文件的地址.key",
				DefaultNoUse:       true,
				AdvanceDepend:      "insecure_skip_verify",
				AdvanceDependValue: false,
				Description:        "私钥文件地址(tls_key)",
				ToolTip:            `私钥文件地址`,
			},
		},
		metric.AttributesString: KeyValueSlice{
			{StatsIndices, "文件索引信息", ""},
			{StatsOS, "操作系统信息", ""},
			{StatsJVM, "Java虚拟机信息", ""},
			{StatsProcess, "进程信息", ""},
			{StatsThreadPool, "线程池信息", ""},
			{StatsFS, "文件系统信息", ""},
			{StatsTransport, "传输信息", ""},
			{StatsHttp, "http信息", ""},
			{StatsBreaker, "breaker信息", ""},
		},
	})
}

type collector struct {
	*telegraf.Collector
}

func (c *collector) SyncConfig(data map[string]interface{}, meta *reader.Meta) error {
	es, ok := c.Input.(*elasticsearch.Elasticsearch)
	if !ok {
		return errors.New("unexpected elasticsearch type, want '*elasticsearch.Elasticsearch'")
	}
	servers, ok := data[ConfigServers].(string)
	if !ok {
		return fmt.Errorf("key servers want as string,actual get %T\n", data[ConfigServers])
	}
	es.Servers = strings.Split(servers, ",")

	local, ok := data[ConfigLocal].(bool)
	if ok {
		es.Local = local
	}
	health, ok := data[ConfigClusterHealth].(bool)
	if ok {
		es.ClusterHealth = health
	}
	healthLevel, ok := data[ConfigClusterHealthLevel].(string)
	if ok {
		es.ClusterHealthLevel = strings.TrimSpace(healthLevel)
	}
	ClusterStats, ok := data[ConfigClusterStats].(bool)
	if ok {
		es.ClusterStats = ClusterStats
	}
	InsecureSkipVerify, ok := data[ConfigInsecureSkipVerify].(bool)
	if ok {
		es.InsecureSkipVerify = InsecureSkipVerify
	}
	TLSCA, ok := data[ConfigTLSCA].(string)
	if ok {
		es.TLSCA = TLSCA
	}
	TLSCert, ok := data[ConfigTLSCert].(string)
	if ok {
		es.TLSCert = TLSCert
	}
	TLSKey, ok := data[ConfigTLSKey].(string)
	if ok {
		es.TLSKey = TLSKey
	}

	return nil
}

// NewCollector creates a new Elasticsearch collector.
func NewCollector() metric.Collector {
	input := inputs.Inputs[MetricName]()
	if _, err := toml.Decode(input.SampleConfig(), input); err != nil {
		log.Errorf("metric: failed to decode sample config of elasticsearch: %v", err)
	}
	return &collector{telegraf.NewCollector(MetricName, input)}
}

func init() {
	metric.Add(MetricName, NewCollector)
}
