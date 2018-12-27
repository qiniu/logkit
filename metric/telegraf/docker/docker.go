package docker

import (
	"errors"
	"fmt"
	"strings"

	"github.com/BurntSushi/toml"
	"github.com/influxdata/telegraf/plugins/inputs"
	"github.com/influxdata/telegraf/plugins/inputs/docker"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/metric"
	"github.com/qiniu/logkit/metric/telegraf"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/utils/models"
)

const MetricName = "docker"

var (
	ConfigEndpoint              = "endpoint"
	ConfigGatherServices        = "gather_services"
	ConfigContainerNames        = "container_names"
	ConfigContainerNameInclude  = "container_name_include"
	ConfigContainerNameExclude  = "container_name_exclude"
	ConfigContainerStateInclude = "container_state_include"
	ConfigContainerStateExclude = "container_state_exclude"
	ConfigPerDevice             = "perdevice"
	ConfigTotal                 = "total"

	ConfigInsecureSkipVerify = "insecure_skip_verify"
	ConfigTLSCA              = "tls_ca"
	ConfigTLSCert            = "tls_cert"
	ConfigTLSKey             = "tls_key"

	StatsNCPU              = "n_cpus"
	StatsNFd               = "n_used_file_descriptors"
	StatsContainers        = "n_containers"
	StatsContainersRunning = "n_containers_running"
	StatsContainersStopped = "n_containers_stopped"
	StatsContainersPaused  = "n_containers_paused"
	StatsImages            = "n_images"
	StatsNGoroutines       = "n_goroutines"
	StatsNEventsListener   = "n_listener_events"
	StatsMemoryTotal       = "memory_total"
	StatsPoolBlocksize     = "pool_blocksize"
	StatsOOMKilled         = "oomkilled"
	StatsPid               = "pid"
	StatsExitCode          = "exitcode"
	StatsHealthStatus      = "health_status"
	StatsFailingStreak     = "failing_streak"

	// memstats
	StatsActiveAnon              = "active_anon"
	StatsActiveFile              = "active_file"
	StatsCache                   = "cache"
	StatsHierarchicalMemoryLimit = "hierarchical_memory_limit"
	StatsInactiveAnon            = "inactive_anon"
	StatsInactiveFile            = "inactive_file"
	StatsMappedFile              = "mapped_file"
	StatsPgFault                 = "pgfault"
	StatsPgMajFault              = "pgmajfault"
	StatsPgPgIn                  = "pgpgin"
	StatsPgPgOut                 = "pgpgout"
	StatsRss                     = "rss"
	StatsRssHuge                 = "rss_huge"
	StatsUnevictable             = "unevictable"
	StatsWriteback               = "writeback"

	StatsTotalActiveAnon   = "total_active_anon"
	StatsTotalActiveFile   = "total_active_file"
	StatsTotalCache        = "total_cache"
	StatsTotalInactiveAnon = "total_inactive_anon"
	StatsTotalInaciveFile  = "total_inactive_file"
	StatsTotalMappedFile   = "total_mapped_file"
	StatsTotalPgFault      = "total_pgfault"
	StatsTotalPgMajFault   = "total_pgmajfault"
	StatsTotalPgPgIn       = "total_pgpgin"
	StatsTotalPgPgOut      = "total_pgpgout"
	StatsTotalRss          = "total_rss"
	StatsTotalRssHuge      = "total_rss_huge"
	StatsTotalUnevictable  = "total_unevictable"
	StatsTotalWriteback    = "total_writeback"

	StatsFailCount = "fail_count"
	StatsLimit     = "limit"
	StatsMaxUsage  = "max_usage"
	StatsUsage     = "usage"

	StatsWindowsCommitBytes      = "commit_bytes"
	StatsWindowsCommitPeakBytes  = "commit_peak_bytes"
	StatsWindowsPrivatWorkingSet = "private_working_set"

	// swarm info
	StatsTasksRunning  = "tasks_running"
	StatsWTasksDesired = "tasks_desired"

	// cpu
	StatsUsageTotal                 = "usage_total"
	StatsUsageInUsermode            = "usage_in_usermode"
	StatsUsageInKernalmode          = "usage_in_kernelmode"
	StatsUsageSystem                = "usage_system"
	StatsThrottlingPeriods          = "throttling_periods"
	StatsThrottlingThrottledPeriods = "throttling_throttled_periods"
	StatsThrottlingThrottledTime    = "throttling_throttled_time"
	StatsContainerId                = "container_id"
	StatsUsagePercent               = "usage_percent"

	// net
	StatsRxDropped = "rx_dropped"
	StatsRxBytes   = "rx_bytes"
	StatsRxErrors  = "rx_errors"
	StatsTxPackets = "tx_packets"
	StatsTxDropped = "tx_dropped"
	StatsRxPackets = "rx_packets"
	StatsTxErrors  = "tx_errors"
	StatsTxBytes   = "tx_bytes"

	// block io stats
	StatsIOTimeRecursive  = "io_time_recursive"
	StatsSectorsRecursive = "sectors_recursive"
)

func init() {
	telegraf.AddUsage(MetricName, "Docker(docker)")
	telegraf.AddConfig(MetricName, map[string]interface{}{
		metric.OptionString: []Option{
			{
				KeyName:      ConfigEndpoint,
				ChooseOnly:   false,
				Default:      `unix:///var/run/docker.sock`,
				Placeholder:  `tcp://[ip]:[port]`,
				DefaultNoUse: true,
				Description:  "连接地址(支持填写环境变量)",
				Type:         metric.ConfigTypeString,
			},
			{
				KeyName:       ConfigGatherServices,
				ChooseOnly:    true,
				ChooseOptions: []interface{}{"true", "false"},
				Default:       false,
				DefaultNoUse:  false,
				Description:   "是否收集Swarm metrics(desired_replicas, running_replicas)",
				Type:          metric.ConfigTypeBool,
			},
			{
				KeyName:      ConfigContainerNames,
				ChooseOnly:   false,
				Default:      ``,
				DefaultNoUse: true,
				Description:  "只收集指定 containers 的监控信息, 为空则收集所有 containers 的监控信息(逗号分隔多个)",
				Type:         metric.ConfigTypeString,
			},
			{
				KeyName:      ConfigContainerNameInclude,
				ChooseOnly:   false,
				Default:      ``,
				DefaultNoUse: true,
				Description:  "指定收集的 container 名称，支持通配符, 为空则默认为所有 containers(逗号分隔多个)",
				Type:         metric.ConfigTypeString,
			},
			{
				KeyName:      ConfigContainerNameExclude,
				ChooseOnly:   false,
				Default:      ``,
				DefaultNoUse: true,
				Description:  "指定不需要收集的 container 名称，支持通配符, 默认为空 (逗号分隔多个)",
				Type:         metric.ConfigTypeString,
			},
			{
				KeyName:      ConfigContainerStateInclude,
				ChooseOnly:   false,
				Default:      ``,
				DefaultNoUse: true,
				Description:  "指定收集的container state，支持通配符，为空默认为 running",
				Type:         metric.ConfigTypeString,
			},
			{
				KeyName:      ConfigContainerStateExclude,
				ChooseOnly:   false,
				Default:      ``,
				DefaultNoUse: true,
				Description:  "指定不需要收集的container state，支持通配符，默认为空",
				Type:         metric.ConfigTypeString,
			},
			{
				KeyName:       ConfigPerDevice,
				ChooseOnly:    true,
				ChooseOptions: []interface{}{"true", "false"},
				Default:       true,
				DefaultNoUse:  false,
				Description:   "是否收集每个 container 的 blkio (8:0, 8:1...) 和  network (eth0, eth1, ...) stats",
				Type:          metric.ConfigTypeBool,
			},
			{
				KeyName:       ConfigTotal,
				ChooseOnly:    true,
				ChooseOptions: []interface{}{"true", "false"},
				Default:       false,
				DefaultNoUse:  false,
				Description:   "是否收集每个 container 总的 blkio 和 network stats",
				Type:          metric.ConfigTypeBool,
			},

			{
				KeyName:       ConfigInsecureSkipVerify,
				ChooseOnly:    true,
				ChooseOptions: []interface{}{"true", "false"},
				Default:       false,
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
			{StatsNCPU, "docker可用的系统逻辑cpu的个数", ""},
			{StatsNFd, "docker正在使用的文件描述符的个数", ""},
			{StatsContainers, "containers个数", ""},
			{StatsContainersRunning, "running containers个数", ""},
			{StatsContainersStopped, "stopped containers个数", ""},
			{StatsContainersPaused, "paused containers个数", ""},
			{StatsImages, "本地镜像个数", ""},
			{StatsNGoroutines, "goroutine个数", ""},
			{StatsNEventsListener, "当前连接到docker的listener个数", ""},
			{StatsMemoryTotal, "所有container使用的内存总数", ""},
			{StatsPoolBlocksize, "pool_blocksize", ""},
			{StatsOOMKilled, "oomkilled", ""},
			{StatsPid, "pid", ""},
			{StatsExitCode, "exitcode", ""},
			{StatsHealthStatus, "health_status", ""},
			{StatsFailingStreak, "failing_streak", ""},

			// memstats
			{StatsActiveAnon, "active_anon", ""},
			{StatsActiveFile, "active_file", ""},
			{StatsCache, "cache", ""},
			{StatsHierarchicalMemoryLimit, "hierarchical_memory_limit", ""},
			{StatsInactiveAnon, "inactive_anon", ""},
			{StatsInactiveFile, "inactive_file", ""},
			{StatsMappedFile, "mapped_file", ""},
			{StatsPgFault, "pgfault", ""},
			{StatsPgMajFault, "pgmajfault", ""},
			{StatsPgPgIn, "pgpgin", ""},
			{StatsPgPgOut, "pgpgout", ""},
			{StatsRss, "rss", ""},
			{StatsRssHuge, "rss_huge", ""},
			{StatsUnevictable, "unevictable", ""},
			{StatsWriteback, "writeback", ""},

			{StatsTotalActiveAnon, "total_active_anon", ""},
			{StatsTotalActiveFile, "total_active_file", ""},
			{StatsTotalCache, "total_cache", ""},
			{StatsTotalInactiveAnon, "total_inactive_anon", ""},
			{StatsTotalInaciveFile, "total_inactive_file", ""},
			{StatsTotalMappedFile, "total_mapped_file", ""},
			{StatsTotalPgFault, "total_pgfault", ""},
			{StatsTotalPgMajFault, "total_pgmajfault", ""},
			{StatsTotalPgPgIn, "total_pgpgin", ""},
			{StatsTotalPgPgOut, "total_pgpgout", ""},
			{StatsTotalRss, "total_rss", ""},
			{StatsTotalRssHuge, "total_rss_huge", ""},
			{StatsTotalUnevictable, "total_unevictable", ""},
			{StatsTotalWriteback, "total_writeback", ""},

			{StatsFailCount, "fail_count", ""},
			{StatsLimit, "limit", ""},
			{StatsMaxUsage, "max_usage", ""},
			{StatsUsage, "usage", ""},
			{StatsUsagePercent, "usage_percent", ""},
			{StatsWindowsCommitBytes, "commit_bytes", ""},
			{StatsWindowsCommitPeakBytes, "commit_peak_bytes", ""},
			{StatsWindowsPrivatWorkingSet, "private_working_set", ""},

			// memstats
			{StatsActiveAnon, "active_anon", ""},
			{StatsActiveFile, "active_file", ""},
			{StatsCache, "cache", ""},
			{StatsHierarchicalMemoryLimit, "hierarchical_memory_limit", ""},
			{StatsInactiveAnon, "inactive_anon", ""},
			{StatsInactiveFile, "inactive_file", ""},
			{StatsMappedFile, "mapped_file", ""},
			{StatsPgFault, "pgfault", ""},
			{StatsPgMajFault, "pgmajfault", ""},
			{StatsPgPgIn, "pgpgin", ""},
			{StatsPgPgOut, "pgpgout", ""},
			{StatsRss, "rss", ""},
			{StatsRssHuge, "rss_huge", ""},
			{StatsUnevictable, "unevictable", ""},
			{StatsWriteback, "writeback", ""},

			{StatsTotalActiveAnon, "total_active_anon", ""},
			{StatsTotalActiveFile, "total_active_file", ""},
			{StatsTotalCache, "total_cache", ""},
			{StatsTotalInactiveAnon, "total_inactive_anon", ""},
			{StatsTotalInaciveFile, "total_inactive_file", ""},
			{StatsTotalMappedFile, "total_mapped_file", ""},
			{StatsTotalPgFault, "total_pgfault", ""},
			{StatsTotalPgMajFault, "total_pgmajfault", ""},
			{StatsTotalPgPgIn, "total_pgpgin", ""},
			{StatsTotalPgPgOut, "total_pgpgout", ""},
			{StatsTotalRss, "total_rss", ""},
			{StatsTotalRssHuge, "total_rss_huge", ""},
			{StatsTotalUnevictable, "total_unevictable", ""},
			{StatsTotalWriteback, "total_writeback", ""},

			{StatsFailCount, "fail_count", ""},
			{StatsLimit, "limit", ""},
			{StatsMaxUsage, "max_usage", ""},
			{StatsUsage, "usage", ""},
			//StatsUsagePercent = "usage_percent" // 和cpu字段冲突

			{StatsWindowsCommitBytes, "commit_bytes", ""},
			{StatsWindowsCommitPeakBytes, "commit_peak_bytes", ""},
			{StatsWindowsPrivatWorkingSet, "private_working_set", ""},

			// swarm info
			{StatsTasksRunning, "tasks_running", ""},
			{StatsWTasksDesired, "tasks_desired", ""},

			// cpu
			{StatsUsageTotal, "usage_total", ""},
			{StatsUsageInUsermode, "usage_in_usermode", ""},
			{StatsUsageInKernalmode, "usage_in_kernelmode", ""},
			{StatsUsageSystem, "usage_system", ""},
			{StatsThrottlingPeriods, "throttling_periods", ""},
			{StatsThrottlingThrottledPeriods, "throttling_throttled_periods", ""},
			{StatsThrottlingThrottledTime, "throttling_throttled_time", ""},
			{StatsContainerId, "container_id", ""},
			{StatsUsagePercent, "usage_percent", ""},

			// net
			{StatsRxDropped, "rx_dropped", ""},
			{StatsRxBytes, "rx_bytes", ""},
			{StatsRxErrors, "rx_errors", ""},
			{StatsTxPackets, "tx_packets", ""},
			{StatsTxDropped, "tx_dropped", ""},
			{StatsRxPackets, "rx_packets", ""},
			{StatsTxErrors, "tx_errors", ""},
			{StatsTxBytes, "tx_bytes", ""},

			// block io stats
			{StatsIOTimeRecursive, "io_time_recursive", ""},
			{StatsSectorsRecursive, "sectors_recursive", ""},
		},
	})
}

type collector struct {
	*telegraf.Collector
}

func (c *collector) SyncConfig(data map[string]interface{}, meta *reader.Meta) error {
	dc, ok := c.Input.(*docker.Docker)
	if !ok {
		return errors.New("unexpected docker type, want '*docker.Docker'")
	}

	endpoint, ok := data[ConfigEndpoint].(string)
	if !ok {
		return fmt.Errorf("key endpoint want as string,actual get %T\n", data[ConfigEndpoint])
	}
	dc.Endpoint = endpoint

	gatherServices, ok := data[ConfigGatherServices].(bool)
	if ok {
		dc.GatherServices = gatherServices
	}

	containerNameInclude, ok := data[ConfigContainerNameInclude].(string)
	if ok {
		containerNameInclude = strings.TrimSpace(containerNameInclude)
		if containerNameInclude != "" {
			dc.ContainerInclude = strings.Split(containerNameInclude, ",")
		}
	}
	containerStateInclude, ok := data[ConfigContainerStateInclude].(string)
	if ok {
		containerStateInclude = strings.TrimSpace(containerStateInclude)
		if containerStateInclude != "" {
			dc.ContainerStateInclude = strings.Split(containerStateInclude, ",")
		}
	}
	containerStateExclude, ok := data[ConfigContainerStateExclude].(string)
	if ok {
		containerStateExclude = strings.TrimSpace(containerStateExclude)
		if containerStateExclude != "" {
			dc.ContainerStateExclude = strings.Split(containerStateExclude, ",")
		}
	}
	containerNameExclude, ok := data[ConfigContainerNameExclude].(string)
	if ok {
		containerNameExclude = strings.TrimSpace(containerNameExclude)
		if containerNameExclude != "" {
			dc.ContainerExclude = strings.Split(containerNameExclude, ",")
		}
	}

	perDevice, ok := data[ConfigPerDevice].(bool)
	if !ok {
		// 默认为true
		perDevice = true
	}
	dc.PerDevice = perDevice

	total, ok := data[ConfigTotal].(bool)
	if ok {
		dc.Total = total
	}

	insecureSkipVerify, ok := data[ConfigInsecureSkipVerify].(bool)
	if ok {
		dc.InsecureSkipVerify = insecureSkipVerify

	}

	TLSCA, ok := data[ConfigTLSCA].(string)
	if ok {
		dc.TLSCA = TLSCA
	}
	TLSCert, ok := data[ConfigTLSCert].(string)
	if ok {
		dc.TLSCert = TLSCert
	}
	TLSKey, ok := data[ConfigTLSKey].(string)
	if ok {
		dc.TLSKey = TLSKey
	}

	return nil
}

// NewCollector creates a new Elasticsearch collector.
func NewCollector() metric.Collector {
	input := inputs.Inputs[MetricName]()
	if _, err := toml.Decode(input.SampleConfig(), input); err != nil {
		log.Warnf("metric: failed to decode sample config of docker: %v", err)
	}
	return &collector{telegraf.NewCollector(MetricName, input)}
}

func init() {
	metric.Add(MetricName, NewCollector)
}
