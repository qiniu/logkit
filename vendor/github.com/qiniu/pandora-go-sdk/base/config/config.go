package config

import (
	"time"

	"github.com/qiniu/pandora-go-sdk/base"
)

type Config struct {
	Endpoint         string
	Ak               string
	Sk               string
	Logger           base.Logger
	DialTimeout      time.Duration
	ResponseTimeout  time.Duration
	RequestRateLimit int64 //每秒请求数限制
	FlowRateLimit    int64 //每秒流量限制(kb),若FlowRateLimit为100，则表示限速100KB/s
	Gzip             bool

	HeaderUserAgent string

	//以下是新版本，上面的Endpoint是老版本，都兼容，默认使用新版，新版为空则用老的Endpoint
	LogdbEndpoint    string
	TsdbEndpoint     string
	PipelineEndpoint string
	ReportEndpoint   string
	ConfigType       string
}

const (
	TypeLOGDB    = "logdb"
	TypePipeline = "pipeline"
	TypeTSDB     = "tsdb"
	TypeReport   = "report"
)

const (
	defaultDialTimeout     time.Duration = 10 * time.Second
	defaultResponseTimeout time.Duration = 30 * time.Second

	DefaultTSDBEndpoint     = "https://tsdb.qiniu.com"
	DefaultLogDBEndpoint    = "https://logdb.qiniu.com"
	DefaultPipelineEndpoint = "https://pipeline.qiniu.com"
	DefaultReportEndpoint   = "https://report.qiniu.com"
)

func NewConfig() *Config {
	return &Config{
		DialTimeout:     defaultDialTimeout,
		ResponseTimeout: defaultResponseTimeout,
	}
}

func (c *Config) Clone() *Config {
	return &Config{
		Endpoint:         c.Endpoint,
		Ak:               c.Ak,
		Sk:               c.Sk,
		Logger:           c.Logger,
		DialTimeout:      c.DialTimeout,
		ResponseTimeout:  c.ResponseTimeout,
		RequestRateLimit: c.RequestRateLimit,
		FlowRateLimit:    c.FlowRateLimit,
		Gzip:             c.Gzip,
		HeaderUserAgent:  c.HeaderUserAgent,

		//以下是新版本，上面的Endpoint是老版本，都兼容，默认使用新版，新版为空则用老的Endpoint
		LogdbEndpoint:    c.LogdbEndpoint,
		TsdbEndpoint:     c.TsdbEndpoint,
		PipelineEndpoint: c.PipelineEndpoint,
		ReportEndpoint:   c.ReportEndpoint,
		ConfigType:       c.ConfigType,
	}
}

func (c *Config) WithEndpoint(endpoint string) *Config {
	c.Endpoint = endpoint
	return c
}

func (c *Config) WithLogDBEndpoint(endpoint string) *Config {
	c.LogdbEndpoint = endpoint
	return c
}

func (c *Config) WithPipelineEndpoint(endpoint string) *Config {
	c.PipelineEndpoint = endpoint
	return c
}

func (c *Config) WithTSDBEndpoint(endpoint string) *Config {
	c.TsdbEndpoint = endpoint
	return c
}

func (c *Config) WithReportEndpoint(endpoint string) *Config {
	c.ReportEndpoint = endpoint
	return c
}

func (c *Config) WithAccessKeySecretKey(ak, sk string) *Config {
	c.Ak, c.Sk = ak, sk
	return c
}

func (c *Config) WithDialTimeout(t time.Duration) *Config {
	c.DialTimeout = t
	return c
}

func (c *Config) WithResponseTimeout(t time.Duration) *Config {
	c.ResponseTimeout = t
	return c
}

func (c *Config) WithLogger(l base.Logger) *Config {
	c.Logger = l
	return c
}

func (c *Config) WithLoggerLevel(level base.LogLevelType) *Config {
	c.Logger.SetLoggerLevel(level)
	return c
}

func (c *Config) WithRequestRateLimit(limit int64) *Config {
	c.RequestRateLimit = limit
	return c
}

func (c *Config) WithFlowRateLimit(limit int64) *Config {
	c.FlowRateLimit = limit
	return c
}

func (c *Config) WithGzipData(enable bool) *Config {
	c.Gzip = enable
	return c
}

func (c *Config) WithHeaderUserAgent(userAgent string) *Config {
	c.HeaderUserAgent = userAgent
	return c
}
