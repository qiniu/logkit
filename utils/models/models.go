package models

import (
	"fmt"
	"sync/atomic"

	"github.com/qiniu/logkit/conf"
)

const (
	GlobalKeyName = "name"
	ExtraInfo     = "extra_info"
	/* 该选项兼容如下配置 KeyPandoraExtraInfo */

	KeyCore     = "core"
	KeyHostName = "hostname"
	KeyOsInfo   = "osinfo"
	KeyLocalIp  = "localip"

	ContentTypeHeader     = "Content-Type"
	ContentEncodingHeader = "Content-Encoding"

	ApplicationJson = "application/json"
	TestPlain       = "text/plain"
	ApplicationGzip = "application/gzip"

	KeyPandoraStash      = "pandora_stash"       // 当只有一条数据且 sendError 时候，将其转化为 raw 发送到 pandora_stash 这个字段
	KeyPandoraSeparateId = "pandora_separate_id" // 当一条数据大于2M且 sendError 时候，将其切片，切片记录到 pandora_separate_id 这个字段

	SchemaFreeTokensPrefix = "schema_free_tokens_"
	LogDBTokensPrefix      = "logdb_tokens_"
	TsDBTokensPrefix       = "tsdb_tokens_"
	KodoTokensPrefix       = "kodo_tokens_"

	KeyRunnerName = "runner_name"

	DefaultDirPerm  = 0755
	DefaultFilePerm = 0600

	DefaultMaxBatchSize = 2 * 1024 * 1024

	Text     = "text"
	Checkbox = "checkbox"
	Radio    = "radio"
)

type Option struct {
	KeyName       string
	ChooseOnly    bool
	Element       string
	ChooseOptions []interface{}
	Default       interface{}
	DefaultNoUse  bool
	Description   string
	CheckRegex    string
	Style         string `json:"style"`
	Required      bool   `json:"required"`
	Placeholder   string `json:"placeholder"`
	Type          string `json:"Type,omitempty"`
	Secret        bool
	Advance       bool   `json:"advance,omitempty"`
	AdvanceDepend string `json:"advance_depend,omitempty"`
	ToolTip       string `json:"tooltip,omitempty"`
	ToolTipActive bool   `json:"tooltip_active,omitempty"`
}

type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Data store as use key/value map
type Data map[string]interface{}

type AuthTokens struct {
	RunnerName   string
	SenderIndex  int
	SenderTokens conf.MapConf
}

type LagInfo struct {
	Size     int64  `json:"size"`
	SizeUnit string `json:"sizeunit"`
	Ftlags   int64  `json:"ftlags"`
	Total    int64  `json:"total"`
}

type StatsError struct {
	StatsInfo
	ErrorDetail         error `json:"error"`
	Ft                  bool  `json:"-"`
	FtNotRetry          bool  `json:"-"`
	DatasourceSkipIndex []int
	RemainDatas         []Data
}

type StatsInfo struct {
	Errors     int64   `json:"errors"`
	Success    int64   `json:"success"`
	Speed      float64 `json:"speed"`
	Trend      string  `json:"trend"`
	LastError  string  `json:"last_error"`
	FtQueueLag int64   `json:"-"`
}

func (se *StatsError) AddSuccess() {
	if se == nil {
		return
	}
	atomic.AddInt64(&se.Success, 1)
}

func (se *StatsError) AddSuccessNum(n int) {
	if se == nil {
		return
	}
	atomic.AddInt64(&se.Success, int64(n))
}

func (se *StatsError) AddErrors() {
	if se == nil {
		return
	}
	atomic.AddInt64(&se.Errors, 1)
}

func (se *StatsError) AddErrorsNum(n int) {
	if se == nil {
		return
	}
	atomic.AddInt64(&se.Errors, int64(n))
}

func (se *StatsError) Error() string {
	if se == nil {
		return ""
	}
	return fmt.Sprintf("success %v errors %v errordetail %v", se.Success, se.Errors, se.ErrorDetail)
}

func (se *StatsError) ErrorIndexIn(idx int) bool {
	for _, v := range se.DatasourceSkipIndex {
		if v == idx {
			return true
		}
	}
	return false
}
