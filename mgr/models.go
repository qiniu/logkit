package mgr

import (
	"time"

	"github.com/qiniu/logkit/audit"
	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/router"
	"github.com/qiniu/logkit/utils/equeue"
	. "github.com/qiniu/logkit/utils/models"
)

// RunnerStatus runner运行状态，添加字段请在clone函数中相应添加
type RunnerStatus struct {
	Name             string               `json:"name"`
	Logpath          string               `json:"logpath"`
	ReadDataSize     int64                `json:"readDataSize"`
	ReadDataCount    int64                `json:"readDataCount"`
	Elaspedtime      float64              `json:"elaspedtime"`
	Lag              LagInfo              `json:"lag"`
	ReaderStats      StatsInfo            `json:"readerStats"`
	ParserStats      StatsInfo            `json:"parserStats"`
	SenderStats      map[string]StatsInfo `json:"senderStats"`
	TransformStats   map[string]StatsInfo `json:"transformStats"`
	Error            string               `json:"error,omitempty"`
	lastState        time.Time
	ReadSpeedKB      int64  `json:"readspeed_kb"`
	ReadSpeed        int64  `json:"readspeed"`
	ReadSpeedTrendKb string `json:"readspeedtrend_kb"`
	ReadSpeedTrend   string `json:"readspeedtrend"`
	RunningStatus    string `json:"runningStatus"`
	Tag              string `json:"tag,omitempty"`
	Url              string `json:"url,omitempty"`

	//仅作为将history error同步上传到服务端时使用
	HistorySyncErrors CompatibleErrorResult `json:"history_errors"`
}

//Clone 复制出一个完整的RunnerStatus
func (src *RunnerStatus) Clone() RunnerStatus {
	dst := RunnerStatus{}
	dst.TransformStats = make(map[string]StatsInfo, len(src.TransformStats))
	dst.SenderStats = make(map[string]StatsInfo, len(src.SenderStats))
	for k, v := range src.SenderStats {
		dst.SenderStats[k] = v
	}
	for k, v := range src.TransformStats {
		dst.TransformStats[k] = v
	}
	dst.ParserStats = src.ParserStats
	dst.ReaderStats = src.ReaderStats
	dst.ReadDataSize = src.ReadDataSize
	dst.ReadDataCount = src.ReadDataCount
	dst.ReadSpeedKB = src.ReadSpeedKB
	dst.ReadSpeed = src.ReadSpeed
	dst.ReadSpeedTrendKb = src.ReadSpeedTrendKb
	dst.ReadSpeedTrend = src.ReadSpeedTrend

	dst.Name = src.Name
	dst.Logpath = src.Logpath

	dst.Elaspedtime = src.Elaspedtime
	dst.Lag = src.Lag

	dst.Error = src.Error
	dst.lastState = src.lastState

	dst.RunningStatus = src.RunningStatus
	dst.Tag = src.Tag
	dst.Url = src.Url
	return dst
}

// RunnerConfig 从多数据源读取，经过解析后，发往多个数据目的地
type RunnerConfig struct {
	RunnerInfo
	SourceData    string                   `json:"sourceData,omitempty"`
	MetricConfig  []MetricConfig           `json:"metric,omitempty"`
	ReaderConfig  conf.MapConf             `json:"reader"`
	CleanerConfig conf.MapConf             `json:"cleaner,omitempty"`
	ParserConf    conf.MapConf             `json:"parser"`
	Transforms    []map[string]interface{} `json:"transforms,omitempty"`
	SendersConfig []conf.MapConf           `json:"senders"`
	Router        router.RouterConfig      `json:"router,omitempty"`
	IsInWebFolder bool                     `json:"web_folder,omitempty"`
	IsStopped     bool                     `json:"is_stopped,omitempty"`
	IsFromServer  bool                     `json:"from_server,omitempty"` // 判读是否从服务器拉取的配置
	AuditChan     chan<- audit.Message     `json:"-"`
}

type RunnerInfo struct {
	RunnerName             string `json:"name"`
	Note                   string `json:"note,omitempty"`
	CollectInterval        int    `json:"collect_interval,omitempty"`           // metric runner收集的频率
	MaxBatchLen            int    `json:"batch_len,omitempty"`                  // 每个read batch的行数
	MaxBatchSize           int    `json:"batch_size,omitempty"`                 // 每个read batch的字节数
	MaxBatchInterval       int    `json:"batch_interval,omitempty"`             // 最大发送时间间隔
	MaxBatchTryTimes       int    `json:"batch_try_times,omitempty"`            // 最大发送次数，小于等于0代表无限重试
	MaxReaderCloseWaitTime int    `json:"max_reader_close_wait_time,omitempty"` // runner 等待reader close时间，
	ErrorsListCap          int    `json:"errors_list_cap"`                      // 记录错误信息的最大条数
	SyncEvery              int    `json:"sync_every,omitempty"`                 // 每多少次sync一下，填小于的0数字表示stop时sync，正整数表示发送成功多少次以后同步，填0或1就是每次发送成功都同步，兼容原来不配置的逻辑
	CreateTime             string `json:"createtime"`
	EnvTag                 string `json:"env_tag,omitempty"` // 用这个字段的值来获取环境变量, 作为 tag 添加到数据中
	ExtraInfo              bool   `json:"extra_info"`
	LogAudit               bool   `json:"log_audit"`
	SendRaw                bool   `json:"send_raw"`            //使用发送原始字符串的接口，而不是Data
	ReadTime               bool   `json:"read_time"`           // 读取时间
	InternalKeyPrefix      string `json:"internal_key_prefix"` // 内置字段名前缀
}

type ErrorsList struct {
	ReadErrors      *equeue.ErrorQueue            `json:"read_errors"`
	ParseErrors     *equeue.ErrorQueue            `json:"parse_errors"`
	TransformErrors map[string]*equeue.ErrorQueue `json:"transform_errors"`
	SendErrors      map[string]*equeue.ErrorQueue `json:"send_errors"`
}

type ErrorsResult struct {
	ReadErrors      []equeue.ErrorInfo            `json:"read_errors"`
	ParseErrors     []equeue.ErrorInfo            `json:"parse_errors"`
	TransformErrors map[string][]equeue.ErrorInfo `json:"transform_errors"`
	SendErrors      map[string][]equeue.ErrorInfo `json:"send_errors"`
}

//为了兼容之前的消息传递是errorqueue的结构
type CompatibleErrorResult struct {
	ReadErrors      *ErrorStatistic            `json:"read_errors"`
	ParseErrors     *ErrorStatistic            `json:"parse_errors"`
	TransformErrors map[string]*ErrorStatistic `json:"transform_errors"`
	SendErrors      map[string]*ErrorStatistic `json:"send_errors"`
}

func NewErrorsList() *ErrorsList {
	return &ErrorsList{
		TransformErrors: make(map[string]*equeue.ErrorQueue),
		SendErrors:      make(map[string]*equeue.ErrorQueue),
	}
}

//Reset 清空列表
func (list *ErrorsList) Reset() {
	list.ReadErrors = nil
	list.ParseErrors = nil
	list.TransformErrors = nil
	list.SendErrors = nil
}

//List 复制出一个顺序的 Errors
func (list *ErrorsList) List() (dst ErrorsResult) {
	if list.Empty() {
		return ErrorsResult{}
	}
	dst = ErrorsResult{
		ReadErrors:  list.ReadErrors.List(),
		ParseErrors: list.ParseErrors.List(),
	}
	if list.TransformErrors != nil {
		dst.TransformErrors = make(map[string][]equeue.ErrorInfo)
	}
	for name, transformQueue := range list.TransformErrors {
		dst.TransformErrors[name] = transformQueue.List()
	}
	if list.SendErrors != nil {
		dst.SendErrors = make(map[string][]equeue.ErrorInfo)
	}
	for name, sendQueue := range list.SendErrors {
		dst.SendErrors[name] = sendQueue.List()
	}
	return dst
}

// Empty 检查列表是否为空
func (list *ErrorsList) Empty() bool {
	if list == nil {
		return true
	}
	if list.HasReadErr() {
		return false
	}
	if list.HasParseErr() {
		return false
	}
	if list.HasTransformErr() {
		return false
	}
	if list.HasSendErr() {
		return false
	}
	return true
}

func (list *ErrorsList) HasReadErr() bool {
	if list == nil {
		return false
	}
	return !list.ReadErrors.Empty()
}

func (list *ErrorsList) HasParseErr() bool {
	if list == nil {
		return false
	}
	return !list.ParseErrors.Empty()
}

func (list *ErrorsList) HasSendErr() bool {
	if list == nil {
		return false
	}
	for _, v := range list.SendErrors {
		if !v.Empty() {
			return true
		}
	}
	return false
}

func (list *ErrorsList) HasTransformErr() bool {
	if list == nil {
		return false
	}
	for _, v := range list.TransformErrors {
		if !v.Empty() {
			return true
		}
	}
	return false
}

// Clone 返回当前 ErrorList 的完整拷贝，若无数据则会返回 nil
func (list *ErrorsList) Clone() *ErrorsList {
	if list.Empty() {
		return nil
	}
	dst := &ErrorsList{
		ReadErrors:  list.ReadErrors.Clone(),
		ParseErrors: list.ParseErrors.Clone(),
	}
	if len(list.TransformErrors) > 0 {
		dst.TransformErrors = make(map[string]*equeue.ErrorQueue)
	}
	for name, transformErrors := range list.TransformErrors {
		dst.TransformErrors[name] = transformErrors.Clone()
	}

	if len(list.SendErrors) > 0 {
		dst.SendErrors = make(map[string]*equeue.ErrorQueue)
	}
	for name, sendErrors := range list.SendErrors {
		dst.SendErrors[name] = sendErrors.Clone()
	}
	return dst
}
