package mgr

import (
	"time"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/router"
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
	ReadSpeedKB      float64     `json:"readspeed_kb"`
	ReadSpeed        float64     `json:"readspeed"`
	ReadSpeedTrendKb string      `json:"readspeedtrend_kb"`
	ReadSpeedTrend   string      `json:"readspeedtrend"`
	RunningStatus    string      `json:"runningStatus"`
	Tag              string      `json:"tag,omitempty"`
	Url              string      `json:"url,omitempty"`
	HistoryErrors    *ErrorsList `json:"history_errors"`
	RunningError     string      `json:"runningError"`
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
	if src.HistoryErrors != nil {
		dst.HistoryErrors = src.HistoryErrors.Clone()
	}
	return dst
}

// RunnerConfig 从多数据源读取，经过解析后，发往多个数据目的地
type RunnerConfig struct {
	RunnerInfo
	SourceData    string                   `json:"sourceData, omitempty"`
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
	ErrorInfo     string                   `json:"error_info"`
}

type RunnerInfo struct {
	RunnerName       string `json:"name"`
	Note             string `json:"note,omitempty"`
	CollectInterval  int    `json:"collect_interval,omitempty"` // metric runner收集的频率
	MaxBatchLen      int    `json:"batch_len,omitempty"`        // 每个read batch的行数
	MaxBatchSize     int    `json:"batch_size,omitempty"`       // 每个read batch的字节数
	MaxBatchInterval int    `json:"batch_interval,omitempty"`   // 最大发送时间间隔
	MaxBatchTryTimes int    `json:"batch_try_times,omitempty"`  // 最大发送次数，小于等于0代表无限重试
	ErrorsListCap    int    `json:"errors_list_cap"`            // 记录错误信息的最大条数
	CreateTime       string `json:"createtime"`
	EnvTag           string `json:"env_tag,omitempty"`
	ExtraInfo        bool   `json:"extra_info"`
	// 用这个字段的值来获取环境变量, 作为 tag 添加到数据中
}

type ErrorsList struct {
	ReadErrors      *ErrorQueue            `json:"read_errors"`
	ParseErrors     *ErrorQueue            `json:"parse_errors"`
	TransformErrors map[string]*ErrorQueue `json:"transform_errors"`
	SendErrors      map[string]*ErrorQueue `json:"send_errors"`
}

type ErrorsResult struct {
	ReadErrors      []ErrorInfo            `json:"read_errors"`
	ParseErrors     []ErrorInfo            `json:"parse_errors"`
	TransformErrors map[string][]ErrorInfo `json:"transform_errors"`
	SendErrors      map[string][]ErrorInfo `json:"send_errors"`
}

// 返回队列实际容量
func (list *ErrorsList) Reset() {
	list.ReadErrors = nil
	list.ParseErrors = nil
	list.TransformErrors = nil
	list.SendErrors = nil
}

// 复制出一个顺序的 Errors
func (list *ErrorsList) Sort() (dst ErrorsResult) {
	dst = ErrorsResult{}
	if list.ReadErrors != nil {
		dst.ReadErrors = list.ReadErrors.Sort()
	}
	if list.ParseErrors != nil {
		dst.ParseErrors = list.ParseErrors.Sort()
	}
	for transform, transformQueue := range list.TransformErrors {
		if dst.TransformErrors == nil {
			dst.TransformErrors = make(map[string][]ErrorInfo)
		}
		dst.TransformErrors[transform] = transformQueue.Sort()
	}
	for send, sendQueue := range list.SendErrors {
		if dst.SendErrors == nil {
			dst.SendErrors = make(map[string][]ErrorInfo)
		}
		dst.SendErrors[send] = sendQueue.Sort()
	}
	return dst
}

// Clone 返回当前 ErrorList 的完整拷贝，若无数据则会返回 nil
func (list *ErrorsList) Clone() *ErrorsList {
	var dst ErrorsList
	isEmpty := true
	if !list.ReadErrors.IsEmpty() {
		dst.ReadErrors = NewErrorQueue(list.ReadErrors.GetMaxSize())
		dst.ReadErrors.CopyQueue(list.ReadErrors)
		isEmpty = false
	}

	if !list.ParseErrors.IsEmpty() {
		dst.ParseErrors = NewErrorQueue(list.ParseErrors.GetMaxSize())
		dst.ParseErrors.CopyQueue(list.ParseErrors)
		isEmpty = false
	}

	if list.TransformErrors != nil {
		for transform, transformErrors := range list.TransformErrors {
			if !transformErrors.IsEmpty() {
				if dst.TransformErrors == nil {
					dst.TransformErrors = make(map[string]*ErrorQueue)
				}
				dst.TransformErrors[transform] = NewErrorQueue(transformErrors.GetMaxSize())
				dst.TransformErrors[transform].CopyQueue(transformErrors)
				isEmpty = false
			}
		}
	}

	if list.SendErrors != nil {
		for send, sendErrors := range list.SendErrors {
			if !sendErrors.IsEmpty() {
				if dst.SendErrors == nil {
					dst.SendErrors = make(map[string]*ErrorQueue)
				}
				dst.SendErrors[send] = NewErrorQueue(sendErrors.GetMaxSize())
				dst.SendErrors[send].CopyQueue(sendErrors)
				isEmpty = false
			}
		}
	}

	if isEmpty {
		return nil
	}
	return &dst
}
