package models

import "github.com/qiniu/logkit/conf"

const (
	GlobalKeyName = "name"
	KeyCore       = "core"
	KeyHostName   = "hostname"
	KeyOsInfo     = "osinfo"
	KeyLocalIp    = "localip"

	ContentTypeHeader     = "Content-Type"
	ContentEncodingHeader = "Content-Encoding"

	ApplicationJson = "application/json"
	ApplicationGzip = "application/gzip"

	KeyPandoraStash = "pandora_stash" // 当只有一条数据且 sendError 时候，将其转化为 raw 发送到 pandora_stash 这个字段

	SchemaFreeTokensPrefix = "schema_free_tokens_"
	LogDBTokensPrefix      = "logdb_tokens_"
	TsDBTokensPrefix       = "tsdb_tokens_"
	KodoTokensPrefix       = "kodo_tokens_"

	DefaultDirPerm  = 0755
	DefaultFilePerm = 0600
)

type Option struct {
	KeyName       string
	ChooseOnly    bool
	ChooseOptions []interface{}
	Default       interface{}
	DefaultNoUse  bool
	Description   string
	CheckRegex    string
	Type          string `json:"Type,omitempty"`
	Secret        bool
	Advance       bool   `json:"advance,omitempty"`
	AdvanceDepend string `json:"advance_depend,omitempty"`
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
}
