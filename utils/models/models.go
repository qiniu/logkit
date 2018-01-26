package models

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
}

type KeyValue struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// Data store as use key/value map
type Data map[string]interface{}
