package config

import (
	. "github.com/qiniu/logkit/utils/models"
)

// conf 字段
const (
	KeyParserName           = GlobalKeyName
	KeyParserType           = "type"
	KeyLabels               = "labels" // 额外增加的标签信息，比如机器信息等
	KeyDisableRecordErrData = "disable_record_errdata"
	KeyKeepRawData          = "keep_raw_data"
	KeyRawData              = "raw_data"
)

// parser 的类型
const (
	TypeCSV        = "csv"
	TypeLogv1      = "qiniulog"
	TypeKafkaRest  = "kafkarest"
	TypeRaw        = "raw"
	TypeEmpty      = "empty"
	TypeGrok       = "grok"
	TypeInnerSQL   = "_sql"
	TypeInnerMySQL = "_mysql"
	TypeJSON       = "json"
	TypeNginx      = "nginx"
	TypeSyslog     = "syslog"
	TypeMySQL      = "mysqllog"
	TypeLogfmt     = "logfmt"
	TypeKeyValue   = "KV"
	TypeLinuxAudit = "linuxaudit"
	TypeQPlayerQos = "qplayerqos"
)

// 数据常量类型
type DataType string

const (
	TypeFloat   DataType = "float"
	TypeLong    DataType = "long"
	TypeString  DataType = "string"
	TypeDate    DataType = "date"
	TypeJSONMap DataType = "jsonmap"
)
