package config

import (
	. "github.com/qiniu/logkit/utils/models"
)

// Constants for csv
const (
	KeyCSVSchema             = "csv_schema"               // csv 每个列的列名和类型 long/string/float/date
	KeyCSVSplitter           = "csv_splitter"             // csv 的分隔符
	KeyCSVLabels             = "csv_labels"               // csv 额外增加的标签信息，比如机器信息等
	KeyCSVAutoRename         = "csv_auto_rename"          // 是否将不合法的字段名称重命名一下, 比如 header-host 重命名为 header_host
	KeyCSVAllowNoMatch       = "csv_allow_no_match"       // 允许实际分隔的数据和schema不相等，不相等时按顺序赋值
	KeyCSVAllowMore          = "csv_allow_more"           // 允许实际字段比schema多
	KeyCSVAllowMoreStartNum  = "csv_more_start_number"    // 允许实际字段比schema多，名称开始的数字
	KeyCSVIgnoreInvalidField = "csv_ignore_invalid"       // 忽略解析错误的字段
	KeyCSVContainSplitterKey = "csv_contain_splitter_key" // 包含分隔符的字段名
)

// Constants for logfmt/KV
const (
	KeySplitter   = "splitter" //logfmt/KV 的分隔符
	KeyKeepString = "keep_string"
)

// Constants for Grok
const (
	KeyGrokMode               = "grok_mode"     //是否替换\n以匹配多行
	KeyGrokPatterns           = "grok_patterns" // grok 模式串名
	KeyGrokCustomPatternFiles = "grok_custom_pattern_files"
	KeyGrokCustomPatterns     = "grok_custom_patterns"

	KeyTimeZoneOffset = "timezone_offset"
)

// Constants for Nginx
const (
	NginxSchema      = "nginx_schema"
	NginxConfPath    = "nginx_log_format_path"
	NginxLogFormat   = "nginx_log_format_name"
	NginxFormatRegex = "nginx_log_format_regex"
)

// Constants for Qiniu
const (
	KeyLogHeaders = "qiniulog_log_headers"
)

// Constants for raw
const (
	KeyRaw       = "raw"
	KeyTimestamp = "timestamp"
)

// Constants for syslog
const (
	KeyRFCType              = "syslog_rfc"
	KeySyslogMaxline        = "syslog_maxline"
	PandoraParseFlushSignal = "!@#pandora-EOF-line#@!"
)

// ModeUsages 和 ModeTooltips 用途说明
var (
	ModeUsages = KeyValueSlice{
		{TypeRaw, "原始日志逐行发送", ""},
		{TypeJSON, "json 格式解析", ""},
		{TypeNginx, "nginx 日志解析", ""},
		{TypeGrok, "grok 格式解析", ""},
		{TypeCSV, "csv 格式解析", ""},
		{TypeSyslog, "syslog 格式解析", ""},
		{TypeLogv1, "七牛日志库格式解析", ""},
		{TypeKafkaRest, "kafkarest 日志解析", ""},
		{TypeEmpty, "通过解析清空数据", ""},
		{TypeMySQL, "mysql 慢请求日志解析", ""},
		{TypeKeyValue, "key value 日志解析", ""},
		{TypeLinuxAudit, "redhat 审计日志解析", ""},
	}

	ModeToolTips = KeyValueSlice{
		{TypeRaw, "将日志文件的每一行解析为一条日志，解析后的日志由两个字段，raw和timestamp，前者是日志，后者为解析该条日志的时间戳。", ""},
		{TypeJSON, "通过json反序列化解析日志的方式。若日志的json格式不规范，则解析失败，解析失败的数据会被忽略。", ""},
		{TypeNginx, "是专门解析Nginx日志的解析器。仅需指定nginx的配置文件地址，即可进行nginx日志解析。", ""},
		{TypeGrok, "类似于Logstash Grok Parser一样的解析配置方式，其本质是按照正则表达式匹配解析日志。", ""},
		{TypeCSV, "按行读取日志，对于每一行，以分隔符分隔，然后通过csv_schema命名分隔出来的字段名称以及字段类型。默认情况下CSV是按\t分隔日志的，可以配置的分隔符包括但不限于, 各类字母、数字、特殊符号(#、!、*、@、%、^、...)等等。", ""},
		{TypeSyslog, " 是直接根据 RFC3164/RFC5424 规则解析syslog数据的解析器，使用该解析器请确保日志数据严格按照RFC协议规则配置，否则该解析器将无法正确解析。该解析器能够自动识别多行构成的同一条日志。", ""},
		{TypeLogv1, " 为使用了七牛开源的Golang日志库(https://github.com/qiniu/log) 生成的日志提供的解析方式。", ""},
		{TypeKafkaRest, "将Kafka Rest日志文件的每一行解析为一条结构化的日志.", ""},
		{TypeEmpty, "通过解析清空数据", ""},
		{TypeMySQL, "解析mysql的慢请求日志。", ""},
		{TypeKeyValue, "按照key value解析日志", ""},
		{TypeLinuxAudit, "按 redhat 审计日志解析", ""},
	}
)

var (
	OptionTimezoneOffset = Option{
		KeyName:    KeyTimeZoneOffset,
		ChooseOnly: true,
		Default:    "0",
		ChooseOptions: []interface{}{"0", "-1", "-2", "-3", "-4",
			"-5", "-6", "-7", "-8", "-9", "-10", "-11", "-12",
			"1", "2", "3", "4", "5", "6", "7", "8", "9", "11", "12"},
		DefaultNoUse: false,
		Description:  "时区偏移量(timezone_offset)",
		Advance:      true,
		ToolTip:      `若实际为东八区时间，读取为UTC时间，则实际多读取了8小时，选择"-8"，修正回CST中国北京时间。`,
	}

	OptionLabels = Option{
		KeyName:      KeyLabels,
		ChooseOnly:   false,
		Default:      "",
		DefaultNoUse: false,
		Description:  "额外的标签信息(labels)",
		Advance:      true,
		ToolTip:      `额外的标签信息，同样逗号分隔，如 "app logkit, user pandora"`,
	}

	OptionKeepRawData = Option{
		KeyName:       KeyKeepRawData,
		Element:       Radio,
		ChooseOnly:    true,
		ChooseOptions: []interface{}{"false", "true"},
		Default:       "false",
		DefaultNoUse:  false,
		Description:   "是否保留原始数据(" + KeyKeepRawData + ")",
		Advance:       true,
		ToolTip:       `默认不保留，保留后会在每条记录中新增"raw_data"字段，用以保存每条记录的原始数据`,
	}

	OptionDisableRecordErrData = Option{
		KeyName:       KeyDisableRecordErrData,
		Element:       Radio,
		ChooseOnly:    true,
		ChooseOptions: []interface{}{"false", "true"},
		Default:       "false",
		DefaultNoUse:  false,
		Description:   "禁止记录解析失败数据(disable_record_errdata)",
		Advance:       true,
		ToolTip:       `解析失败的数据会默认出现在"pandora_stash"字段，该选项可以禁止记录解析失败的数据`,
	}

	OptionParserName = Option{
		KeyName:      KeyParserName,
		ChooseOnly:   false,
		Default:      "parser",
		DefaultNoUse: false,
		Description:  "指定名称(name)",
		Advance:      true,
	}
	OptionKeepString = Option{
		KeyName:       KeyKeepString,
		Element:       Radio,
		ChooseOnly:    true,
		ChooseOptions: []interface{}{"false", "true"},
		Default:       "false",
		Advance:       true,
		DefaultNoUse:  false,
		Description:   "数字保持字符串形式",
		ToolTip:       "数字保持字符串形式",
	}
)

var ModeKeyOptions = map[string][]Option{
	TypeJSON: {
		OptionParserName,
		OptionLabels,
		OptionDisableRecordErrData,
		OptionKeepRawData,
	},
	TypeNginx: {
		{
			KeyName:      NginxConfPath,
			ChooseOnly:   false,
			Default:      "",
			Required:     false,
			Placeholder:  "/opt/nginx/conf/nginx.conf",
			DefaultNoUse: true,
			Description:  "nginx配置路径(nginx_log_format_path)",
			ToolTip:      `nginx配置文件，确认配置文件包含 log_format 中填写的格式`,
		},
		{
			KeyName:      NginxLogFormat,
			ChooseOnly:   false,
			Default:      "main",
			Placeholder:  "main",
			Required:     true,
			DefaultNoUse: true,
			Description:  "nginx日志格式名称(nginx_log_format_name)",
		},
		{
			KeyName:      NginxFormatRegex,
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "指定正则表达式解析(nginx_log_format_regex)",
			ToolTip:      "若根据配置文件自动生成的正则表达式无效，可通过此配置修改指定填写",
		},
		{
			KeyName:      NginxSchema,
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "手动指定字段类型(nginx_schema)",
			Advance:      true,
			ToolTip:      `nginx日志都被解析为string，指定该格式可以设置为float、long、date三种类型。如 time_local date,bytes_sent long,request_time float`,
		},
		OptionParserName,
		OptionLabels,
		OptionDisableRecordErrData,
		OptionKeepRawData,
	},
	TypeGrok: {
		{
			KeyName:      KeyGrokPatterns,
			ChooseOnly:   false,
			Default:      "%{COMMON_LOG_FORMAT}",
			Placeholder:  "%{COMMON_LOG_FORMAT}",
			Required:     true,
			DefaultNoUse: true,
			Description:  "匹配日志的grok表达式(grok_patterns)",
			ToolTip:      `用于匹配日志的grok表达式，多个用逗号分隔，如 "%{COMMON_LOG_FORMAT},%{QINIU_LOG_FORMAT}"`,
		},
		{
			KeyName:      KeyGrokCustomPatterns,
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "自定义grok表达式(grok_custom_patterns)",
		},
		{
			KeyName:      KeyGrokCustomPatternFiles,
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "自定义grok表达式文件路径(grok_custom_pattern_files)",
			Advance:      true,
			ToolTip:      `从机器获得自定义grok表达式文件`,
		},
		OptionParserName,
		OptionTimezoneOffset,
		OptionLabels,
		OptionDisableRecordErrData,
		OptionKeepRawData,
	},

	TypeCSV: {
		{
			KeyName:      KeyCSVSplitter,
			ChooseOnly:   false,
			Default:      ",",
			Placeholder:  ",",
			Required:     false,
			DefaultNoUse: false,
			Description:  "分隔符(csv_splitter)",
			ToolTip:      `csv_splitter csv文件的分隔符定义，默认为','`,
		},
		{
			KeyName:      KeyCSVSchema,
			ChooseOnly:   false,
			Default:      "",
			Required:     true,
			Placeholder:  "abc string,xyz long,data1 string,data2 float",
			DefaultNoUse: true,
			Description:  "指定字段类型(csv_schema)",
			ToolTip:      `按照逗号分隔的字符串，如"abc string"，字段类型现在支持string, long, jsonmap, float, date，如果连续的字段类型相同，可以用 | 分隔，例如"a|b string"，表示"a string,b string"`,
		},
		{
			KeyName:       KeyCSVAllowNoMatch,
			Element:       Radio,
			ChooseOnly:    true,
			Advance:       true,
			ChooseOptions: []interface{}{"false", "true"},
			Default:       "false",
			DefaultNoUse:  false,
			Description:   "允许日志数量可变(csv_allow_no_match)",
			ToolTip:       `允许schema定义的字段数量和实际日志分隔出来的数量不匹配，默认不允许`,
		},
		{
			KeyName:      KeyCSVAllowMore,
			Advance:      true,
			Default:      "unknown",
			DefaultNoUse: false,
			Description:  "多余字段命名(csv_allow_more)",
			ToolTip:      `允许日志数量比定义的schema多且命名为该字段后跟数字，如"unknown0, unknown1"`,
		},
		{
			KeyName:      KeyCSVAllowMoreStartNum,
			Advance:      true,
			Default:      "0",
			DefaultNoUse: false,
			Description:  "多余字段开始数字(csv_more_start_number)",
			ToolTip:      `多余字段命名数字后缀开始数字，默认如"unknown0", 填1则为"unknown1"`,
		},
		{
			KeyName:      KeyCSVContainSplitterKey,
			Advance:      true,
			Default:      "",
			DefaultNoUse: false,
			Description:  "包含分隔符的字段名",
			ToolTip:      `比如"name,subject,time"的表头，字段内容为: "luffy,One, Piece,2019-05-20" 'subject'字段中包含分隔符,所以填写'subject'则可避免解析出错"`,
		},
		{
			KeyName:       KeyCSVIgnoreInvalidField,
			Element:       Radio,
			ChooseOnly:    true,
			Advance:       true,
			ChooseOptions: []interface{}{"false", "true"},
			Default:       "false",
			DefaultNoUse:  false,
			Description:   "忽略解析错误的字段(csv_ignore_invalid)",
			ToolTip:       `忽略解析错误的部分，剩余部分继续发送`,
		},
		OptionParserName,
		OptionLabels,
		OptionTimezoneOffset,
		{
			KeyName:       KeyCSVAutoRename,
			Element:       Radio,
			ChooseOnly:    true,
			ChooseOptions: []interface{}{"true", "false"},
			Default:       "true",
			DefaultNoUse:  false,
			Description:   "将字段名称中的'-'更改为'_'",
			Advance:       true,
		},
		OptionDisableRecordErrData,
		OptionKeepRawData,
	},
	TypeRaw: {
		{
			KeyName:       KeyTimestamp,
			Element:       Radio,
			ChooseOnly:    true,
			ChooseOptions: []interface{}{"true", "false"},
			Default:       "true",
			DefaultNoUse:  false,
			Description:   "添加系统时间戳(timestamp)",
		},
		OptionParserName,
		OptionLabels,
		OptionDisableRecordErrData,
	},
	TypeLogv1: {
		OptionLabels,
		{
			KeyName:      KeyLogHeaders,
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "日志字段与顺序(qiniulog_log_headers)",
			ToolTip:      "用来定义解析的顺序，默认情况下没有prefix，按 date,time,reqid,level,module,file,log的顺序依次解析，可以调换。还可以写 combinedReqidLevel 表示reqid和level组合，代表reqid可能有、可能没有，但是如果都有，前者被认定为一定reqid；combinedModuleFile表示module和file的组合，代表可能有module，可能没有，如果存在中括号开头就认为是module",
			Advance:      true,
		},
		OptionParserName,
		OptionDisableRecordErrData,
		OptionKeepRawData,
	},
	TypeSyslog: {
		{
			KeyName:       KeyRFCType,
			ChooseOnly:    true,
			Default:       "automic",
			ChooseOptions: []interface{}{"automic", "rfc3164", "rfc5424", "rfc6587"},
			DefaultNoUse:  false,
			Description:   "rfc协议(syslog_rfc)",
		},
		{
			KeyName:      KeySyslogMaxline,
			ChooseOnly:   false,
			Default:      "100",
			DefaultNoUse: false,
			Description:  "最大读取行数(syslog_maxline)",
			Advance:      true,
		},
		OptionParserName,
		OptionLabels,
		OptionDisableRecordErrData,
		OptionKeepRawData,
		OptionTimezoneOffset,
	},
	TypeKafkaRest: {
		OptionParserName,
		OptionLabels,
		OptionDisableRecordErrData,
		OptionKeepRawData,
	},
	TypeEmpty: {},
	TypeMySQL: {
		OptionParserName,
		OptionLabels,
		OptionDisableRecordErrData,
		OptionKeepRawData,
	},
	TypeLogfmt: {
		{
			KeyName:      KeySplitter,
			ChooseOnly:   false,
			Default:      "=",
			DefaultNoUse: false,
			Description:  "分隔符(splitter)",
		},
		OptionKeepString,
		OptionParserName,
		OptionDisableRecordErrData,
		OptionKeepRawData,
	},
	TypeKeyValue: {
		{
			KeyName:      KeySplitter,
			ChooseOnly:   false,
			Default:      "=",
			DefaultNoUse: false,
			Description:  "分隔符(splitter)",
		},
		OptionKeepString,
		OptionParserName,
		OptionDisableRecordErrData,
		OptionKeepRawData,
	},
	TypeLinuxAudit: {
		OptionParserName,
		OptionDisableRecordErrData,
		OptionKeepRawData,
	},
}

// SampleLogs 样例日志，用于前端界面试玩解析器
var SampleLogs = map[string]string{
	TypeNginx: `110.110.101.101 - - [21/Mar/2017:18:14:17 +0800] "GET /files/yyyysx HTTP/1.1" 206 607 1 "-" "Apache-HttpClient/4.4.1 (Java/1.7.0_80)" "-" "122.121.111.222, 122.121.111.333, 192.168.90.61" "192.168.42.54:5000" www.qiniu.com llEAAFgmnoIa3q0U "0.040" 0.040 760 "-" "-" - - QCloud`,
	TypeGrok: `127.0.0.1 user-identifier frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326
123.45.12.1 user-identifier bob [10/Oct/2013:13:55:36 -0700] "GET /hello.gif HTTP/1.0" 200 2326`,
	TypeJSON:      `{"a":"b","c":1,"d":1.1}`,
	TypeCSV:       `a,123,bcd,1.2`,
	TypeRaw:       `raw log1[05-May-2017 13:44:39]  [pool log] pid 4109`,
	TypeSyslog:    `<38>Feb 05 01:02:03 abc system[253]: Listening at 0.0.0.0:3000`,
	TypeLogv1:     `2016/10/20 17:30:21.433423 [GE2owHck-Y4IWJHS][WARN] github.com/qiniu/http/rpcutil.v1/rpc_util.go:203: E18102: The specified repo does not exist under the provided appid ~`,
	TypeKafkaRest: `[2016-12-05 03:35:20,682] INFO 172.16.16.191 - - [05/Dec/2016:03:35:20 +0000] "POST /topics/VIP_VvBVy0tuMPPspm1A_0000000000 HTTP/1.1" 200 101640  46 (io.confluent.rest-utils.requests)`,
	TypeEmpty:     "empty 通过解析清空数据",
	TypeMySQL: `# Time: 2017-12-24T02:42:00.126000Z
# User@Host: rdsadmin[rdsadmin] @ localhost [127.0.0.1]  Id:     3
# Query_time: 0.020363  Lock_time: 0.018450 Rows_sent: 0  Rows_examined: 1
SET timestamp=1514083320;
use foo;
SELECT count(*) from mysql.rds_replication_status WHERE master_host IS NOT NULL and master_port IS NOT NULL GROUP BY action_timestamp,called_by_user,action,mysql_version,master_host,master_port ORDER BY action_timestamp LIMIT 1;
#`,
	TypeLogfmt: `ts=2018-01-02T03:04:05.123Z lvl=5 msg="error" log_id=123456abc
method=PUT duration=1.23 log_id=123456abc`,
	TypeKeyValue: `ts=2018-01-02T03:04:05.123Z lvl=5 msg="error" log_id=123456abc
method=PUT duration=1.23 log_id=123456abc`,
	TypeLinuxAudit: `type=SYSCALL msg=audit(1364481363.243:24287): arch=c000003e syscall=2 success=no exit=-13 a0=7fffd19c5592 a1=0    a2=7fffd19c4b50`,
}
