package parser

import "github.com/qiniu/logkit/utils"

// ModeUsages 用途说明
var ModeUsages = []utils.KeyValue{
	{TypeJson, "json 格式解析"},
	{TypeNginx, "nginx 日志解析"},
	{TypeGrok, "grok 方式解析"},
	{TypeCSV, "csv 格式日志解析"},
	{TypeRaw, "raw 原始日志按行解析"},
	{TypeSyslog, "syslog 日志解析"},
	{TypeLogv1, "qiniulog 七牛日志库解析"},
	{TypeKafkaRest, "kafkarest 日志格式解析"},
	{TypeEmpty, "empty 通过解析清空数据"},
}

var (
	OptionTimezoneOffset = utils.Option{
		KeyName:    KeyTimeZoneOffset,
		ChooseOnly: true,
		Default:    "0",
		ChooseOptions: []interface{}{"0", "-1", "-2", "-3", "-4",
			"-5", "-6", "-7", "-8", "-9", "-10", "-11", "-12",
			"1", "2", "3", "4", "5", "6", "7", "8", "9", "11", "12"},
		DefaultNoUse: false,
		Description:  "时区偏移量(timezone_offset)",
	}
)

var ModeKeyOptions = map[string][]utils.Option{
	TypeJson: {
		{
			KeyName:      KeyParserName,
			ChooseOnly:   false,
			Default:      "parser",
			DefaultNoUse: false,
			Description:  "parser名称(name)",
		},
		{
			KeyName:      KeyLabels,
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "额外的标签信息(labels)",
		},
	},
	TypeNginx: {
		{
			KeyName:      NginxConfPath,
			ChooseOnly:   false,
			Default:      "/opt/nginx/conf/nginx.conf",
			DefaultNoUse: true,
			Description:  "nginx配置路径(nginx_log_format_path)",
		},
		{
			KeyName:      NginxLogFormat,
			ChooseOnly:   false,
			Default:      "main",
			DefaultNoUse: true,
			Description:  "nginx日志格式名称(nginx_log_format_name)",
		},
		{
			KeyName:      KeyParserName,
			ChooseOnly:   false,
			Default:      "parser",
			DefaultNoUse: false,
			Description:  "parser名称(name)",
		},
		{
			KeyName:      NginxSchema,
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "指定nginx字段类型(nginx_schema)",
		},
		{
			KeyName:      NginxFormatRegex,
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "直接通过正则表达式解析(nginx_log_format_regex)",
		},
		{
			KeyName:      KeyLabels,
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "额外的标签信息(labels)",
		},
	},
	TypeGrok: {
		{
			KeyName:      KeyGrokPatterns,
			ChooseOnly:   false,
			Default:      "%{COMMON_LOG_FORMAT}",
			DefaultNoUse: true,
			Description:  "匹配日志的grok表达式(grok_patterns)",
		},
		{
			KeyName:      KeyParserName,
			ChooseOnly:   false,
			Default:      "parser",
			DefaultNoUse: false,
			Description:  "parser名称(name)",
		},
		{
			KeyName:       KeyGrokMode,
			ChooseOnly:    true,
			ChooseOptions: []interface{}{"oneline", ModeMulti},
			Default:       "oneline",
			DefaultNoUse:  false,
			Description:   "grok单行多行模式(grok_mode)",
		},
		{
			KeyName:      KeyGrokCustomPatternFiles,
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "自定义 pattern 文件路径(grok_custom_pattern_files)",
		},
		{
			KeyName:      KeyGrokCustomPatterns,
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "自定义 pattern (grok_custom_patterns)",
		},
		OptionTimezoneOffset,
		{
			KeyName:      KeyLabels,
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "额外的标签信息(labels)",
		},
	},

	TypeCSV: {
		{
			KeyName:      KeyCSVSchema,
			ChooseOnly:   false,
			Default:      "abc string,xyz long,data1 string,data2 float",
			DefaultNoUse: true,
			Description:  "csv格式的字段类型(csv_schema)",
		},
		{
			KeyName:      KeyParserName,
			ChooseOnly:   false,
			Default:      "parser",
			DefaultNoUse: false,
			Description:  "parser名称(name)",
		},
		{
			KeyName:      KeyCSVSplitter,
			ChooseOnly:   false,
			Default:      ",",
			DefaultNoUse: false,
			Description:  "csv分隔符(csv_splitter)",
		},
		{
			KeyName:      KeyLabels,
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "额外的标签信息(labels)",
		},
		OptionTimezoneOffset,
	},
	TypeRaw: {
		{
			KeyName:      KeyParserName,
			ChooseOnly:   false,
			Default:      "parser",
			DefaultNoUse: false,
			Description:  "parser名称(name)",
		},
		{
			KeyName:       KeyTimestamp,
			ChooseOnly:    true,
			ChooseOptions: []interface{}{"true", "false"},
			Default:       "true",
			DefaultNoUse:  false,
			Description:   "数据附带时间戳(timestamp)",
		},
		{
			KeyName:      KeyLabels,
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "额外的标签信息(labels)",
		},
	},
	TypeLogv1: {
		{
			KeyName:      KeyParserName,
			ChooseOnly:   false,
			Default:      "parser",
			DefaultNoUse: false,
			Description:  "parser名称(name)",
		},
		{
			KeyName:      KeyLabels,
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "额外的标签信息(labels)",
		},
		{
			KeyName:      KeyQiniulogPrefix,
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "日志前缀(qiniulog_prefix)",
		},
		{
			KeyName:      KeyLogHeaders,
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "七牛日志格式顺序(qiniulog_log_headers)",
		},
	},
	TypeSyslog: {
		{
			KeyName:      KeyParserName,
			ChooseOnly:   false,
			Default:      "parser",
			DefaultNoUse: false,
			Description:  "parser名称(name)",
		},
		{
			KeyName:       KeyRFCType,
			ChooseOnly:    true,
			Default:       "automic",
			ChooseOptions: []interface{}{"automic", "rfc3164", "rfc5424", "rfc6587"},
			DefaultNoUse:  false,
			Description:   "syslog使用的rfc协议(syslog_rfc)",
		},
		{
			KeyName:      KeyLabels,
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "额外的标签信息(labels)",
		},
	},
	TypeKafkaRest: {
		{
			KeyName:      KeyParserName,
			ChooseOnly:   false,
			Default:      "parser",
			DefaultNoUse: false,
			Description:  "parser名称(name)",
		},
		{
			KeyName:      KeyLabels,
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "额外的标签信息(labels)",
		},
	},
	TypeEmpty: {},
}

// SampleLogs 样例日志，用于前端界面试玩解析器
var SampleLogs = map[string]string{
	TypeNginx: `110.110.101.101 - - [21/Mar/2017:18:14:17 +0800] "GET /files/yyyysx HTTP/1.1" 206 607 1 "-" "Apache-HttpClient/4.4.1 (Java/1.7.0_80)" "-" "122.121.111.222, 122.121.111.333, 192.168.90.61" "192.168.42.54:5000" www.qiniu.com llEAAFgmnoIa3q0U "0.040" 0.040 760 "-" "-" - - QCloud
1.11.1.1 - - [25/Mar/2017:18:14:17 +0800] "GET /files HTTP/1.1" 200 607 1 "-" "Apache-HttpClient/4.4.1 (Java/1.7.0_80)" "-" "122.121.111.222, 122.121.111.333, 192.168.90.61" "192.168.42.54:5000" www.qiniu.com sfvfv123axs "0.040" 0.040 760 "-" "-" - - QCloud`,
	TypeGrok: `127.0.0.1 user-identifier frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326
123.45.12.1 user-identifier bob [10/Oct/2013:13:55:36 -0700] "GET /hello.gif HTTP/1.0" 200 2326`,
	TypeJson: `{"a":"b","c":1,"d":1.1}`,
	TypeCSV: `a,123,bcd,1.2
xsxs,456,asv,5.12`,
	TypeRaw: `raw log1[05-May-2017 13:44:39]  [pool log] pid 4109
script_filename = /data/html/
[0x00007fec119d1720] curl_exec() /data/html/xyframework/base.go:123
[05-May-2017 13:45:39]  [pool log] pid 4108`,
	TypeSyslog:    `<38>Feb 05 01:02:03 abc system[253]: Listening at 0.0.0.0:3000`,
	TypeLogv1:     `2016/10/20 17:30:21.433423 [GE2owHck-Y4IWJHS][WARN] github.com/qiniu/http/rpcutil.v1/rpc_util.go:203: E18102: The specified repo does not exist under the provided appid ~`,
	TypeKafkaRest: `[2016-12-05 03:35:20,682] INFO 172.16.16.191 - - [05/Dec/2016:03:35:20 +0000] "POST /topics/VIP_VvBVy0tuMPPspm1A_0000000000 HTTP/1.1" 200 101640  46 (io.confluent.rest-utils.requests)`,
	TypeEmpty:     "empty 通过解析清空数据",
}
