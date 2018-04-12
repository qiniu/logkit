package parser

import (
	. "github.com/qiniu/logkit/utils/models"
)

// ModeUsages 用途说明
var ModeUsages = []KeyValue{
	{TypeRaw, "按原始日志逐行发送"},
	{TypeJson, "按 json 格式解析"},
	{TypeNginx, "按 nginx 日志解析"},
	{TypeGrok, "按 grok 格式解析"},
	{TypeCSV, "按 csv 格式解析"},
	{TypeSyslog, "按 syslog 格式解析"},
	{TypeLogv1, "按七牛日志库格式解析"},
	{TypeKafkaRest, "按 kafkarest 日志解析"},
	{TypeEmpty, "通过解析清空数据"},
	{TypeMysqlLog, "按 mysql 慢请求日志解析"},
}

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
		KeyName:       KeyLabels,
		ChooseOnly:    false,
		Default:       "",
		DefaultNoUse:  false,
		Description:   "额外的标签信息(labels)",
		Advance:       true,
		ToolTip:       `额外的标签信息，同样逗号分隔，如 "app logkit, user pandora"`,
		ToolTipActive: true,
	}

	OptionDisableRecordErrData = Option{
		KeyName:       KeyDisableRecordErrData,
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
)

var ModeKeyOptions = map[string][]Option{
	TypeJson: {
		OptionParserName,
		OptionLabels,
		OptionDisableRecordErrData,
	},
	TypeNginx: {
		{
			KeyName:      NginxConfPath,
			ChooseOnly:   false,
			Default:      "",
			Required:     true,
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
			KeyName:       NginxSchema,
			ChooseOnly:    false,
			Default:       "",
			DefaultNoUse:  false,
			Description:   "手动指定字段类型(nginx_schema)",
			Advance:       true,
			ToolTip:       `nginx日志都被解析为string，指定该格式可以设置为float、long、date三种类型。如 time_local date,bytes_sent long,request_time float`,
			ToolTipActive: true,
		},
		{
			KeyName:      NginxFormatRegex,
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "手动指定正则表达式解析(nginx_log_format_regex)",
			Advance:      true,
			ToolTip:      "若根据配置文件自动生成的正则表达式无效，可通过此配置手动填写",
		},
		OptionParserName,
		OptionLabels,
		OptionDisableRecordErrData,
	},
	TypeGrok: {
		{
			KeyName:       KeyGrokPatterns,
			ChooseOnly:    false,
			Default:       "%{COMMON_LOG_FORMAT}",
			Placeholder:   "%{COMMON_LOG_FORMAT}",
			Required:      true,
			DefaultNoUse:  true,
			Description:   "匹配日志的grok表达式(grok_patterns)",
			ToolTip:       `用于匹配日志的grok表达式，多个用逗号分隔，如 "%{COMMON_LOG_FORMAT},%{QINIU_LOG_FORMAT}"`,
			ToolTipActive: true,
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
	},

	TypeCSV: {
		{
			KeyName:      KeyCSVSplitter,
			ChooseOnly:   false,
			Default:      ",",
			Placeholder:  ",",
			Required:     true,
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
			ToolTip:      `按照逗号分隔的字符串，如"abc string"，字段类型现在支持string, long, jsonmap, float，date`,
		},
		{
			KeyName:       KeyCSVAllowNoMatch,
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
			ToolTip:      `允许日志数量比定义的schema多且命名为该字段后跟数字，如"unknow0, unknow1"`,
		},
		{
			KeyName:       KeyCSVIgnoreInvalidField,
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
			KeyName:       KeyAutoRename,
			ChooseOnly:    true,
			ChooseOptions: []interface{}{"true", "false"},
			Default:       "true",
			DefaultNoUse:  false,
			Description:   "将字段名称中的'-'更改为'_'",
			Advance:       true,
		},
		OptionDisableRecordErrData,
	},
	TypeRaw: {
		{
			KeyName:       KeyTimestamp,
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
			KeyName:      KeyQiniulogPrefix,
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "日志前缀(qiniulog_prefix)",
			Advance:      true,
		},
		{
			KeyName:      KeyLogHeaders,
			ChooseOnly:   false,
			Default:      "",
			DefaultNoUse: false,
			Description:  "日志格式顺序(qiniulog_log_headers)",
			Advance:      true,
		},
		OptionParserName,
		OptionDisableRecordErrData,
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
		OptionParserName,
		OptionLabels,
		OptionDisableRecordErrData,
	},
	TypeKafkaRest: {
		OptionParserName,
		OptionLabels,
		OptionDisableRecordErrData,
	},
	TypeEmpty: {},
	TypeMysqlLog: {
		OptionParserName,
		OptionLabels,
		OptionDisableRecordErrData,
	},
}

// SampleLogs 样例日志，用于前端界面试玩解析器
var SampleLogs = map[string]string{
	TypeNginx: `110.110.101.101 - - [21/Mar/2017:18:14:17 +0800] "GET /files/yyyysx HTTP/1.1" 206 607 1 "-" "Apache-HttpClient/4.4.1 (Java/1.7.0_80)" "-" "122.121.111.222, 122.121.111.333, 192.168.90.61" "192.168.42.54:5000" www.qiniu.com llEAAFgmnoIa3q0U "0.040" 0.040 760 "-" "-" - - QCloud`,
	TypeGrok: `127.0.0.1 user-identifier frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326
123.45.12.1 user-identifier bob [10/Oct/2013:13:55:36 -0700] "GET /hello.gif HTTP/1.0" 200 2326`,
	TypeJson:      `{"a":"b","c":1,"d":1.1}`,
	TypeCSV:       `a,123,bcd,1.2`,
	TypeRaw:       `raw log1[05-May-2017 13:44:39]  [pool log] pid 4109`,
	TypeSyslog:    `<38>Feb 05 01:02:03 abc system[253]: Listening at 0.0.0.0:3000`,
	TypeLogv1:     `2016/10/20 17:30:21.433423 [GE2owHck-Y4IWJHS][WARN] github.com/qiniu/http/rpcutil.v1/rpc_util.go:203: E18102: The specified repo does not exist under the provided appid ~`,
	TypeKafkaRest: `[2016-12-05 03:35:20,682] INFO 172.16.16.191 - - [05/Dec/2016:03:35:20 +0000] "POST /topics/VIP_VvBVy0tuMPPspm1A_0000000000 HTTP/1.1" 200 101640  46 (io.confluent.rest-utils.requests)`,
	TypeEmpty:     "empty 通过解析清空数据",
	TypeMysqlLog: `# Time: 2017-12-24T02:42:00.126000Z
# User@Host: rdsadmin[rdsadmin] @ localhost [127.0.0.1]  Id:     3
# Query_time: 0.020363  Lock_time: 0.018450 Rows_sent: 0  Rows_examined: 1
SET timestamp=1514083320;
use foo;
SELECT count(*) from mysql.rds_replication_status WHERE master_host IS NOT NULL and master_port IS NOT NULL GROUP BY action_timestamp,called_by_user,action,mysql_version,master_host,master_port ORDER BY action_timestamp LIMIT 1;
#`,
}
