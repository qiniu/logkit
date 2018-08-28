package nginx

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	"github.com/qiniu/logkit/utils"
	. "github.com/qiniu/logkit/utils/models"
)

var accLog2 = []string{`110.110.101.101 - - [21/Mar/2017:18:14:17 +0800] "GET /files/yyyysx HTTP/1.1" 206 607 1 "-" "Apache-HttpClient/4.4.1 (Java/1.7.0_80)" "-" "122.121.111.222, 122.121.111.333, 192.168.90.61" "192.168.42.54:5000" www.qiniu.com llEAAFgmnoIa3q0U "0.040" 0.040 760 "-" "-" - - QCloud`}
var accLog1 = []string{`111.111.111.101 - - [30/Aug/2016:14:03:37 +0800] "GET /s5/M00/CE/91/xaxsxsxsxs HTTP/1.1" 200 4962 4259 "http://www.abc.cn" "Mozilla/5.0 (Windows NT 6.1; WOW64)" "-" "123.123.123.123" 192.168.41.58:5000 mirror.qiniu.com WEQAAM8htpudgG8U 0.204 0.204 938 - -  -`, ""}
var accErrLog = []string{`can't work'`}
var timeformat1 string
var accLog1Entry Data

func init() {
	timelocal1, _ := time.Parse(time.RFC3339, "2016-08-30T14:03:37+08:00")
	timeformat1 = timelocal1.Format(time.RFC3339)
	accLog1Entry = Data{"request": `GET /s5/M00/CE/91/xaxsxsxsxs HTTP/1.1`,
		"sent_http_x_reqid": "WEQAAM8htpudgG8U", "request_length": int64(938),
		"http_x_from_cdn": "-", "time_local": timeformat1, "status": int64(200), "upstream_addr": "192.168.41.58:5000", "host": "mirror.qiniu.com", "http_x_estat": `-`, "bytes_sent": int64(4962), "http_user_agent": `Mozilla/5.0 (Windows NT 6.1; WOW64)`,
		"http_x_forwarded_for": "123.123.123.123", "http_x_stat": "-", "http_transfer_encoding": "-", "upstream_response_time": "0.204", "request_time": 0.204, "remote_addr": "111.111.111.101",
		"remote_user": "-", "body_bytes_sent": int64(4259), "http_referer": `http://www.abc.cn`}
}

var (
	cfg  = conf.MapConf{"name": "nginx", "disable_record_errdata": "true", parser.NginxConfPath: "test_data/nginx.conf", parser.NginxLogFormat: "main", parser.NginxSchema: "remote_addr:string, remote_user:string, time_local:date, request:string, status:long, bytes_sent:long, body_bytes_sent:long, http_referer:string, http_user_agent:string, http_transfer_encoding:string, http_x_forwarded_for:string, upstream_addr:string, host:string, sent_http_x_reqid:string, upstream_response_time:string, request_time:float, request_length:long, upstream_http_x_tag:string, upstream_http_x_uid:string, http_x_stat:string, http_x_estat:string, http_x_from_cdn:string"}
	cfg2 = conf.MapConf{"name": "nginx", parser.NginxConfPath: "test_data/nginx.conf", parser.NginxLogFormat: "logkit", parser.NginxSchema: "remote_addr:string, remote_user:string, time_local:date, request:string, status:long, bytes_sent:long, body_bytes_sent:long, http_referer:string, http_user_agent:string, http_transfer_encoding:string, http_x_forwarded_for:string, upstream_addr:string, host:string, sent_http_x_reqid:string, upstream_response_time:string, request_time:float, request_length:long, upstream_http_x_tag:string, upstream_http_x_uid:string, http_x_stat:string, http_x_estat:string, http_x_from_cdn:string"}
	cfg3 = conf.MapConf{"name": "nginx", parser.NginxConfPath: "test_data/nginx.conf", parser.NginxLogFormat: "testmain", parser.NginxSchema: "remote_addr string, remote_user string, time_local date, request string, status long, bytes_sent long, body_bytes_sent long, http_referer string, http_user_agent string, http_x_forwarded_for string, upstream_addr string, host string, sent_http_x_reqid string, request_time float"}
	cfg4 = conf.MapConf{"name": "nginx", parser.NginxFormatRegex: `^(?P<remote_addr>[^ ]*) - (?P<remote_user>[^ ]*) \[(?P<time_local>[^]]*)\] "(?P<request>[^"]*)" (?P<status>[^ ]*) (?P<bytes_sent>[^ ]*) (?P<body_bytes_sent>[^ ]*) "(?P<http_referer>[^"]*)" "(?P<http_user_agent>[^"]*)" "(?P<http_transfer_encoding>[^"]*)" "(?P<http_x_forwarded_for>[^"]*)" "(?P<upstream_addr>[^"]*)" (?P<host>[^ ]*) (?P<sent_http_x_reqid>[^ ]*) "(?P<upstream_response_time>[^"]*)" (?P<request_time>[^ ]*) (?P<request_length>[^ ]*) "(?P<upstream_http_x_tag>[^"]*)" "(?P<upstream_http_x_uid>[^"]*)" (?P<http_x_stat>[^ ]*) (?P<http_x_estat>[^ ]*) (?P<http_x_from_cdn>[^ ]*)$`, parser.NginxSchema: "remote_addr string, remote_user string, time_local date, request string, status long, bytes_sent long, body_bytes_sent long, http_referer string, http_user_agent string, http_x_forwarded_for string, upstream_addr string, host string, sent_http_x_reqid string, request_time float"}

	bench    []Data
	testData = utils.GetParseTestData(`111.111.111.101 - - [30/Aug/2016:14:03:37 +0800] "GET /s5/M00/CE/91/xaxsxsxsxs HTTP/1.1" 200 4962 4259 "http://www.abc.cn" "Mozilla/5.0 (Windows NT 6.1; WOW64)" "-" "123.123.123.123" 192.168.41.58:5000 mirror.qiniu.com WEQAAM8htpudgG8U 0.204 0.204 938 - -  -`, DefaultMaxBatchSize)
)

// old: 10	 128034530 ns/op	routine = 1  (2MB)
// now: 10	 181913631 ns/op	routine = 1  (2MB)
// now: 20	  79034431 ns/op	routine = 2  (2MB)
func Benchmark_NginxParse(b *testing.B) {
	p, _ := NewNginxAccParser(cfg)

	var m []Data
	for n := 0; n < b.N; n++ {
		m, _ = p.Parse(testData)
	}
	bench = m
}

func TestNewNginxParser(t *testing.T) {
	p, err := NewNginxAccParser(cfg)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, p.Name(), "nginx", "nginx parser name not equal")
	entry1S, err := p.Parse(accLog1)
	if c, ok := err.(*StatsError); ok {
		err = c.ErrorDetail
		assert.Equal(t, int64(0), c.Errors)
	}
	entry1 := entry1S[0]
	for k, v := range entry1 {
		assert.Equal(t, accLog1Entry[k], v, "parser "+k+" not match")
	}
	errFormat := fmt.Errorf("NginxParser fail to parse log line [%v], given format is [%v]", accErrLog[0], p.regexp)
	_, err = p.Parse(accErrLog)
	if c, ok := err.(*StatsError); ok {
		err = c.ErrorDetail
	}
	assert.Equal(t, err, errFormat, "it should be err format")

	p2, err := NewNginxAccParser(cfg2)
	if err != nil {
		t.Fatal(err)
	}
	_, err = p2.Parse(accLog2)
	if c, ok := err.(*StatsError); ok {
		err = c.ErrorDetail
	}
	if err != nil {
		t.Error(err)
	}
}

func TestNewNginxParserForErrData(t *testing.T) {
	cfg[parser.KeyDisableRecordErrData] = "false"
	p, err := NewNginxAccParser(cfg)
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, p.Name(), "nginx", "nginx parser name not equal")
	entry1S, err := p.Parse(accLog1)
	if c, ok := err.(*StatsError); ok {
		err = c.ErrorDetail
		assert.Equal(t, int64(0), c.Errors)
	}
	if len(entry1S) != 1 {
		t.Fatalf("parse lines error, expect 1 lines but got %v lines", len(entry1S))
	}
	entry1 := entry1S[0]
	for k, v := range entry1 {
		assert.Equal(t, accLog1Entry[k], v, "parser "+k+" not match")
	}
}

func TestNewNginxWithManuelRegex(t *testing.T) {
	p2, err := NewNginxAccParser(cfg4)
	if err != nil {
		t.Fatal(err)
	}
	_, err = p2.Parse(accLog2)
	if c, ok := err.(*StatsError); ok {
		err = c.ErrorDetail
	}
	if err != nil {
		t.Error(err)
	}
}

func TestFindAllRegexpsFromConf(t *testing.T) {
	confPath := "test_data/nginx.conf"
	patterns, err := FindAllRegexpsFromConf(confPath)
	assert.NoError(t, err)

	{
		assert.NotNil(t, patterns["main"])

		re, err := ResolveRegexpFromConf(confPath, "main")
		assert.NoError(t, err)
		assert.Equal(t, patterns["main"].String(), re.String())
	}

	{
		assert.NotNil(t, patterns["logkit"])

		re, err := ResolveRegexpFromConf(confPath, "logkit")
		assert.NoError(t, err)
		assert.Equal(t, patterns["logkit"].String(), re.String())
	}

	{
		assert.NotNil(t, patterns["testmain"])

		re, err := ResolveRegexpFromConf(confPath, "testmain")
		assert.NoError(t, err)
		assert.Equal(t, patterns["testmain"].String(), re.String())
	}
}

var grokBench Data

func Benchmark_BenchNginxParser(b *testing.B) {
	var err error
	p, err := NewNginxAccParser(cfg3)
	if err != nil {
		b.Fatal(err)
	}
	var m Data
	for n := 0; n < b.N; n++ {
		m, err = p.parse(`123.0.0.1 - - [17/Jul/2017:14:56:24 +0800] "POST /v2/repos/x/data HTTP/1.1" 200 479 2 "-" "QiniuPandoraJava/0.0.1 (Linux amd64 2.6.32-696.1.1.el6.x86_64) Java/1.8.0_131" "-" 192.168.160.75:80 pipeline.qiniu.com abc123bdc 0.072`)
		if err != nil {
			b.Fatal(err)
		}
	}
	grokBench = m
}
