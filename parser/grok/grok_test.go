package grok

import (
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/qiniu/logkit/utils"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	grokBench Data
	bench     []Data
	testData  = utils.GetParseTestData(`127.0.0.1 user-identifier frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326`, DefaultMaxBatchSize)
)

// old: 1		4154037708 ns/op 	routine = 1  (2MB)
// now: 1		4582286082 ns/op 	routine = 1  (2MB)
// now: 1		2242491924 ns/op 	routine = 2  (2MB)
func Benchmark_GrokParse_NGINX(b *testing.B) {
	p := &Parser{
		Patterns:   []string{"%{NGINX_LOG}"},
		numRoutine: 2,
	}
	p.compile()

	var m []Data
	m, _ = p.Parse(testData)
	bench = m
}

func Benchmark_GrokParseLine_NGINX(b *testing.B) {
	p := &Parser{
		Patterns: []string{"%{NGINX_LOG}"},
	}
	p.compile()

	var m Data
	for n := 0; n < b.N; n++ {
		m, _ = p.parse(`127.0.0.1 user-identifier frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326`)
	}
	grokBench = m
}

func Benchmark_GrokParseLine_PANDORANGINX(b *testing.B) {
	p := &Parser{
		Patterns: []string{"%{PANDORA_NGINX}"},
	}
	p.compile()

	var m Data
	for n := 0; n < b.N; n++ {
		m, _ = p.parse(`123.0.0.1 - - [17/Jul/2017:14:56:24 +0800] "POST /v2/repos/x/data HTTP/1.1" 200 479 2 "-" "QiniuPandoraJava/0.0.1 (Linux amd64 2.6.32-696.1.1.el6.x86_64) Java/1.8.0_131" "-" 192.168.160.75:80 pipeline.qiniu.com abc123bdc 0.072`)
	}
	grokBench = m
}

func Benchmark_GrokParseLine_Common(b *testing.B) {
	p := &Parser{
		Patterns: []string{"%{COMMON_LOG_FORMAT}"},
	}
	p.compile()

	var m Data
	for n := 0; n < b.N; n++ {
		m, _ = p.parse(`127.0.0.1 user-identifier frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326`)
	}
	grokBench = m
}

func Benchmark_GroktrimInvalidSpace(b *testing.B) {
	src := "TEST_LOG_A %{NUMBER :myfloat:  float} %{  RESPONSE_CODE} %{IPORHOST : clientip} %{  RESPONSE_TIME}"
	for i := 0; i < b.N; i++ {
		trimInvalidSpace(src)
	}
}

//100000	     17110 ns/op

// Test a very simple parse pattern.
func TestSimpleParse(t *testing.T) {
	p := &Parser{
		Patterns: []string{"%{TESTLOG}"},
		CustomPatterns: `
			TESTLOG %{NUMBER:num:long} %{WORD:client}
		`,
	}
	assert.NoError(t, p.compile())

	m, err := p.parse(`142 bot`)
	assert.NoError(t, err)
	require.NotNil(t, m)

	assert.Equal(t,
		Data{
			"num":    int64(142),
			"client": "bot",
		},
		m)
}

// Test a nginx time.
func TestNginxTimeParse(t *testing.T) {
	p := &Parser{
		Patterns: []string{"%{NGINX_LOG}"},
	}
	assert.NoError(t, p.compile())

	m, err := p.parse(`192.168.45.53 - - [05/Apr/2017:17:25:06 +0800] "POST /v2/repos/kodo_z0_app_pfdstg/data HTTP/1.1" 200 497 2 "-" "Go 1.1 package http" "-" 192.168.160.1:80 pipeline.qiniu.io KBkAAD7W6-UfdrIU 0.139`)
	assert.NoError(t, err)
	require.NotNil(t, m)
	assert.Equal(t, "2017-04-05T17:25:06+08:00", m["ts"])
}

func TestTimeZoneOffsetParse(t *testing.T) {
	p := &Parser{
		Patterns:       []string{"%{NGINX_LOG}"},
		timeZoneOffset: -3,
	}
	assert.NoError(t, p.compile())

	m, err := p.parse(`192.168.45.53 - - [05/Apr/2017:17:25:06 +0800] "POST /v2/repos/kodo_z0_app_pfdstg/data HTTP/1.1" 200 497 2 "-" "Go 1.1 package http" "-" 192.168.160.1:80 pipeline.qiniu.io KBkAAD7W6-UfdrIU 0.139`)
	assert.NoError(t, err)
	require.NotNil(t, m)
	assert.Equal(t, "2017-04-05T14:25:06+08:00", m["ts"])

	p = &Parser{
		Patterns:       []string{"%{NGINX_LOG}"},
		timeZoneOffset: 8,
	}
	assert.NoError(t, p.compile())

	m, err = p.parse(`192.168.45.53 - - [05/Apr/2017:10:25:06 +0800] "POST /v2/repos/kodo_z0_app_pfdstg/data HTTP/1.1" 200 497 2 "-" "Go 1.1 package http" "-" 192.168.160.1:80 pipeline.qiniu.io KBkAAD7W6-UfdrIU 0.139`)
	assert.NoError(t, err)
	require.NotNil(t, m)
	assert.Equal(t, "2017-04-05T18:25:06+08:00", m["ts"])
}

// Verify that patterns with a regex lookahead fail at compile time.
func TestParsePatternsWithLookahead(t *testing.T) {
	p := &Parser{
		Patterns: []string{"%{MYLOG}"},
		CustomPatterns: `
			NOBOT ((?!bot|crawl).)*
			MYLOG %{NUMBER:num:long} %{NOBOT:client}
		`,
	}
	assert.NoError(t, p.compile())

	_, err := p.parse(`1466004605359052000 bot`)
	assert.Error(t, err)
}

func TestParserName(t *testing.T) {
	p := &Parser{
		Patterns: []string{"%{NGINX_LOG}"},
		name:     "my_web_log",
	}
	assert.NoError(t, p.compile())

	// Parse an influxdb POST request
	m, err := p.parse(`127.0.0.1 user-identifier frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326`)
	require.NotNil(t, m)
	assert.NoError(t, err)
	assert.Equal(t,
		Data{
			"resp_bytes":   int64(2326),
			"auth":         "frank",
			"client_ip":    "127.0.0.1",
			"http_version": float64(1.0),
			"ident":        "user-identifier",
			"request":      "/apache_pb.gif",
			"ts":           "2000-10-10T13:55:36-07:00",
			"resp_code":    "200",
			"verb":         "GET",
		},
		m)
	assert.Equal(t, "my_web_log", p.Name())
}

func TestCLF_IPv6(t *testing.T) {
	p := &Parser{
		name:     "my_web_log",
		Patterns: []string{"%{NGINX_LOG}"},
	}
	assert.NoError(t, p.compile())

	m, err := p.parse(`2001:0db8:85a3:0000:0000:8a2e:0370:7334 user-identifier frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326`)
	require.NotNil(t, m)
	assert.NoError(t, err)
	assert.Equal(t,
		Data{
			"client_ip":    "2001:0db8:85a3:0000:0000:8a2e:0370:7334",
			"ts":           "2000-10-10T13:55:36-07:00",
			"verb":         "GET",
			"resp_bytes":   int64(2326),
			"auth":         "frank",
			"http_version": float64(1.0),
			"ident":        "user-identifier",
			"request":      "/apache_pb.gif",
			"resp_code":    "200",
		},
		m)
	assert.Equal(t, "my_web_log", p.Name())

	m, err = p.parse(`::1 user-identifier frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 400 2326`)
	require.NotNil(t, m)
	assert.NoError(t, err)
	assert.Equal(t,
		Data{
			"resp_bytes":   int64(2326),
			"auth":         "frank",
			"client_ip":    "::1",
			"http_version": float64(1.0),
			"ident":        "user-identifier",
			"request":      "/apache_pb.gif",
			"ts":           "2000-10-10T13:55:36-07:00",
			"verb":         "GET",
			"resp_code":    "400",
		},
		m)

	assert.Equal(t, "my_web_log", p.Name())
}

func TestCustomInfluxdbHttpd(t *testing.T) {
	p := &Parser{
		Patterns: []string{`\[httpd\] %{COMBINED_LOG_FORMAT} %{UUID:uuid} %{NUMBER:response_time_us:long}`},
	}
	assert.NoError(t, p.compile())

	// Parse an influxdb POST request
	m, err := p.parse(`[httpd] ::1 - - [14/Jun/2016:11:33:29 +0100] "POST /write?consistency=any&db=logkit&precision=ns&rp= HTTP/1.1" 204 0 "-" "InfluxDBClient" 6f61bc44-321b-11e6-8050-000000000000 2513`)
	require.NotNil(t, m)
	assert.NoError(t, err)
	assert.Equal(t,
		Data{
			"resp_bytes":       int64(0),
			"auth":             "-",
			"client_ip":        "::1",
			"http_version":     float64(1.1),
			"ident":            "-",
			"referrer":         "-",
			"verb":             "POST",
			"request":          "/write?consistency=any&db=logkit&precision=ns&rp=",
			"response_time_us": int64(2513),
			"agent":            "InfluxDBClient",
			"resp_code":        "204",
			"uuid":             "6f61bc44-321b-11e6-8050-000000000000",
			"ts":               "2016-06-14T11:33:29+01:00",
		},
		m)

	// Parse an influxdb GET request
	m, err = p.parse(`[httpd] ::1 - - [14/Jun/2016:12:10:02 +0100] "GET /query?db=logkit&q=SELECT+bytes%2Cresponse_time_us+FROM+logGrokParser_grok+WHERE+http_method+%3D+%27GET%27+AND+response_time_us+%3E+0+AND+time+%3E+now%28%29+-+1h HTTP/1.1" 200 578 "http://localhost:8083/" "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.84 Safari/537.36" 8a3806f1-3220-11e6-8006-000000000000 988`)
	require.NotNil(t, m)
	assert.NoError(t, err)
	assert.Equal(t,
		Data{
			"resp_bytes":       int64(578),
			"auth":             "-",
			"client_ip":        "::1",
			"http_version":     float64(1.1),
			"ident":            "-",
			"referrer":         "http://localhost:8083/",
			"request":          "/query?db=logkit&q=SELECT+bytes%2Cresponse_time_us+FROM+logGrokParser_grok+WHERE+http_method+%3D+%27GET%27+AND+response_time_us+%3E+0+AND+time+%3E+now%28%29+-+1h",
			"response_time_us": int64(988),
			"agent":            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/51.0.2704.84 Safari/537.36",
			"resp_code":        "200",
			"uuid":             "8a3806f1-3220-11e6-8006-000000000000",
			"ts":               "2016-06-14T12:10:02+01:00",
			"verb":             "GET",
		},
		m)
}

// common log format
// 127.0.0.1 user-identifier frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326
func TestBuiltinCommonLogFormat(t *testing.T) {
	p := &Parser{
		Patterns: []string{"%{NGINX_LOG}"},
	}
	assert.NoError(t, p.compile())

	// Parse an influxdb POST request
	m, err := p.parse(`127.0.0.1 user-identifier frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326`)
	require.NotNil(t, m)
	assert.NoError(t, err)
	assert.Equal(t,
		Data{
			"resp_bytes":   int64(2326),
			"auth":         "frank",
			"client_ip":    "127.0.0.1",
			"http_version": float64(1.0),
			"ident":        "user-identifier",
			"request":      "/apache_pb.gif",
			"verb":         "GET",
			"resp_code":    "200",
			"ts":           "2000-10-10T13:55:36-07:00",
		},
		m)
}

// common log format
// 127.0.0.1 user1234 frank1234 [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326
func TestBuiltinCommonLogFormatWithNumbers(t *testing.T) {
	p := &Parser{
		Patterns: []string{"%{NGINX_LOG}"},
	}
	assert.NoError(t, p.compile())

	// Parse an influxdb POST request
	m, err := p.parse(`127.0.0.1 user1234 frank1234 [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326`)
	require.NotNil(t, m)
	assert.NoError(t, err)
	assert.Equal(t,
		Data{
			"resp_bytes":   int64(2326),
			"auth":         "frank1234",
			"client_ip":    "127.0.0.1",
			"http_version": float64(1.0),
			"ident":        "user1234",
			"request":      "/apache_pb.gif",
			"ts":           "2000-10-10T13:55:36-07:00",
			"verb":         "GET",
			"resp_code":    "200",
		},
		m)
}

// combined log format
// 127.0.0.1 user-identifier frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326 "-" "Mozilla"
func TestBuiltinCombinedLogFormat(t *testing.T) {
	p := &Parser{
		Patterns: []string{"%{COMBINED_LOG_FORMAT}"},
	}
	assert.NoError(t, p.compile())

	// Parse an influxdb POST request
	m, err := p.parse(`127.0.0.1 user-identifier frank [10/Oct/2000:13:55:36 -0700] "GET /apache_pb.gif HTTP/1.0" 200 2326 "-" "Mozilla"`)
	require.NotNil(t, m)
	assert.NoError(t, err)
	assert.Equal(t,
		Data{
			"resp_bytes":   int64(2326),
			"auth":         "frank",
			"client_ip":    "127.0.0.1",
			"http_version": float64(1.0),
			"ident":        "user-identifier",
			"request":      "/apache_pb.gif",
			"referrer":     "-",
			"agent":        "Mozilla",
			"verb":         "GET",
			"resp_code":    "200",
			"ts":           "2000-10-10T13:55:36-07:00",
		},
		m)
}

func TestCompileStringAndParse(t *testing.T) {
	p := &Parser{
		Patterns: []string{"%{TEST_LOG_A}"},
		CustomPatterns: `
			DURATION %{NUMBER}[nuµm]?s
			RESPONSE_CODE %{NUMBER:response_code}
			RESPONSE_TIME %{DURATION:response_time}
			TEST_LOG_A %{NUMBER:myfloat:float} %{RESPONSE_CODE} %{IPORHOST:clientip} %{RESPONSE_TIME}
		`,
	}
	assert.NoError(t, p.compile())

	metricA, err := p.parse(`1.25 200 192.168.1.1 5.432µs`)
	require.NotNil(t, metricA)
	assert.NoError(t, err)
	assert.Equal(t,
		Data{
			"clientip":      "192.168.1.1",
			"myfloat":       float64(1.25),
			"response_time": "5.432µs",
			"response_code": "200",
		},
		metricA)
}

func TestCompileInvalidStringAndParse(t *testing.T) {
	p := &Parser{
		Patterns: []string{"%{TEST_LOG_A}"},
		CustomPatterns: `
			DURATION %{NUMBER}[nuµm]?s
			RESPONSE_CODE %{ NUMBER :   response_code }
			RESPONSE_TIME %{ DURATION :  response_time  }
			TEST_LOG_A %{ NUMBER :myfloat:  float} %{ RESPONSE_CODE  } %{ IPORHOST : clientip} %{  RESPONSE_TIME }
		`,
	}
	assert.NoError(t, p.compile())

	metricA, err := p.parse(`1.25 200 192.168.1.1 5.432µs`)
	require.NotNil(t, metricA)
	assert.NoError(t, err)
	assert.Equal(t,
		Data{
			"clientip":      "192.168.1.1",
			"myfloat":       float64(1.25),
			"response_time": "5.432µs",
			"response_code": "200",
		},
		metricA)
}

func TestCompileErrorsOnInvalidPattern(t *testing.T) {
	p := &Parser{
		Patterns: []string{"%{TEST_LOG_A}", "%{TEST_LOG_B}"},
		CustomPatterns: `
			DURATION %{NUMBER}[nuµm]?s
			RESPONSE_CODE %{NUMBER:response_code:tag}
			RESPONSE_TIME %{DURATION:response_time:duration}
			TEST_LOG_A %{NUMBER:myfloat:float} %{RESPONSE_CODE} %{IPORHOST:clientip} %{RESPONSE_TIME}
		`,
	}
	assert.Error(t, p.compile())

	metricA, _ := p.parse(`1.25 200 192.168.1.1 5.432µs`)
	require.Nil(t, metricA)
}

func TestParsePatternsWithoutCustom(t *testing.T) {
	p := &Parser{
		Patterns: []string{"%{POSINT:ts:long} response_time=%{POSINT:response_time:long} mymetric=%{NUMBER:metric:float}"},
	}
	assert.NoError(t, p.compile())

	metricA, err := p.parse(`1466004605359052000 response_time=20821 mymetric=10890.645`)
	require.NotNil(t, metricA)
	assert.NoError(t, err)
	assert.Equal(t,
		Data{
			"response_time": int64(20821),
			"metric":        float64(10890.645),
			"ts":            int64(1466004605359052000),
		},
		metricA)
}

func TestCompileFileAndParse(t *testing.T) {
	p := &Parser{
		Patterns:           []string{"%{TEST_LOG_A}", "%{TEST_LOG_B}"},
		CustomPatternFiles: []string{"./test_data/test-patterns"},
	}
	assert.NoError(t, p.compile())

	metricA, err := p.parse(`[04/Jun/2016:12:41:45 +0100] 1.25 200 192.168.1.1 5.432µs 101`)
	require.NotNil(t, metricA)
	assert.NoError(t, err)
	assert.Equal(t,
		Data{
			"clientip":      "192.168.1.1",
			"myfloat":       float64(1.25),
			"response_time": "5.432µs",
			"myint":         int64(101),
			"response_code": "200",
			"timestamp":     "2016-06-04T12:41:45+01:00",
		},
		metricA)

	metricB, err := p.parse(`[04/06/2016--12:41:45] 1.25 mystring dropme nomodifier`)
	require.NotNil(t, metricB)
	assert.NoError(t, err)
	assert.Equal(t,
		Data{
			"myfloat":    1.25,
			"mystring":   "mystring",
			"nomodifier": "nomodifier",
			"timestamp":  "2016-06-04T12:41:45Z",
		},
		metricB)
}

func TestCompileNoModifiersAndParse(t *testing.T) {
	p := &Parser{
		Patterns: []string{"%{TEST_LOG_C}"},
		CustomPatterns: `
			DURATION %{NUMBER}[nuµm]?s
			TEST_LOG_C %{NUMBER:myfloat} %{NUMBER} %{IPORHOST:clientip} %{DURATION:rt}
		`,
	}
	assert.NoError(t, p.compile())

	metricA, err := p.parse(`1.25 200 192.168.1.1 5.432µs`)
	require.NotNil(t, metricA)
	assert.NoError(t, err)
	assert.Equal(t,
		Data{
			"clientip": "192.168.1.1",
			"myfloat":  "1.25",
			"rt":       "5.432µs",
		},
		metricA)
}

func TestCompileNoNamesAndParse(t *testing.T) {
	p := &Parser{
		Patterns: []string{"%{TEST_LOG_C}"},
		CustomPatterns: `
			DURATION %{NUMBER}[nuµm]?s
			TEST_LOG_C %{NUMBER} %{NUMBER} %{IPORHOST} %{DURATION}
		`,
	}
	assert.NoError(t, p.compile())

	metricA, err := p.parse(`1.25 200 192.168.1.1 5.432µs`)
	require.Nil(t, metricA)
	assert.Error(t, err)
}

func TestParseNoMatch(t *testing.T) {
	p := &Parser{
		Patterns:           []string{"%{TEST_LOG_A}", "%{TEST_LOG_B}"},
		CustomPatternFiles: []string{"./test_data/test-patterns"},
	}
	assert.NoError(t, p.compile())

	metricA, err := p.parse(`[04/Jun/2016:12:41:45 +0100] notnumber 200 192.168.1.1 5.432µs 101`)
	assert.Error(t, err)
	assert.Nil(t, metricA)
}

func TestCompileErrors(t *testing.T) {
	// compile fails because there are multiple timestamps:
	p := &Parser{
		Patterns: []string{"%{TEST_LOG_A}", "%{TEST_LOG_B}"},
		CustomPatterns: `
			TEST_LOG_A %{HTTPDATE:ts1:date} %{HTTPDATE:ts2:date} %{NUMBER:mynum:long}
		`,
	}
	assert.Error(t, p.compile())

	// compile fails because file doesn't exist:
	p = &Parser{
		Patterns:           []string{"%{TEST_LOG_A}", "%{TEST_LOG_B}"},
		CustomPatternFiles: []string{"/tmp/foo/bar/baz"},
	}
	assert.Error(t, p.compile())
}

func TestParseErrors(t *testing.T) {
	// Parse fails because the pattern doesn't exist
	p := &Parser{
		Patterns: []string{"%{TEST_LOG_B}"},
		CustomPatterns: `
			TEST_LOG_A %{HTTPDATE:ts:date} %{WORD:myword:long} %{}
		`,
	}
	assert.Error(t, p.compile())
	_, err := p.parse(`[04/Jun/2016:12:41:45 +0100] notnumber 200 192.168.1.1 5.432µs 101`)
	assert.Error(t, err)

	// Parse fails because myword is not an long
	p = &Parser{
		Patterns: []string{"%{TEST_LOG_A}"},
		CustomPatterns: `
			TEST_LOG_A %{HTTPDATE:ts:date} %{WORD:myword:long}
		`,
	}
	assert.NoError(t, p.compile())
	_, err = p.parse(`04/Jun/2016:12:41:45 +0100 notnumber`)
	assert.NoError(t, err) // 只打日志，不报错

	// Parse fails because myword is not a float
	p = &Parser{
		Patterns: []string{"%{TEST_LOG_A}"},
		CustomPatterns: `
			TEST_LOG_A %{HTTPDATE:ts:date} %{WORD:myword:float}
		`,
	}
	assert.NoError(t, p.compile())
	_, err = p.parse(`04/Jun/2016:12:41:45 +0100 notnumber`)
	assert.NoError(t, err) // 只打日志，不报错

}

func TestParseMultiLine(t *testing.T) {

	pattern := `^\[\d+-\w+-\d+\s\d+:\d+:\d+]\s.*`
	matched, err := regexp.MatchString(pattern, "[05-May-2017 13:44:39]  [pool log] pid 4109")
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, true, matched)

	matched, err = regexp.MatchString(pattern, "script_filename = /data/html/log.ushengsheng.com/index.php")
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, false, matched)

	p := &Parser{
		Patterns: []string{"%{PHP_FPM_SLOW_LOG}"},
		mode:     "multi",
		CustomPatterns: `
			PHPLOGTIMESTAMP (%{MONTHDAY}-%{MONTH}-%{YEAR}|%{YEAR}-%{MONTHNUM}-%{MONTHDAY}) %{HOUR}:%{MINUTE}:%{SECOND}
			PHPTZ (%{WORD}\/%{WORD})
			PHPTIMESTAMP \[%{PHPLOGTIMESTAMP:timestamp}(?:\s+%{PHPTZ}|)\]

			PHPFPMPOOL \[pool %{WORD:pool}\]
			PHPFPMCHILD child %{NUMBER:childid}

			FPMERRORLOG \[%{PHPLOGTIMESTAMP:timestamp}\] %{WORD:type}: %{GREEDYDATA:message}
			PHPERRORLOG %{PHPTIMESTAMP} %{WORD:type} %{GREEDYDATA:message}

			PHP_FPM_SLOW_LOG (?m)^\[%{PHPLOGTIMESTAMP:timestamp}\]\s\s\[%{WORD:type}\s%{WORD}\]\s%{GREEDYDATA:message}$
		`,
	}
	assert.NoError(t, p.compile())
	lines := []string{
		`[05-May-2017 13:44:39]  [pool log] pid 4109`,
		`script_filename = /data/html/log.ushengsheng.com/index.php`,
		`[0x00007fec119d1720] curl_exec() /data/html/xyframework/base/XySoaClient.php:357`,
		`[0x00007fec119d1590] request_post() /data/html/xyframework/base/XySoaClient.php:284`,
		`[0x00007fff39d538b0] __call() unknown:0`,
		`[0x00007fec119d13a8] add() /data/html/log.ushengsheng.com/1/interface/ErrorLogInterface.php:70`,
		`[0x00007fec119d1298] log() /data/html/log.ushengsheng.com/1/interface/ErrorLogInterface.php:30`,
		`[0x00007fec119d1160] android() /data/html/xyframework/core/x.php:215`,
		`[0x00007fec119d0ff8] +++ dump failed`,
	}

	data, err := p.parse(strings.Join(lines, "\n"))
	assert.NoError(t, err)
	assert.Equal(t,
		Data{
			"timestamp": "05-May-2017 13:44:39",
			"type":      "pool",
			"message":   "pid 4109 script_filename = /data/html/log.ushengsheng.com/index.php [0x00007fec119d1720] curl_exec() /data/html/xyframework/base/XySoaClient.php:357 [0x00007fec119d1590] request_post() /data/html/xyframework/base/XySoaClient.php:284 [0x00007fff39d538b0] __call() unknown:0 [0x00007fec119d13a8] add() /data/html/log.ushengsheng.com/1/interface/ErrorLogInterface.php:70 [0x00007fec119d1298] log() /data/html/log.ushengsheng.com/1/interface/ErrorLogInterface.php:30 [0x00007fec119d1160] android() /data/html/xyframework/core/x.php:215 [0x00007fec119d0ff8] +++ dump failed",
		}, data)
}

func TestTrimInvalidSpace(t *testing.T) {
	tests := []struct {
		s   string
		exp string
	}{
		{
			"%{aaa}",
			"%{aaa}",
		},
		{
			"%{  aa}",
			"%{aa}",
		},
		{
			"%{aaa }",
			"%{aaa}",
		},
		{
			"%{ aa a }",
			"%{aa a}",
		},
		{
			"%{ a a:	bb }",
			"%{a a:bb}",
		},
		{
			"%{ aa a : b	bb b :ss }",
			"%{aa a:b	bb b:ss}",
		},
		{
			"%{ a aa: b b :c} :$ s absc%{ aa: b bb }",
			"%{a aa:b b:c} :$ s absc%{aa:b bb}",
		},
		{
			"%{ a a : b b : c c } : %{ d d : e e } : %{ f f }",
			"%{a a:b b:c c} : %{d d:e e} : %{f f}",
		},
		{
			"%{a:a} aa : bb %{b:c} bb : cc %{e} ee: ff",
			"%{a:a} aa : bb %{b:c} bb : cc %{e} ee: ff",
		},
		{
			"%{aaa:bbb:ccc}%{aaa:bbb}%{aaa}",
			"%{aaa:bbb:ccc}%{aaa:bbb}%{aaa}",
		},
		{
			"DURATION %{NUMBER  }[nuµm]?s",
			"DURATION %{NUMBER}[nuµm]?s",
		},
		{
			"DURATION %{NUMBER  }[nuµm]?s",
			"DURATION %{NUMBER}[nuµm]?s",
		},
		{
			"RESPONSE_CODE %{ NUMBER :   response_code }",
			"RESPONSE_CODE %{NUMBER:response_code}",
		},
		{
			"RESPONSE_TIME %{ DURATION :  response_time  }",
			"RESPONSE_TIME %{DURATION:response_time}",
		},
		{
			"TEST_LOG_A %{NUMBER :myfloat:  float} %{  RESPONSE_CODE} %{IPORHOST : clientip} %{  RESPONSE_TIME}",
			"TEST_LOG_A %{NUMBER:myfloat:float} %{RESPONSE_CODE} %{IPORHOST:clientip} %{RESPONSE_TIME}",
		},
		{
			"%{{}",
			"%{{}",
		},
		{
			"%{ { }",
			"%{{}",
		},
		{
			"%{ { } } ",
			"%{{} } ",
		},
		{
			"%{}",
			"%{}",
		},
		{
			"%{ }",
			"%{}",
		},
		{
			"%{",
			"%{",
		},
		{
			"%}",
			"%}",
		},
		{
			"{ }",
			"{ }",
		},
	}
	for _, ti := range tests {
		got := trimInvalidSpace(ti.s)
		assert.Equal(t, ti.exp, got)
	}
}

func TestAddCustomPatterns(t *testing.T) {
	p := &Parser{
		Patterns: []string{"%{TEST_LOG_A}"},
		CustomPatterns: `
			DURATION
			RESPONSE_CODE %{NUMBER:response_code}
			RESPONSE_TIME %{DURATION:response_time}
			TEST_LOG_A %{NUMBER:myfloat:float} %{RESPONSE_CODE} %{IPORHOST:clientip} %{RESPONSE_TIME}
		`,
	}
	assert.Error(t, p.compile())
}

func TestNginxTimeParseForErrData(t *testing.T) {
	p := &Parser{
		Patterns:   []string{"%{test}"},
		numRoutine: 2,
	}

	lines := []string{`192.168.45.53 - - [05/Apr/2017:17:25:06 +0800] "POST /v2/repos/kodo_z0_app_pfdstg/data HTTP/1.1" 200 497 2 "-" "Go 1.1 package http" "-" 192.168.160.1:80 pipeline.qiniu.io KBkAAD7W6-UfdrIU 0.139`}
	m, err := p.Parse(lines)
	if err != nil {
		errx, _ := err.(*StatsError)
		assert.Equal(t, int64(1), errx.StatsInfo.Errors)
	}
	if len(m) != 1 {
		t.Fatalf("parse lines error, expect 1 line but got %v lines", len(m))
	}

	for _, v := range m {
		assert.EqualValues(t, lines[0], v[KeyPandoraStash])
	}
}

func TestCompileFileAndParseMultiLine(t *testing.T) {
	p := &Parser{
		Patterns:       []string{"%{CUSTOM_GROK_FINAL}"},
		CustomPatterns: "CUSTOM_GROK_FINAL \\[%{TIMESTAMP_ISO8601:time}\\]\\[12345\\] Level 5:\\n发送时间\\[%{TIMESTAMP_ISO8601:time2}\\],接收时间\\[20T09:57:58\\.123456\\]\\n本地队列名:\\[%{WORD:ok1}\\],\\n报文发送队列名:\\[%{WORD:ok3}\\],\\n报文头部信息:\\[无\\]\\n报文内容:\\n{%{NOTSPACE:a} %{NOTSPACE:b} %{NOTSPACE:c} %{NOTSPACE:d}}\\n%{GREEDYDATALINEFEED:xml}",
	}
	assert.NoError(t, p.compile())

	multiLine, err := p.parse(`[2017-03-28 12:34:56.123456][12345] Level 5:
发送时间[2017-03-27 01:23:45.67],接收时间[20T09:57:58.123456]
本地队列名:[ABCDEFGHIJK],
报文发送队列名:[ABCDEFGHIJKLMNOPQRST],
报文头部信息:[无]
报文内容:
{H:000000 1234567890 QINIU1234567890XMLQINIU.100.100.01 1234567890QINIU1234567890}
<?xml version="1.0" encoding="UTF-8" ?>
<Document xml="urn:123:456:789" xml:test="https://www.qiniu.com">

 <MytestConf>
  <MytestInfo>
   <MytestStr></MytestStr>
   <MytestStatus>400</MytestStatus>
   <Mytest>mytest.100.10.01</Mytest>
   <MsgId>123456789<?msgId>
  </MytestInfo>
 </MytestConf>
</Document>`)
	require.NotNil(t, multiLine)
	assert.NoError(t, err)
	assert.Equal(t,
		Data{
			"time":  "2017-03-28 12:34:56.123456",
			"time2": "2017-03-27 01:23:45.67",
			"ok1":   "ABCDEFGHIJK",
			"ok3":   "ABCDEFGHIJKLMNOPQRST",
			"a":     "H:000000",
			"b":     "1234567890",
			"c":     "QINIU1234567890XMLQINIU.100.100.01",
			"d":     "1234567890QINIU1234567890",
			"xml":   "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n<Document xml=\"urn:123:456:789\" xml:test=\"https://www.qiniu.com\">\n\n <MytestConf>\n  <MytestInfo>\n   <MytestStr></MytestStr>\n   <MytestStatus>400</MytestStatus>\n   <Mytest>mytest.100.10.01</Mytest>\n   <MsgId>123456789<?msgId>\n  </MytestInfo>\n </MytestConf>\n</Document>",
		},
		multiLine)
}

func TestNagiosLog(t *testing.T) {
	p := &Parser{
		Patterns: []string{"%{NAGIOSLOGLINE}", "%{NAGIOSLOGOTHER}"},
	}
	assert.NoError(t, p.compile())
	got, err := p.parse(`[1473609600] CURRENT HOST STATE: test_zzebgd;UP;HARD;1;PING OK - Packet loss = 0%, RTA = 0.26 ms`)
	assert.NoError(t, err)
	assert.Equal(t, Data{
		"nagios_epoch":     "1473609600",
		"nagios_type":      "CURRENT HOST STATE",
		"nagios_hostname":  "test_zzebgd",
		"nagios_state":     "UP",
		"nagios_statetype": "HARD",
		"nagios_statecode": "1",
		"nagios_message":   "PING OK - Packet loss = 0%, RTA = 0.26 ms",
	}, got)
	got, err = p.parse(`[1474520444] Auto-save of retention data completed successfully.`)
	assert.Equal(t, Data{
		"nagios_epoch": "1474520444",
		"nagios_log":   "Auto-save of retention data completed successfully.",
	}, got)
}
