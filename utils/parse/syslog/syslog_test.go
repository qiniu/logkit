package syslog

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDetectSyslogType(t *testing.T) {
	tests := []struct {
		input string
		exp   int
	}{
		{
			input: "<34>1 2003-10-11T22:14:15.003Z mymachine.example.com su - ID47- BOM'su root' failed for lonvick on /dev/pts/8",
			exp:   DetectedRFC5424,
		},
		{
			input: "<34>Oct 11 22:14:15 mymachine su: 'su root' failed for lonvicn on /dev/pts/8",
			exp:   DetectedRFC3164,
		},
		{
			input: "<r> lonvicn on /dev/pts/8",
			exp:   DetectedLeftLog,
		},
	}
	for idx, v := range tests {
		got := DetectType([]byte(v.input))
		assert.Equal(t, v.exp, got, fmt.Sprintf("test index: %d", idx))
	}
}

func TestSyslogParser5424(t *testing.T) {
	fpas := &RFC5424{}
	buf := []byte("<34>1 2003-10-11T22:14:15.003Z mymachine.example.com \nsu - ID47 - BOM'su root' failed for lonvick on /dev/pts/8")
	pas := fpas.GetParser(buf)
	msg, err := pas.Parse(buf)
	assert.NotNil(t, err)

	fpas = &RFC5424{}
	buf = []byte(`<165>1 2003-08-24T05:14:15.000003-07:00 192.0.2.1
         myproc 8710 - - %% It's time to make the do-nuts.`)
	pas = fpas.GetParser(buf)
	msg, err = pas.Parse(buf)
	assert.NotNil(t, err)

	fpas = &RFC5424{}
	buf = []byte(`<165>1 2003-10-11T22:14:15.003Z mymachine.example.com evntslog - ID47 [exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"] BOMAn application
           event log entry..`)
	pas = fpas.GetParser(buf)
	msg, err = pas.Parse(buf)
	assert.NoError(t, err)
	//map[structured_data:[exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"] message:BOMAn application
	//           event log entry.. priority:165 hostname:mymachine.example.com app_name:evntslog proc_id:- msg_id:ID47 facility:20 severity:5 version:1 timestamp:2003-10-11 22:14:15.003 +0000 UTC]
	assert.Equal(t, uint8(165), msg["priority"])
	assert.Equal(t, uint8(20), msg["facility"])
	assert.Equal(t, uint8(5), msg["severity"])
	assert.Equal(t, uint16(1), msg["version"])
	assert.Equal(t, "mymachine.example.com", msg["hostname"])
	assert.Equal(t, "-", msg["proc_id"])
	assert.Equal(t, "BOMAn application\n           event log entry..", msg["message"])
	assert.Equal(t, "evntslog", msg["app_name"])
	assert.Equal(t, "ID47", msg["msg_id"])
	assert.Equal(t, map[string]map[string]string{"exampleSDID@32473": {"iut": "3", "eventSource": "Application", "eventID": "1011"}}, msg["structured_data"])
	assert.Equal(t, "2003-10-11 22:14:15.003 +0000 UTC", msg["timestamp"].(time.Time).String())

	fpas = &RFC5424{}
	buf = []byte(`<165>1 2003-10-11T22:14:15.003Z mymachine.example.com evntslog - ID47 [exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"][examplePriority@32473 class="high"]`)
	pas = fpas.GetParser(buf)
	msg, err = pas.Parse(buf)
	assert.NoError(t, err)
	// map[priority:165 facility:20 hostname:mymachine.example.com proc_id:- severity:5 version:1 timestamp:2003-10-11 22:14:15.003 +0000 UTC app_name:evntslog msg_id:ID47 structured_data:[exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"][examplePriority@32473 class="high"] message:]
	assert.Equal(t, uint8(165), msg["priority"])
	assert.Equal(t, uint8(20), msg["facility"])
	assert.Equal(t, uint8(5), msg["severity"])
	assert.Equal(t, uint16(1), msg["version"])
	assert.Equal(t, "mymachine.example.com", msg["hostname"])
	assert.Equal(t, "-", msg["proc_id"])
	assert.Equal(t, "", msg["message"])
	assert.Equal(t, "evntslog", msg["app_name"])
	assert.Equal(t, "ID47", msg["msg_id"])
	assert.Equal(t, map[string]map[string]string{"exampleSDID@32473": {"iut": "3", "eventSource": "Application", "eventID": "1011"}, "examplePriority@32473": {"class": "high"}}, msg["structured_data"])
	assert.Equal(t, "2003-10-11 22:14:15.003 +0000 UTC", msg["timestamp"].(time.Time).String())
}

func TestSyslogParser3164(t *testing.T) {
	// 空格切分
	fpas := &RFC3164{false}
	buf := []byte("<5>time:2020-05-18 11:10:30;danger_degree:2;breaking_sighn:0;event:[24482]多个应用application.ini数据库配置文件泄露漏洞;src_addr:115.238.89.35;src_port:35327;dst_addr:172.16.56.47;dst_port:80;proto:HTTP;user:")
	pas := fpas.GetParser(buf)
	msg, err := pas.Parse(buf)
	assert.NotNil(t, err)
	// 旧的驱动解析内容： map[hostname: tag: content:time:2020-05-18 11:10:30;danger_degree:2;breaking_sighn:0;event:[24482]多个应用application.ini数据库配置文件泄露漏洞;src_addr:115.238.89.35;src_port:35327;dst_addr:172.16.56.47;dst_port:80;proto:HTTP;user: priority:5 facility:0 severity:5 timestamp:2020-12-24 10:37:46 +0800 CST]
	assert.Equal(t, uint8(5), msg["priority"])
	assert.Equal(t, uint8(0), msg["facility"])
	assert.Equal(t, uint8(5), msg["severity"])
	assert.Equal(t, "", msg["hostname"])
	assert.Equal(t, "", msg["tag"])
	assert.Equal(t, time.Now().Day(), msg["timestamp"].(time.Time).Day())
	assert.Equal(t, `time:2020-05-18 11:10:30;danger_degree:2;breaking_sighn:0;event:[24482]多个应用application.ini数据库配置文件泄露漏洞;src_addr:115.238.89.35;src_port:35327;dst_addr:172.16.56.47;dst_port:80;proto:HTTP;user:`, msg["content"])

	buf = []byte("<34>Oct 11 22:14:15 mymachine very.large.syslog.message.tag: 'su root' failed for lonvick on /dev/pts/8")
	pas = fpas.GetParser(buf)
	msg, err = pas.Parse(buf)
	assert.Nil(t, err)
	//  map[timestamp:2020-10-11 22:14:15 +0000 UTC hostname:mymachine tag:very.large.syslog.message.tag content:'su root' failed for lonvick on /dev/pts/8 priority:34 facility:4 severity:2]
	assert.Equal(t, uint8(34), msg["priority"])
	assert.Equal(t, uint8(4), msg["facility"])
	assert.Equal(t, uint8(2), msg["severity"])
	assert.Equal(t, "mymachine", msg["hostname"])
	assert.Equal(t, `'su root' failed for lonvick on /dev/pts/8`, msg["content"])
	assert.Equal(t, `very.large.syslog.message.tag`, msg["tag"])
	assert.Equal(t, strconv.Itoa(time.Now().Year())+"-10-11 22:14:15 +0000 UTC", msg["timestamp"].(time.Time).String())

	fpas.parseYear = true
	buf = []byte("<34>Oct 11 22:14:15 2019 mymachine very.large.syslog.message.tag: 'su root' failed for lonvick on /dev/pts/8")
	pas1 := fpas.GetParser(buf)
	msg, err = pas1.Parse(buf)
	assert.Nil(t, err)
	// map[timestamp:2019-10-11 22:14:15 +0000 UTC hostname:mymachine tag:very.large.syslog.message.tag content:'su root' failed for lonvick on /dev/pts/8 priority:34 facility:4 severity:2]
	assert.Equal(t, uint8(34), msg["priority"])
	assert.Equal(t, uint8(4), msg["facility"])
	assert.Equal(t, uint8(2), msg["severity"])
	assert.Equal(t, "mymachine", msg["hostname"])
	assert.Equal(t, `'su root' failed for lonvick on /dev/pts/8`, msg["content"])
	assert.Equal(t, `very.large.syslog.message.tag`, msg["tag"])
	assert.Equal(t, "2019-10-11 22:14:15 +0000 UTC", msg["timestamp"].(time.Time).String())

	buf = []byte("<34>Oct 11 22:14:15 2019;")
	pas = fpas.GetParser(buf)
	msg, err = pas.Parse(buf)
	assert.NotNil(t, err)
	// map[content: priority:34 facility:4 severity:2 timestamp:2019-10-11 22:14:15 +0000 UTC hostname:; tag:]
	assert.Equal(t, uint8(34), msg["priority"])
	assert.Equal(t, uint8(4), msg["facility"])
	assert.Equal(t, uint8(2), msg["severity"])
	assert.Equal(t, "", msg["hostname"])
	assert.Equal(t, `;`, msg["content"])
	assert.Equal(t, "", msg["tag"])
	assert.Equal(t, "2019-10-11 22:14:15 +0000 UTC", msg["timestamp"].(time.Time).String())

	buf = []byte("<34>Oct 11 22:14:15 2019")
	pas = fpas.GetParser(buf)
	msg, err = pas.Parse(buf)
	assert.Nil(t, err)
	assert.Equal(t, uint8(34), msg["priority"])
	assert.Equal(t, uint8(4), msg["facility"])
	assert.Equal(t, uint8(2), msg["severity"])
	assert.Equal(t, "", msg["hostname"])
	assert.Equal(t, ``, msg["content"])
	assert.Equal(t, "", msg["tag"])
	assert.Equal(t, "2019-10-11 22:14:15 +0000 UTC", msg["timestamp"].(time.Time).String())
}

// github.com/influxdata/go-syslog
// Benchmark_RFC3164-4   	 1000000	      2138 ns/op
func Benchmark_RFC3164(b *testing.B) {
	fpas := &RFC3164{true}
	buf := []byte("<34>Oct 11 22:14:15 2019 mymachine very.large.syslog.message.tag: 'su root' failed for lonvick on /dev/pts/8")
	for n := 0; n < b.N; n++ {
		pas := fpas.GetParser(buf)
		pas.Parse(buf)
	}
}

// Benchmark_RFC5424-4   	  500000	      3338 ns/op
func Benchmark_RFC5424(b *testing.B) {
	fpas := &RFC5424{}
	buf := []byte(`<165>1 2003-10-11T22:14:15.003Z mymachine.example.com evntslog - ID47 [exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"] BOMAn application
           event log entry..`)
	for n := 0; n < b.N; n++ {
		pas := fpas.GetParser(buf)
		pas.Parse(buf)
	}
}
