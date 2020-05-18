package syslog

import (
	"fmt"
	"testing"

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
	pas := fpas.GetParser([]byte("<34>1 2003-10-11T22:14:15.003Z mymachine.example.com su - ID47\n- BOM'su root' failed for lonvick on /dev/pts/8"))

	fpas = &RFC5424{}
	pas = fpas.GetParser([]byte(`<165>1 2003-08-24T05:14:15.000003-07:00 192.0.2.1
         myproc 8710 - - %% It's time to make the do-nuts.`))

	fpas = &RFC5424{}
	pas = fpas.GetParser([]byte(`<165>1 2003-10-11T22:14:15.003Z mymachine.example.com evntslog - ID47 [exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"] BOMAn application
           event log entry..`))
	assert.NoError(t, pas.Parse())

	fpas = &RFC5424{}
	pas = fpas.GetParser([]byte(`<165>1 2003-10-11T22:14:15.003Z mymachine.example.com evntslog - ID47 [exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"][examplePriority@32473 class="high"]`))
	assert.NoError(t, pas.Parse())
}

func TestSyslogParser3164(t *testing.T) {
	fpas := &RFC3164{false}
	pas := fpas.GetParser([]byte("<5>time:2020-05-18 11:10:30;danger_degree:2;breaking_sighn:0;event:[24482]多个应用application.ini数据库配置文件泄露漏洞;src_addr:115.238.89.35;src_port:35327;dst_addr:172.16.56.47;dst_port:80;proto:HTTP;user:"))
	assert.Nil(t, pas.Parse())

	pas = fpas.GetParser([]byte("<34>Oct 11 22:14:15 mymachine very.large.syslog.message.tag: 'su root' failed for lonvick on /dev/pts/8"))
	assert.Nil(t, pas.Parse())

	fpas.parseYear = true
	pas = fpas.GetParser([]byte("<34>Oct 11 22:14:15 2019 mymachine very.large.syslog.message.tag: 'su root' failed for lonvick on /dev/pts/8"))
	assert.Nil(t, pas.Parse())
}
