package syslog

import (
	"errors"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/conf"
	. "github.com/qiniu/logkit/parser/config"
	. "github.com/qiniu/logkit/utils/models"
)

// 为了保证datasource不错乱，暂不支持多行采集，需要将socket采集中的获取方式改成按原始包读取
func Test_SyslogParser(t *testing.T) {
	c := conf.MapConf{}
	c[KeyParserType] = "syslog"
	c[KeyLabels] = "machine nb110"
	c[KeyKeepRawData] = "true"
	p, err := NewParser(c)
	assert.Nil(t, err)
	lines := []string{
		`<34>1 2003-10-11T22:14:15.003Z mymachine.example.com su - ID47- BOM'su root' failed for lonvick on /dev/pts/8`,
		`<38>Feb 05 01:02:03 abc system[253]: Listening at 0.0.0.0:3000`,
		`<1>Feb 05 01:02:03 abc system[23]: Listening at 0.0.0.0:3001`,
		`<165>1 2003-10-11T22:14:15.003Z mymachine.example.comevntslog - ID47 [exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"][examplePriority@32473class="high"]`,
		`<165>1 2003-10-11T22:14:15.003Z mymachine.example.comevntslog - ID47 [exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"] BOMAn applicationevent log entry...`,
		`<165>1 2003-08-24T05:14:15.000003-07:00 192.0.2.1myproc 8710 - - %% It's time to make the do-nuts.`,
		`<34>1 2003-10-11T22:14:15.003Z mymachine.example.com su - ID47- BOM'su root' failed for lonvick on /dev/pts/8`,
		PandoraParseFlushSignal,
	}
	dts, err := p.Parse(lines)
	assert.Nil(t, err)
	//indexes := []int{2, 3, 5, 9, 13, 15}
	expected := []string{
		`<34>1 2003-10-11T22:14:15.003Z mymachine.example.com su - ID47- BOM'su root' failed for lonvick on /dev/pts/8`,
		`<38>Feb 05 01:02:03 abc system[253]: Listening at 0.0.0.0:3000`,
		`<1>Feb 05 01:02:03 abc system[23]: Listening at 0.0.0.0:3001`,
		`<165>1 2003-10-11T22:14:15.003Z mymachine.example.comevntslog - ID47 [exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"][examplePriority@32473class="high"]`,
		`<165>1 2003-10-11T22:14:15.003Z mymachine.example.comevntslog - ID47 [exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"] BOMAn applicationevent log entry...`,
		`<165>1 2003-08-24T05:14:15.000003-07:00 192.0.2.1myproc 8710 - - %% It's time to make the do-nuts.`,
		`<34>1 2003-10-11T22:14:15.003Z mymachine.example.com su - ID47- BOM'su root' failed for lonvick on /dev/pts/8`,
	}
	assert.EqualValues(t, len(expected), len(dts))
	for i := range dts {
		assert.Equal(t, expected[i], dts[i][KeyRawData])
	}

	if len(dts) != 7 {
		t.Fatalf("parse lines error expect 7 lines but got %v lines", len(dts))
	}
	ndata, err := p.Parse([]string{PandoraParseFlushSignal})
	assert.Nil(t, err)
	dts = append(dts, ndata...)
	for _, dt := range dts {
		assert.Equal(t, "nb110", dt["machine"])
	}
}

func Test_SyslogParserError(t *testing.T) {
	c := conf.MapConf{}
	c[KeyParserType] = "syslog"
	p, err := NewParser(c)
	assert.Nil(t, err)
	line := "Test my syslog CRON[000]: (root) CMD"
	lines := []string{
		line,
		"!@#pandora-EOF-line#@!",
	}
	parsedData, err := p.Parse(lines)
	st, ok := err.(*StatsError)
	assert.True(t, ok)
	assert.Equal(t, "expecting a priority value within angle brackets [col 0]", st.LastError, st.LastError)
	assert.Equal(t, int64(1), st.Errors)
	assert.Equal(t, 1, len(parsedData))
	assert.Equal(t, line, parsedData[0]["pandora_stash"])
}

func TestSyslogParser_NoPanic(t *testing.T) {
	c := conf.MapConf{}
	c[KeyParserType] = "syslog"
	p, err := NewParser(c)
	assert.Nil(t, err)
	lines := []string{
		`<34>1 2003-10-11T22:14:15.003Z mymachine.example.com su - ID47`,
		`- BOM'su root' failed for lonvick on /dev/pts/8`,
		`<38>Feb 05 01:02:03 abc system[253]: Listening at 0.0.0.0:3000`,
		`<1>Feb 05 01:02:03 abc system[23]: Listening at 0.0.0.0`,
		`:3001`,
	}
	lenLines := len(lines)
	dataLine := make([]string, lenLines+1)
	for i := 0; i < lenLines; i++ {
		str := lines[i]
		lenStr := len(str)
		for j := 1; j <= lenStr; j++ {
			dataLine[i] = str[:j]
			dataLine[i+1] = PandoraParseFlushSignal
			p.Parse(dataLine)
		}
	}
}

func TestSyslogParser_NoMatch(t *testing.T) {
	c := conf.MapConf{}
	c[KeyParserType] = "syslog"
	c[KeySyslogMaxline] = "3"
	p, err := NewParser(c)
	assert.Nil(t, err)
	lines := []string{
		`003-10-11T22:14:15.003Z mymachine.example.com su - ID47`,
		`BOM'su root' failed for lonvick on /dev/pts/8`,
		`01:02:03 abc system[253]: Listening at 0.0.0.0:3000`,
		`1:02:03 abc system[23]: Listening at 0.0.0.0`,
		`:3001`,
		`:3002`,
		`:3003`,
	}
	_, err = p.Parse(lines)
	if st, ok := err.(*StatsError); ok {
		err = fmt.Errorf(st.LastError)
	}
	assert.Equal(t, errors.New("syslog meet max line 3, try to parse err expecting a priority value within angle brackets [col 0], check if this is standard rfc3164/rfc5424 syslog"), err)
}

func TestSyslogParser_TimeZone(t *testing.T) {
	c := conf.MapConf{}
	c[KeyParserType] = "syslog"
	c[KeyTimeZoneOffset] = "-8"
	p, err := NewParser(c)
	assert.Nil(t, err)
	lines := []string{
		`<1>Feb 05 01:02:03 abc system[253]: Listening at 0.0.0.0:3000`,
		`<34>1 2020-02-04T17:02:03.000Z mymachine.example.com su - ID47`,
		`<1>Feb 25 11:02:03 abc system[253]: Listening at 0.0.0.0:3000`,
		`<1>Feb 1 11:02:03 abc system[253]: Listening at 0.0.0.0:3000`,
		`<1>Feb 15 11:02:03 abc system[253]: Listening at 0.0.0.0:3000`,
		`<1>Jan 31 11:02:03 abc system[253]: Listening at 0.0.0.0:3000`,
	}
	dts, err := p.Parse(lines)
	assert.Nil(t, err)
	ndata, err := p.Parse([]string{PandoraParseFlushSignal})
	assert.Nil(t, err)
	dts = append(dts, ndata...)
	assert.Equal(t, strconv.Itoa(time.Now().Year())+"-02-04 17:02:03 +0000 UTC", dts[0]["timestamp"].(time.Time).String())
	assert.Equal(t, "2020-02-04 17:02:03 +0000 UTC", dts[1]["timestamp"].(time.Time).String())
	assert.Equal(t, strconv.Itoa(time.Now().Year())+"-02-25 03:02:03 +0000 UTC", dts[2]["timestamp"].(time.Time).String())
	assert.Equal(t, strconv.Itoa(time.Now().Year())+"-02-01 03:02:03 +0000 UTC", dts[3]["timestamp"].(time.Time).String())
	assert.Equal(t, strconv.Itoa(time.Now().Year())+"-02-15 03:02:03 +0000 UTC", dts[4]["timestamp"].(time.Time).String())
	assert.Equal(t, strconv.Itoa(time.Now().Year())+"-01-31 03:02:03 +0000 UTC", dts[5]["timestamp"].(time.Time).String())
}

func TestSyslogParser_ParseYear(t *testing.T) {
	c := conf.MapConf{}
	c[KeyParserType] = "syslog"
	c[KeyTimeZoneOffset] = "-8"
	c[KeyRFC3164ParseYear] = "true"
	p, err := NewParser(c)
	assert.Nil(t, err)
	lines := []string{
		`<34>Oct 11 22:14:15 2019 mymachine very.large.syslog.message.tag: 'su root' failed for lonvick on /dev/pts/8`,
	}
	dts, err := p.Parse(lines)
	assert.Nil(t, err)
	ndata, err := p.Parse([]string{PandoraParseFlushSignal})
	assert.Nil(t, err)
	dts = append(dts, ndata...)
	for _, dt := range dts {
		assert.Equal(t, "2019-10-11 14:14:15 +0000 UTC", dt["timestamp"].(time.Time).String())
	}

	c = conf.MapConf{}
	c[KeyParserType] = "syslog"
	c[KeyTimeZoneOffset] = "-8"
	c[KeyRFC3164ParseYear] = "false"
	p, err = NewParser(c)
	assert.Nil(t, err)
	lines = []string{
		`<34>Oct 11 22:14:15 2019 mymachine very.large.syslog.message.tag: 'su root' failed for lonvick on /dev/pts/8`,
	}
	dts, err = p.Parse(lines)
	assert.Nil(t, err)
	ndata, err = p.Parse([]string{PandoraParseFlushSignal})
	assert.Nil(t, err)
	dts = append(dts, ndata...)
	for _, dt := range dts {
		assert.Equal(t, strconv.Itoa(time.Now().Year())+"-10-11 14:14:15 +0000 UTC", dt["timestamp"].(time.Time).String())
	}

	c = conf.MapConf{}
	c[KeyParserType] = "syslog"
	c[KeyTimeZoneOffset] = "-8"
	c[KeyRFC3164ParseYear] = "false"
	p, err = NewParser(c)
	assert.Nil(t, err)
	lines = []string{
		`<34>Oct 11 22:14:15 mymachine very.large.syslog.message.tag: 'su root' failed for lonvick on /dev/pts/8`,
	}
	dts, err = p.Parse(lines)
	assert.Nil(t, err)
	ndata, err = p.Parse([]string{PandoraParseFlushSignal})
	assert.Nil(t, err)
	dts = append(dts, ndata...)
	for _, dt := range dts {
		assert.Equal(t, strconv.Itoa(time.Now().Year())+"-10-11 14:14:15 +0000 UTC", dt["timestamp"].(time.Time).String())
	}

	c = conf.MapConf{}
	c[KeyParserType] = "syslog"
	c[KeyTimeZoneOffset] = "-8"
	c[KeyRFC3164ParseYear] = "true"
	p, err = NewParser(c)
	assert.Nil(t, err)
	lines = []string{
		`<34>Oct 11 22:14:15 mymachine very.large.syslog.message.tag: 'su root' failed for lonvick on /dev/pts/8`,
	}
	dts, err = p.Parse(lines)
	assert.Nil(t, err)
	ndata, err = p.Parse([]string{PandoraParseFlushSignal})
	assert.Nil(t, err)
	dts = append(dts, ndata...)
	for _, dt := range dts {
		assert.Equal(t, time.Now().Month().String(), dt["timestamp"].(time.Time).Month().String())
	}
}
