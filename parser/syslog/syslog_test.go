package syslog

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	. "github.com/qiniu/logkit/utils/models"
)

func TestDetectSyslogType(t *testing.T) {
	tests := []struct {
		input string
		exp   int
	}{
		{
			input: "<34>1 2003-10-11T22:14:15.003Z mymachine.example.com su - ID47- BOM'su root' failed for lonvick on /dev/pts/8",
			exp:   detectedRFC5424,
		},
		{
			input: "<34>Oct 11 22:14:15 mymachine su: 'su root' failed for lonvicn on /dev/pts/8",
			exp:   detectedRFC3164,
		},
		{
			input: "<r> lonvicn on /dev/pts/8",
			exp:   detectedLeftLog,
		},
	}
	for idx, v := range tests {
		got := DetectType([]byte(v.input))
		assert.Equal(t, v.exp, got, fmt.Sprintf("test index: %d", idx))
	}
}

func Test_SyslogParser(t *testing.T) {
	c := conf.MapConf{}
	c[parser.KeyParserType] = "syslog"
	c[parser.KeyLabels] = "machine nb110"
	p, err := NewParser(c)
	lines := []string{
		`<34>1 2003-10-11T22:14:15.003Z mymachine.example.com su - ID47`,
		`- BOM'su root' failed for lonvick on /dev/pts/8`,

		`<38>Feb 05 01:02:03 abc system[253]: Listening at 0.0.0.0:3000`,
		`<1>Feb 05 01:02:03 abc system[23]: Listening at 0.0.0.0`,
		`:3001`,
		`<165>1 2003-10-11T22:14:15.003Z mymachine.example.com`,
		`evntslog - ID47 [exampleSDID@32473 iut="3" eventSource=`,
		`"Application" eventID="1011"][examplePriority@32473`,
		`class="high"]`,
		`<165>1 2003-10-11T22:14:15.003Z mymachine.example.com`,
		`evntslog - ID47 [exampleSDID@32473 iut="3" eventSource=`,
		`"Application" eventID="1011"] BOMAn application`,
		`event log entry...`,
		`<165>1 2003-08-24T05:14:15.000003-07:00 192.0.2.1`,
		`myproc 8710 - - %% It's time to make the do-nuts.`,

		`<34>1 2003-10-11T22:14:15.003Z mymachine.example.com su - ID47`,
		`- BOM'su root' failed for lonvick on /dev/pts/8`,
	}
	dts, err := p.Parse(lines)
	if st, ok := err.(*StatsError); ok {
		err = st.ErrorDetail
		assert.Equal(t, "", st.LastError, st.LastError)
		assert.Equal(t, int64(0), st.Errors)
	}
	if err != nil {
		t.Error(err)
	}

	if len(dts) != 6 {
		t.Fatalf("parse lines error expect 6 lines but got %v lines", len(dts))
	}
	ndata, err := p.Parse([]string{parser.PandoraParseFlushSignal})
	if st, ok := err.(*StatsError); ok {
		err = st.ErrorDetail
		assert.Equal(t, "", st.LastError, st.LastError)
		assert.Equal(t, int64(0), st.Errors)
	}
	assert.NoError(t, err)
	dts = append(dts, ndata...)
	for _, dt := range dts {
		assert.Equal(t, "nb110", dt["machine"])
	}
}

func Test_SyslogParserError(t *testing.T) {
	c := conf.MapConf{}
	c[parser.KeyParserType] = "syslog"
	p, err := NewParser(c)
	line := "Test my syslog CRON[000]: (root) CMD"
	lines := []string{
		line,
		"!@#pandora-EOF-line#@!",
	}
	parsedData, err := p.Parse(lines)
	st, ok := err.(*StatsError)
	assert.True(t, ok)
	assert.Equal(t, "No start char found for priority", st.LastError, st.LastError)
	assert.Equal(t, int64(1), st.Errors)
	assert.Equal(t, 1, len(parsedData))
	assert.Equal(t, line, parsedData[0]["pandora_stash"])
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

func TestSyslogParser_NoPanic(t *testing.T) {
	c := conf.MapConf{}
	c[parser.KeyParserType] = "syslog"
	p, err := NewParser(c)
	assert.NoError(t, err)
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
			dataLine[i+1] = parser.PandoraParseFlushSignal
			p.Parse(dataLine)
		}
	}
}

func TestSyslogParser_NoMatch(t *testing.T) {
	c := conf.MapConf{}
	c[parser.KeyParserType] = "syslog"
	c[parser.KeySyslogMaxline] = "3"
	p, err := NewParser(c)
	assert.NoError(t, err)
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
		err = st.ErrorDetail
	}
	assert.Equal(t, errors.New("syslog meet max line 3, try to parse err No start char found for priority, check if this is standard rfc3164/rfc5424 syslog"), err)
}
