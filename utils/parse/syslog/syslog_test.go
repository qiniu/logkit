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
