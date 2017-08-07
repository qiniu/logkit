package mgr

import (
	"testing"

	"github.com/qiniu/logkit/sender"
	"github.com/stretchr/testify/assert"
)

func Test_ParseRaw(t *testing.T) {
	ret, err := ParseRaw("this is a test line 1\nthis is a test line 2")
	if err != nil {
		return
	}
	if len(ret.SamplePoints) != 2 {
		t.Error("test ParseRaw fail")
	}
}

func Test_ParseJson(t *testing.T) {
	ret, _ := ParseJson(`{"client":"android","version":"2.0.0","system_name":"Android","system_version":"6.0.1"}`)

	if len(ret.SamplePoints[0]) != 4 {
		t.Errorf("test parse json parser fail\ngot:%v\nexp:%v\n", len(ret.SamplePoints), 4)
	}
}

func Test_ParseGrok(t *testing.T) {

	mode := "multi"
	pattern := "%{PHP_FPM_SLOW_LOG}"
	custom_pattern := `
            PHPLOGTIMESTAMP (%{MONTHDAY}-%{MONTH}-%{YEAR}|%{YEAR}-%{MONTHNUM}-%{MONTHDAY}) %{HOUR}:%{MINUTE}:%{SECOND}
			PHPTZ (%{WORD}\/%{WORD})
			PHPTIMESTAMP \[%{PHPLOGTIMESTAMP:timestamp}(?:\s+%{PHPTZ}|)\]
			PHPFPMPOOL \[pool %{WORD:pool}\]
			PHPFPMCHILD child %{NUMBER:childid}
			FPMERRORLOG \[%{PHPLOGTIMESTAMP:timestamp}\] %{WORD:type}: %{GREEDYDATA:message}
			PHPERRORLOG %{PHPTIMESTAMP} %{WORD:type} %{GREEDYDATA:message}

			PHP_FPM_SLOW_LOG (?m)^\[%{PHPLOGTIMESTAMP:timestamp}\]\s\s\[%{WORD:type}\s%{WORD}\]\s%{GREEDYDATA:message}$
		`
	sampleLog := `[05-May-2017 13:44:39]  [pool log] pid 4109
script_filename = /data/html/abc.com/index.php
[0x00007fec119d1720] curl_exec() /data/html/xyframework/base/XySoaClient.php:357
[0x00007fec119d1590] request_post() /data/html/xyframework/base/XySoaClient.php:284
[0x00007fff39d538b0] __call() unknown:0
[0x00007fec119d13a8] add() /data/html/abc.com/1/interface/ErrorLogInterface.php:70
[0x00007fec119d1298] log() /data/html/abc.com/1/interface/ErrorLogInterface.php:30
[0x00007fec119d1160] android() /data/html/xyframework/core/x.php:215
[0x00007fec119d0ff8] +++ dump failed`

	ret, err := ParseGrok(mode, pattern, custom_pattern, sampleLog)
	if err != nil {
		t.Error(err)
	}
	exp := PostParseRet{
		SamplePoints: []sender.Data{
			sender.Data{
				"timestamp": "05-May-2017 13:44:39",
				"type":      "pool",
				"message":   "pid 4109 script_filename = /data/html/abc.com/index.php [0x00007fec119d1720] curl_exec() /data/html/xyframework/base/XySoaClient.php:357 [0x00007fec119d1590] request_post() /data/html/xyframework/base/XySoaClient.php:284 [0x00007fff39d538b0] __call() unknown:0 [0x00007fec119d13a8] add() /data/html/abc.com/1/interface/ErrorLogInterface.php:70 [0x00007fec119d1298] log() /data/html/abc.com/1/interface/ErrorLogInterface.php:30 [0x00007fec119d1160] android() /data/html/xyframework/core/x.php:215 [0x00007fec119d0ff8] +++ dump failed",
			},
		},
	}
	assert.Equal(t, exp, ret)
}
