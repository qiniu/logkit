package raw

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	. "github.com/qiniu/logkit/utils/models"
)

func Test_RawlogParser(t *testing.T) {
	c := conf.MapConf{}
	c[parser.KeyParserType] = "raw"
	c[parser.KeyLabels] = "machine nb110"
	c[parser.KeyDisableRecordErrData] = "true"
	p, err := NewParser(c)
	lines := []string{
		"Oct 31 17:56:02 dell sudo:  boponik : TTY=pts/13 ; PWD=/home/boponik ; USER=root ; COMMAND=/bin/cat /var/log/auth.log",
		`Oct 31 17:25:01 dell CRON[22418]: pam_unix(cron:session): session opened for user root by (uid=0)`,
		"Oct 31 17:35:22 dell NetworkManager[1119]: <warn> nl_recvmsgs() error: (-33) Dump inconsistency detected, interrupted",
		"Oct 31 17:35:01 dell CRON[23562]: (root) CMD (command -v debian-sa1 > /dev/null && debian-sa1 1 1)",
		"",
	}
	dts, err := p.Parse(lines)
	if st, ok := err.(*StatsError); ok {
		err = st.ErrorDetail
		assert.Equal(t, int64(0), st.Errors)
	}
	if err != nil {
		t.Error(err)
	}

	if len(dts) != 4 {
		t.Fatalf("parse lines error expect 4 lines but got %v lines", len(dts))
	}
	for _, dt := range dts {
		if len(dt) != 3 {
			t.Fatalf("parse line error expect 3 fields but got %v fields", len(dt))
		}
		if dt["machine"] != "nb110" {
			t.Fatalf("parse label error")
		}
	}
}

func Test_RawlogParserForErrData(t *testing.T) {
	c := conf.MapConf{}
	c[parser.KeyParserType] = "raw"
	c[parser.KeyLabels] = "machine nb110"
	c[parser.KeyDisableRecordErrData] = "false"
	p, err := NewParser(c)
	lines := []string{
		"Oct 31 17:56:02 dell sudo:  boponik : TTY=pts/13 ; PWD=/home/boponik ; USER=root ; COMMAND=/bin/cat /var/log/auth.log",
		"",
	}
	dts, err := p.Parse(lines)
	if st, ok := err.(*StatsError); ok {
		err = st.ErrorDetail
	}
	if err != nil {
		t.Error(err)
	}

	if len(dts) != 1 {
		t.Fatalf("parse lines error, expect 1 lines but got %v lines", len(dts))
	}
	if dts[0]["machine"] != "nb110" {
		t.Fatalf("parse label error")
	}
}
