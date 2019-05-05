package raw

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	. "github.com/qiniu/logkit/parser/config"
)

func Test_RawlogParser(t *testing.T) {
	c := conf.MapConf{}
	c[KeyParserType] = "raw"
	c[KeyLabels] = "machine nb110"
	c[KeyDisableRecordErrData] = "true"
	p, err := NewParser(c)
	assert.Nil(t, err)
	assert.EqualValues(t, "", p.Name())
	pType, ok := p.(parser.ParserType)
	assert.True(t, ok)
	assert.EqualValues(t, TypeRaw, pType.Type())

	lines := []string{
		"Oct 31 17:56:02 dell sudo:  boponik : TTY=pts/13 ; PWD=/home/boponik ; USER=root ; COMMAND=/bin/cat /var/log/auth.log",
		`Oct 31 17:25:01 dell CRON[22418]: pam_unix(cron:session): session opened for user root by (uid=0)`,
		"Oct 31 17:35:22 dell NetworkManager[1119]: <warn> nl_recvmsgs() error: (-33) Dump inconsistency detected, interrupted",
		"Oct 31 17:35:01 dell CRON[23562]: (root) CMD (command -v debian-sa1 > /dev/null && debian-sa1 1 1)",
		"",
	}
	dts, err := p.Parse(lines)
	assert.NotNil(t, err)
	assert.Equal(t, len(lines)-1, len(dts))
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
	c[KeyParserType] = "raw"
	c[KeyLabels] = "machine nb110"
	c[KeyDisableRecordErrData] = "false"
	p, err := NewParser(c)
	assert.Nil(t, err)
	lines := []string{
		"Oct 31 17:56:02 dell sudo:  boponik : TTY=pts/13 ; PWD=/home/boponik ; USER=root ; COMMAND=/bin/cat /var/log/auth.log",
		"",
	}
	dts, err := p.Parse(lines)
	assert.NotNil(t, err)
	assert.Equal(t, len(lines)-1, len(dts))
	if dts[0]["machine"] != "nb110" {
		t.Fatalf("parse label error")
	}

	_, err = p.Parse([]string{"Oct 31 17:56:02 dell sudo:  boponik : TTY=pts/13 ; PWD=/home/boponik ; USER=root ; COMMAND=/bin/cat /var/log/auth.log"})
	assert.Nil(t, err)
}
