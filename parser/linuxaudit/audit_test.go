package linuxaudit

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	. "github.com/qiniu/logkit/parser/config"
	. "github.com/qiniu/logkit/utils/models"
)

func TestParse(t *testing.T) {
	tests := []struct {
		s          []string
		expectData []Data
	}{
		{
			expectData: []Data{},
		},
		{
			s: []string{`type=SYSCALL msg=audit(1364481363.243:24287): arch=c000003e syscall=2 success=no exit=-13 a0=7fffd19c5592 a1=0    a2=7fffd19c4b50`,
				`type=CWD msg='op=PAM:secret test1="a" res=success'
					cwd="/home/shadowman" `,
				`type=PATH msg=audit(1364481363.243:24287): item=0 name="/etc/ssh/sshd_config" inode=409248 dev=fd:00 dev=system_u:object_r:etc_t:s0`},
			expectData: []Data{
				{
					"arch":          "c000003e",
					"type":          "SYSCALL",
					"msg_timestamp": "2013-03-28T14:36:03.243Z",
					"msg_id":        "24287",
					"syscall":       "2",
					"success":       "no",
					"exit":          "-13",
					"a0":            "7fffd19c5592",
					"a1":            "0",
					"a2":            "7fffd19c4b50",
				},
				{
					"type": "CWD",
					"cwd":  "/home/shadowman",
					"msg":  map[string]interface{}{"op": "PAM:secret", "test1": "a", "res": "success"},
				},
				{
					"type":          "PATH",
					"msg_timestamp": "2013-03-28T14:36:03.243Z",
					"msg_id":        "24287",
					"item":          "0",
					"name":          "/etc/ssh/sshd_config",
					"inode":         "409248",
					"dev":           "fd:00",
					"dev_1":         "system_u:object_r:etc_t:s0",
				},
			},
		},
	}
	l, err := NewParser(conf.MapConf{
		"name": TypeLinuxAudit,
	})
	assert.Nil(t, err)
	for _, tt := range tests {
		got, err := l.Parse(tt.s)
		assert.Nil(t, err)
		assert.EqualValues(t, len(tt.expectData), len(got))
		for i, m := range got {
			assert.Equal(t, tt.expectData[i], m)
		}
	}

	got, err := l.Parse([]string{" ", "a"})
	assert.NotNil(t, err)
	assert.EqualValues(t, "success 0 errors 1 last error parsed no data by line a, send error detail <nil>", err.Error())
	assert.EqualValues(t, 0, len(got))

	lType, ok := l.(parser.ParserType)
	assert.True(t, ok)
	assert.EqualValues(t, TypeLinuxAudit, lType.Type())

	l, err = NewParser(conf.MapConf{
		"name":         TypeLinuxAudit,
		KeyKeepRawData: "true",
	})
	assert.Nil(t, err)
	assert.EqualValues(t, TypeLinuxAudit, l.Name())

	got, _ = l.Parse([]string{" ", "a", "msg=a"})
	assert.EqualValues(t, []Data{{"msg": "a", "raw_data": "msg=a"}}, got)

	lServer, ok := l.(parser.ServerParser)
	assert.True(t, ok)
	assert.EqualValues(t, map[string]interface{}{"process_at": "server", "key": "addr", "type": TypeLinuxAudit}, lServer.ServerConfig())
}

func Test_parseLine(t *testing.T) {
	tests := []struct {
		line       string
		expectData Data
	}{
		{
			expectData: Data{},
		},
		{
			line: `type=SYSCALL msg=audit(1364481363.243:24287): arch=c000003e syscall=2 success=no exit=-13 a0=7fffd19c5592 a1=0    a2=7fffd19c4b50`,
			expectData: Data{
				"arch":          "c000003e",
				"type":          "SYSCALL",
				"msg_timestamp": "2013-03-28T14:36:03.243Z",
				"msg_id":        "24287",
				"syscall":       "2",
				"success":       "no",
				"exit":          "-13",
				"a0":            "7fffd19c5592",
				"a1":            "0",
				"a2":            "7fffd19c4b50",
			},
		},
		{
			line: `type=CWD msg=audit(1364481363.243:24287) msg='op=PAM:secret test1="a" res=success b=  ' a=
					cwd="/home/shadowman" `,
			expectData: Data{
				"type":          "CWD",
				"msg_timestamp": "2013-03-28T14:36:03.243Z",
				"msg_id":        "24287",
				"cwd":           "/home/shadowman",
				"msg":           map[string]interface{}{"op": "PAM:secret", "test1": "a", "res": "success"},
			},
		},
		{
			line: `type=PATH msg=audit(1364481363.243:24287): item=0 name="/etc/ssh/sshd_config" inode=409248 dev=fd:00 dev=system_u:object_r:etc_t:s0`,
			expectData: Data{
				"type":          "PATH",
				"msg_timestamp": "2013-03-28T14:36:03.243Z",
				"msg_id":        "24287",
				"item":          "0",
				"name":          "/etc/ssh/sshd_config",
				"inode":         "409248",
				"dev":           "fd:00",
				"dev_1":         "system_u:object_r:etc_t:s0",
			},
		},
	}
	l := Parser{
		name: TypeLinuxAudit,
	}
	for _, tt := range tests {
		got, err := l.parse(tt.line)
		assert.Nil(t, err)
		assert.Equal(t, len(tt.expectData), len(got))
		for i, m := range got {
			assert.Equal(t, tt.expectData[i], m)
		}
	}
}
