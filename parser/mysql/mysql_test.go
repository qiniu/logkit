package mysql

import (
	"strings"
	"testing"
	"time"

	"github.com/qiniu/logkit/parser"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/conf"
	. "github.com/qiniu/logkit/parser/config"
	. "github.com/qiniu/logkit/utils/models"
)

var content = `# Time: 2017-12-24T02:42:00.126000Z
# User@Host: rdsadmin[rdsadmin] @ localhost [127.0.0.1]  Id:     3
# Query_time: 0.020363  Lock_time: 0.018450 Rows_sent: 0  Rows_examined: 1
SET timestamp=1514083320;
use foo;
SELECT count(*) from mysql.rds_replication_status WHERE master_host IS NOT NULL and master_port IS NOT NULL GROUP BY action_timestamp,called_by_user,action,mysql_version,master_host,master_port ORDER BY action_timestamp LIMIT 1;
#`

func TestMySqlLogParser1(t *testing.T) {
	p, err := NewParser(nil)
	assert.NoError(t, err)
	datas, err := p.Parse(strings.Split(content, "\n"))
	assert.Nil(t, err)

	expectedEvent := Data{
		"User":          "rdsadmin",
		"Host":          "localhost",
		"Database":      "foo",
		"Query_time":    float64(0.020363),
		"Lock_time":     float64(0.018450),
		"Rows_sent":     int64(0),
		"Rows_examined": int64(1),
		"Timestamp":     time.Unix(1514083320, 0).UTC(),
		"Statement":     "SELECT count(*) from mysql.rds_replication_status WHERE master_host IS NOT NULL and master_port IS NOT NULL GROUP BY action_timestamp,called_by_user,action,mysql_version,master_host,master_port ORDER BY action_timestamp LIMIT 1;",
	}
	assert.Equal(t, expectedEvent, datas[0])
}

func TestMySqlKeepRawData(t *testing.T) {
	p, err := NewParser(conf.MapConf{KeyKeepRawData: "true", KeyLabels: "name label_test"})
	assert.NoError(t, err)
	assert.EqualValues(t, "", p.Name())
	pType, ok := p.(parser.ParserType)
	assert.True(t, ok)
	assert.EqualValues(t, TypeMySQL, pType.Type())

	datas, err := p.Parse(strings.Split(content, "\n"))
	assert.Nil(t, err)

	expectedEvent := Data{
		"User":          "rdsadmin",
		"Host":          "localhost",
		"Database":      "foo",
		"Query_time":    float64(0.020363),
		"Lock_time":     float64(0.018450),
		"Rows_sent":     int64(0),
		"Rows_examined": int64(1),
		"Timestamp":     time.Unix(1514083320, 0).UTC(),
		"Statement":     "SELECT count(*) from mysql.rds_replication_status WHERE master_host IS NOT NULL and master_port IS NOT NULL GROUP BY action_timestamp,called_by_user,action,mysql_version,master_host,master_port ORDER BY action_timestamp LIMIT 1;",
		KeyRawData:      content,
		"name":          "label_test",
	}
	assert.Equal(t, expectedEvent, datas[0])

	got, err := p.Parse([]string{"a", PandoraParseFlushSignal})
	assert.Nil(t, err)
	assert.EqualValues(t, []Data{{"Statement": "a"}}, got)

	got, err = p.Parse(nil)
	assert.Nil(t, err)
	assert.EqualValues(t, 0, len(got))
}
