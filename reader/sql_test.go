package reader

import (
	"os"
	"testing"
	"time"

	"github.com/qiniu/logkit/conf"

	"fmt"
	"reflect"

	"github.com/qiniu/logkit/utils"
	"github.com/stretchr/testify/assert"
)

func TestConvertMagic(t *testing.T) {
	now1, _ := time.Parse(time.RFC3339, "2017-04-01T06:06:09+08:00")
	now2, _ := time.Parse(time.RFC3339, "2017-11-11T16:16:29+08:00")
	tests := []struct {
		data string
		exp1 string
		exp2 string
	}{
		{
			data: "YYYY",
			exp1: "2017",
			exp2: "2017",
		},
		{
			data: "YY",
			exp1: "17",
			exp2: "17",
		},
		{
			data: "MM",
			exp1: "04",
			exp2: "11",
		},
		{
			data: "M",
			exp1: "4",
			exp2: "11",
		},
		{
			data: "D",
			exp1: "1",
			exp2: "11",
		},
		{
			data: "DD",
			exp1: "01",
			exp2: "11",
		},
		{
			data: "hh",
			exp1: "06",
			exp2: "16",
		},
		{
			data: "h",
			exp1: "6",
			exp2: "16",
		},
		{
			data: "mm",
			exp1: "06",
			exp2: "16",
		},
		{
			data: "m",
			exp1: "6",
			exp2: "16",
		},
		{
			data: "ss",
			exp1: "09",
			exp2: "29",
		},
		{
			data: "s",
			exp1: "9",
			exp2: "29",
		},
	}
	for _, ti := range tests {
		got := convertMagic(ti.data, now1)
		assert.EqualValues(t, ti.exp1, got)
		got = convertMagic(ti.data, now2)
		assert.EqualValues(t, ti.exp2, got)
	}
}

func TestGoMagic(t *testing.T) {
	now, _ := time.Parse(time.RFC3339, "2017-02-01T16:06:19+08:00")
	tests := []struct {
		data string
		exp  string
	}{
		{
			data: "select x@(MM)@(DD) from dbtable@(hh)-@(mm)",
			exp:  "select x0201 from dbtable16-06",
		},
		{
			data: "select x@(M)@(D) from dbtable@(h)-@(m)",
			exp:  "select x21 from dbtable16-6",
		},
		{
			data: "@(YY)",
			exp:  "17",
		},
		{
			data: "@(YYYY)@(MM)",
			exp:  "201702",
		},
		{
			data: "hhhhh",
			exp:  "hhhhh",
		},
	}
	for _, ti := range tests {
		got := goMagic(ti.data, now)
		assert.EqualValues(t, ti.exp, got)
	}
}

func TestSQLReader(t *testing.T) {
	logkitConf := conf.MapConf{
		KeyMetaPath: metaDir,
		KeyFileDone: metaDir,
		KeyMode:     ModeMysql,
	}
	meta, err := NewMetaWithConf(logkitConf)
	assert.NoError(t, err)
	defer os.RemoveAll(metaDir)
	database := "TestSQLReaderdatabase"
	mr := &SqlReader{
		database:  database,
		rawsqls:   "select * from mysql123  ;select * from mysql345;",
		syncSQLs:  []string{"select * from mysql123", "select * from mysql345"},
		readBatch: 100,
		meta:      meta,
		status:    StatusInit,
		offsetKey: "id",
		offsets:   []int64{123, 456},
		dbtype:    "mysql",
	}
	assert.Equal(t, "_"+database, mr.Source())

	// 测试meta备份和恢复
	mr.SyncMeta()
	gotoffsets, gotsqls, omit := restoreMeta(meta, mr.rawsqls)
	assert.EqualValues(t, mr.offsets, gotoffsets, "got offsets error")
	assert.EqualValues(t, mr.syncSQLs, gotsqls, "got sqls error")
	assert.EqualValues(t, false, omit)
	assert.EqualValues(t, "MYSQL_Reader:"+mr.database+"_"+hash(mr.rawsqls), mr.Name())

	// 测试更新Offset
	expoffsets := []int64{123, 0, 0}
	testsqls := []string{"select * from mysql123", "select * from mysql789", "select x from xx"}
	mr.updateOffsets(testsqls)
	assert.EqualValues(t, expoffsets, mr.offsets)
	mr.syncSQLs = testsqls

	//测试getSQL
	gotSQL, err := mr.getSQL(2)
	assert.NoError(t, err)
	assert.EqualValues(t, testsqls[2]+" WHERE id >= 0 AND id < 100;", gotSQL)
	mr.offsetKey = ""
	gotSQL, err = mr.getSQL(0)
	assert.NoError(t, err)
	assert.EqualValues(t, testsqls[0]+" LIMIT 123,223;", gotSQL)

	assert.Equal(t, utils.StatsInfo{}, mr.Status())
}

func TestUpdateSql(t *testing.T) {
	rawsqls := "select * from mysql123  ;select * from mysql345;"
	syncSQLs := []string{"select * from mysql123", "select * from mysql345"}
	got := updateSqls(rawsqls, time.Now())
	assert.EqualValues(t, syncSQLs, got)
}

func TestReflectTime(t *testing.T) {
	fmt.Println(reflect.TypeOf(int64(0)))
	fmt.Println(reflect.TypeOf(int32(0)))
	fmt.Println(reflect.TypeOf(int16(0)))
	fmt.Println(reflect.TypeOf(""))
	fmt.Println(reflect.TypeOf(false))
	fmt.Println(reflect.TypeOf(time.Time{}))
	fmt.Println(reflect.TypeOf([]byte{}))
	fmt.Println(reflect.TypeOf(new(interface{})).Elem())
}
