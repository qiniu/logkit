package sql

import (
	"fmt"
	"os"
	"path"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/reader/test"
	. "github.com/qiniu/logkit/utils/models"
)

var readRecords = DBRecords{
	"db1": TableRecords{
		Table: map[string]TableInfo{
			"db1_tb1": TableInfo{size: -1, offset: -1},
			"db1_tb2": TableInfo{size: -1, offset: -1},
			"db1_tb3": TableInfo{size: -1, offset: -1},
		},
		mutex: sync.RWMutex{},
	},
	"db2": TableRecords{
		Table: map[string]TableInfo{
			"db2_tb1": TableInfo{size: -1, offset: -1},
			"db2_tb2": TableInfo{size: -1, offset: -1},
			"db2_tb3": TableInfo{size: -1, offset: -1},
			"db2_tb4": TableInfo{size: -1, offset: -1},
			"db2_tb5": TableInfo{size: -1, offset: -1},
		},
		mutex: sync.RWMutex{},
	},
	"db3": TableRecords{
		Table: map[string]TableInfo{
			"db3_tb1": TableInfo{size: -1, offset: -1},
			"db3_tb2": TableInfo{size: -1, offset: -1},
		},
		mutex: sync.RWMutex{},
	},
}

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

func TestConvertMagicIndex(t *testing.T) {
	now1, _ := time.Parse(time.RFC3339, "2017-04-01T06:06:09+08:00")
	now2, _ := time.Parse(time.RFC3339, "2017-11-11T16:16:29+08:00")
	tests := []struct {
		data      string
		exp1      string
		exp2      string
		exp_index int
	}{
		{
			data:      "YYYY",
			exp1:      "2017",
			exp2:      "2017",
			exp_index: YEAR,
		},
		{
			data:      "YY",
			exp1:      "17",
			exp2:      "17",
			exp_index: YEAR,
		},
		{
			data:      "MM",
			exp1:      "04",
			exp2:      "11",
			exp_index: MONTH,
		},
		{
			data:      "DD",
			exp1:      "01",
			exp2:      "11",
			exp_index: DAY,
		},
		{
			data:      "hh",
			exp1:      "06",
			exp2:      "16",
			exp_index: HOUR,
		},
		{
			data:      "mm",
			exp1:      "06",
			exp2:      "16",
			exp_index: MINUTE,
		},
		{
			data:      "m",
			exp1:      "",
			exp2:      "",
			exp_index: -1,
		},
		{
			data:      "ss",
			exp1:      "09",
			exp2:      "29",
			exp_index: SECOND,
		},
		{
			data:      "s",
			exp1:      "",
			exp2:      "",
			exp_index: -1,
		},
	}
	for _, ti := range tests {
		got, gotIndex := convertMagicIndex(ti.data, now1)
		assert.Equal(t, ti.exp1, got)
		assert.Equal(t, ti.exp_index, gotIndex)
		got, gotIndex = convertMagicIndex(ti.data, now2)
		assert.Equal(t, ti.exp2, got)
		assert.Equal(t, ti.exp_index, gotIndex)
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

func TestGoMagicIndex(t *testing.T) {
	now, _ := time.Parse(time.RFC3339, "2017-02-01T16:06:19+08:00")
	tests := []struct {
		data           string
		exp_ret        string
		exp_startIndex []int
		exp_endIndex   []int
		exp_timeIndex  []int
	}{
		{
			data:           "x@(MM)abc@(DD)def",
			exp_ret:        "x02abc01def",
			exp_startIndex: []int{-1, 1, 6, -1, -1, -1},
			exp_endIndex:   []int{0, 3, 8, 0, 0, 0},
			exp_timeIndex:  []int{0, 1, 3, 6, 8, 11},
		},
		{
			data:           "x@(MM)abc@(DD)def*",
			exp_ret:        "x02abc01def*",
			exp_startIndex: []int{-1, 1, 6, -1, -1, -1},
			exp_endIndex:   []int{0, 3, 8, 0, 0, 0},
			exp_timeIndex:  []int{0, 1, 3, 6, 8, 11},
		},
		{
			data:           "x@(MM)abc@(DD)",
			exp_ret:        "x02abc01",
			exp_startIndex: []int{-1, 1, 6, -1, -1, -1},
			exp_endIndex:   []int{0, 3, 8, 0, 0, 0},
			exp_timeIndex:  []int{0, 1, 3, 6},
		},
		{
			data:           "x@(MM)@(DD)",
			exp_ret:        "x0201",
			exp_startIndex: []int{-1, 1, 3, -1, -1, -1},
			exp_endIndex:   []int{0, 3, 5, 0, 0, 0},
			exp_timeIndex:  []int{0, 1},
		},
		{
			data:           "x@(DD)@(MM)*",
			exp_ret:        "x0102*",
			exp_startIndex: []int{-1, 3, 1, -1, -1, -1},
			exp_endIndex:   []int{0, 5, 3, 0, 0, 0},
			exp_timeIndex:  []int{0, 1},
		},
		{
			data:           "@(YY)",
			exp_ret:        "17",
			exp_startIndex: []int{0, -1, -1, -1, -1, -1},
			exp_endIndex:   []int{2, 0, 0, 0, 0, 0},
			exp_timeIndex:  []int{0, 0},
		},
		{
			data:           "abcd@(YYYY)@(MM)efg*",
			exp_ret:        "abcd201702efg*",
			exp_startIndex: []int{4, 8, -1, -1, -1, -1},
			exp_endIndex:   []int{8, 10, 0, 0, 0, 0},
			exp_timeIndex:  []int{0, 4, 10, 13},
		},
		{
			data:           "abcd@(YYYY)@(MM)@(DD)@(hh)@(mm)@(ss)*",
			exp_ret:        "abcd20170201160619*",
			exp_startIndex: []int{4, 8, 10, 12, 14, 16},
			exp_endIndex:   []int{8, 10, 12, 14, 16, 18},
			exp_timeIndex:  []int{0, 4},
		},
		{
			data:           "hhhhh",
			exp_ret:        "hhhhh",
			exp_startIndex: []int{-1, -1, -1, -1, -1, -1},
			exp_endIndex:   []int{0, 0, 0, 0, 0, 0},
			exp_timeIndex:  []int{0, 5},
		},
	}
	for _, ti := range tests {
		ret, startIndex, endIndex, timeIndex, err := goMagicIndex(ti.data, now)
		assert.NoError(t, err)
		assert.EqualValues(t, ti.exp_ret, ret)
		assert.EqualValues(t, ti.exp_startIndex, startIndex)
		assert.EqualValues(t, ti.exp_endIndex, endIndex)
		assert.EqualValues(t, ti.exp_timeIndex, timeIndex)
	}

	err_data := "x@(M)@(DD)"
	exp_ret := "x@(M)@(DD)"
	ret, _, _, _, err := goMagicIndex(err_data, now)
	assert.Error(t, err)
	assert.EqualValues(t, exp_ret, ret)
}

func Test_getRemainStr(t *testing.T) {
	tests := []struct {
		origin        string
		timeIndex     []int
		expect_remain string
	}{
		{
			origin:        "x02abc01def",
			timeIndex:     []int{0, 1, 3, 6, 8, 11},
			expect_remain: "xabcdef",
		},
		{
			origin:        "x02abc01def*",
			timeIndex:     []int{0, 1, 3, 6, 8, 11},
			expect_remain: "xabcdef",
		},
		{
			origin:        "x02abc01",
			timeIndex:     []int{0, 1, 3, 6},
			expect_remain: "xabc",
		},
		{
			origin:        "x0201",
			timeIndex:     []int{0, 1},
			expect_remain: "x",
		},
		{
			origin:        "x0102*",
			timeIndex:     []int{0, 1},
			expect_remain: "x",
		},
		{
			origin:        "17",
			timeIndex:     []int{0, 0},
			expect_remain: "",
		},
		{
			origin:        "abcd201702efg*",
			timeIndex:     []int{0, 4, 10, 13},
			expect_remain: "abcdefg",
		},
		{
			origin:        "abcd20170201160619*",
			timeIndex:     []int{0, 4},
			expect_remain: "abcd",
		},
		{
			origin:        "hhhhh",
			timeIndex:     []int{0, 5},
			expect_remain: "hhhhh",
		},
	}
	for _, ti := range tests {
		remain := getRemainStr(ti.origin, ti.timeIndex)
		assert.Equal(t, ti.expect_remain, remain)
	}
}

func Test_matchRemainStr(t *testing.T) {
	tests := []struct {
		origin     string
		match      string
		matchData  string
		timeIndex  []int
		expect_res bool
	}{
		{
			origin:     "x02abc01def",
			match:      "xabcdef",
			matchData:  "x02abc01def",
			timeIndex:  []int{0, 1, 3, 6, 8, 11},
			expect_res: true,
		},
		{
			origin:     "x02abc01defdef",
			match:      "xabcdef",
			matchData:  "x02abc01def*",
			timeIndex:  []int{0, 1, 3, 6, 8, 12},
			expect_res: true,
		},
		{
			origin:     "x02abc01",
			match:      "xabc",
			matchData:  "x02abc01",
			timeIndex:  []int{0, 1, 3, 6},
			expect_res: true,
		},
		{
			origin:     "x0201",
			match:      "x",
			matchData:  "x0201",
			timeIndex:  []int{0, 1},
			expect_res: true,
		},
		{
			origin:     "x0102*",
			match:      "x*",
			matchData:  "x0102*",
			timeIndex:  []int{0, 1, 5, 6},
			expect_res: true,
		},
		{
			origin:     "17",
			match:      "",
			matchData:  "17",
			timeIndex:  []int{0, 0},
			expect_res: true,
		},
		{
			origin:     "abcd201702efg*",
			match:      "xabcdef",
			matchData:  "abcd201702efg*",
			timeIndex:  []int{0, 4, 10, 14},
			expect_res: false,
		},
		{
			origin:     "abcd20170201160619*",
			match:      "xabcdef",
			matchData:  "abcd20170201160619*",
			timeIndex:  []int{0, 4, 18, 19},
			expect_res: false,
		},
		{
			origin:     "abcd20170201160619ef",
			match:      "abcd",
			matchData:  "abcd20170201160619",
			timeIndex:  []int{0, 4},
			expect_res: false,
		},
		{
			origin:     "hhhhh",
			match:      "hhhhh",
			matchData:  "hhhhh",
			timeIndex:  []int{0, 5},
			expect_res: true,
		},
		{
			origin:     "hhhh",
			match:      "hhhhh",
			matchData:  "hhhhh",
			timeIndex:  []int{0, 5},
			expect_res: false,
		},
	}
	for _, ti := range tests {
		remain := matchRemainStr(ti.origin, ti.match, ti.matchData, ti.timeIndex)
		assert.Equal(t, ti.expect_res, remain)
	}
}

func Test_checkMagic(t *testing.T) {
	tests := []struct {
		data string
		exp  bool
	}{
		{
			data: "x@(DD)@(MM)*",
			exp:  true,
		},
		{
			data: "@(YY)",
			exp:  true,
		},
		{
			data: "abcd@(YYYY)@(M)efg*",
			exp:  false,
		},
		{
			data: "abcd@(YYYY)@(MM)@(DD)@(hh)@(mm)@(ss)efg*",
			exp:  true,
		},
		{
			data: "hhhhh",
			exp:  true,
		},
	}
	for _, ti := range tests {
		got := checkMagic(ti.data)
		assert.EqualValues(t, ti.exp, got)
	}
}

func TestSQLReader(t *testing.T) {
	meta, err := getMeta(MetaDir)
	assert.NoError(t, err)
	defer os.RemoveAll(MetaDir)
	database := "TestSQLReaderdatabase"
	mr := &Reader{
		rawDatabase: database,
		database:    database,
		rawsqls:     "select * from mysql123  ;select * from mysql345;",
		syncSQLs:    []string{"select * from mysql123", "select * from mysql345"},
		readBatch:   100,
		meta:        meta,
		offsetKey:   "id",
		offsets:     []int64{123, 456},
		dbtype:      "mysql",
	}
	assert.Equal(t, mr.dbtype+"_"+database, mr.Source())

	// 测试meta备份和恢复
	mr.SyncMeta()
	gotoffsets, gotsqls, omit := restoreMeta(meta, mr.rawsqls, 0)
	assert.EqualValues(t, mr.offsets, gotoffsets, "got offsets error")
	assert.EqualValues(t, mr.syncSQLs, gotsqls, "got sqls error")
	assert.EqualValues(t, false, omit)
	assert.EqualValues(t, "MYSQL_Reader:"+mr.database+"_"+Hash(mr.rawsqls), mr.Name())

	// 测试更新Offset
	expoffsets := []int64{123, 0, 0}
	testsqls := []string{"select * from mysql123", "select * from mysql789", "select x from xx"}
	mr.updateOffsets(testsqls)
	assert.EqualValues(t, expoffsets, mr.offsets)
	mr.syncSQLs = testsqls

	//测试getSQL
	gotSQL, err := mr.getSQL(2, mr.syncSQLs[2])
	assert.NoError(t, err)
	assert.EqualValues(t, testsqls[2]+" WHERE id >= 0 AND id < 100;", gotSQL)
	mr.offsetKey = ""
	gotSQL, err = mr.getSQL(0, mr.syncSQLs[0])
	assert.NoError(t, err)
	assert.EqualValues(t, testsqls[0], gotSQL)

	assert.Equal(t, StatsInfo{}, mr.Status())
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

func Test_getDefaultSql(t *testing.T) {
	database := "my_database"
	actualSql, err := getDefaultSql(database, "mysql")
	assert.NoError(t, err)
	expectSql := strings.Replace(DefaultMySQLTable, "DATABASE_NAME", database, -1)
	assert.Equal(t, actualSql, expectSql)

	actualSql, err = getDefaultSql(database, "postgres")
	assert.NoError(t, err)
	expectSql = strings.Replace(DefaultPGSQLTable, "DATABASE_NAME", database, -1)
	assert.Equal(t, actualSql, expectSql)

	actualSql, err = getDefaultSql(database, "mssql")
	assert.NoError(t, err)
	expectSql = strings.Replace(DefaultMsSQLTable, "DATABASE_NAME", database, -1)
	assert.Equal(t, actualSql, expectSql)
}

func Test_restoreMeta(t *testing.T) {
	meta, err := reader.NewMeta(MetaDir, MetaDir, "mysql", "logpath", "", 7)
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(MetaDir)

	file, offset, err := meta.ReadOffset()
	if err == nil {
		t.Error("offset must be nil")
	}
	encodeSQLs := make([]string, 0)
	syncSQLs := []string{"SELECT * FROM A", "SELECT * FROM B", "SELECT * FROM C"}
	offsets := []int64{1, 2, 3}
	for _, sql := range syncSQLs {
		encodeSQLs = append(encodeSQLs, strings.Replace(sql, " ", "@", -1))
	}
	for _, offset := range offsets {
		encodeSQLs = append(encodeSQLs, strconv.FormatInt(offset, 10))
	}
	all := strings.Join(encodeSQLs, sqlOffsetConnector)
	err = meta.WriteOffset(all, int64(len(syncSQLs)))
	if err != nil {
		t.Error(err)
	}
	file, offset, err = meta.ReadOffset()
	if file != all {
		t.Error("sql meta offset should be " + all)
	}
	if offset != 3 {
		t.Error("file offset should be 3")
	}

	var mgld time.Duration
	rawSqls := "SELECT * FROM A; SELECT * FROM B; SELECT * FROM C;"
	actualOffsets, actualSqls, omitMeta := restoreMeta(meta, rawSqls, mgld)
	assert.EqualValues(t, false, omitMeta)
	assert.EqualValues(t, offsets, actualOffsets)
	assert.EqualValues(t, syncSQLs, actualSqls)

	rawSqls = "SELECT * FROM A; SELECT * FROM B; SELECT * FROM D;"
	actualOffsets, actualSqls, omitMeta = restoreMeta(meta, rawSqls, mgld)
	assert.EqualValues(t, false, omitMeta)
	assert.EqualValues(t, offsets, actualOffsets)
	assert.EqualValues(t, actualSqls, actualSqls)

	rawSqls = "SELECT * FROM A; SELECT * FROM B;"
	actualOffsets, actualSqls, omitMeta = restoreMeta(meta, rawSqls, mgld)
	assert.EqualValues(t, true, omitMeta)
}

func Test_validTime(t *testing.T) {
	tests := []struct {
		data       string
		match      string
		startIndex []int
		endIndex   []int
		exp_res    bool
	}{
		{
			data:       "x02abc01",
			match:      "x02abc01",
			startIndex: []int{-1, 1, 6, -1, -1, -1},
			endIndex:   []int{0, 3, 8, 0, 0, 0},
			exp_res:    true,
		},
		{
			data:       "hhhhh",
			match:      "hhhhh",
			startIndex: []int{-1, -1, -1, -1, -1, -1},
			endIndex:   []int{0, 0, 0, 0, 0, 0},
			exp_res:    true,
		},
		{
			data:       "x01abc31def",
			match:      "x02abc01def",
			startIndex: []int{-1, 1, 6, -1, -1, -1},
			endIndex:   []int{0, 3, 8, 0, 0, 0},
			exp_res:    true,
		},
		{
			data:       "x01abc31def*",
			match:      "x02abc01def*",
			startIndex: []int{-1, 1, 6, -1, -1, -1},
			endIndex:   []int{0, 3, 8, 0, 0, 0},
			exp_res:    true,
		},
		{
			data:       "x0201",
			match:      "x0201",
			startIndex: []int{-1, 1, 3, -1, -1, -1},
			endIndex:   []int{0, 3, 5, 0, 0, 0},
			exp_res:    true,
		},
		{
			data:       "x0102abc",
			match:      "x0102*",
			startIndex: []int{-1, 3, 1, -1, -1, -1},
			endIndex:   []int{0, 5, 3, 0, 0, 0},
			exp_res:    true,
		},
		{
			data:       "x0102",
			match:      "x0102*",
			startIndex: []int{-1, 3, 1, -1, -1, -1},
			endIndex:   []int{0, 5, 3, 0, 0, 0},
			exp_res:    true,
		},
		{
			data:       "17",
			match:      "17",
			startIndex: []int{0, -1, -1, -1, -1, -1},
			endIndex:   []int{2, 0, 0, 0, 0, 0},
			exp_res:    true,
		},
		{
			data:       "abcd201703efg*",
			match:      "abcd201702efg*",
			startIndex: []int{4, 8, -1, -1, -1, -1},
			endIndex:   []int{8, 10, 0, 0, 0, 0},
			exp_res:    false,
		},
		{
			data:       "abcd20170201160618*",
			match:      "abcd20170201160619*",
			startIndex: []int{4, 8, 10, 12, 14, 16},
			endIndex:   []int{8, 10, 12, 14, 16, 18},
			exp_res:    true,
		},
	}
	for _, ti := range tests {
		valid := validTime(ti.data, ti.match, ti.startIndex, ti.endIndex, true)
		assert.EqualValues(t, ti.exp_res, valid)
	}

	tests2 := []struct {
		data       string
		match      string
		startIndex []int
		endIndex   []int
		exp_res    bool
	}{
		{
			data:       "x02abc01",
			match:      "x02abc01",
			startIndex: []int{-1, 1, 6, -1, -1, -1},
			endIndex:   []int{0, 3, 8, 0, 0, 0},
			exp_res:    true,
		},
		{
			data:       "hhhhh",
			match:      "hhhhh",
			startIndex: []int{-1, -1, -1, -1, -1, -1},
			endIndex:   []int{0, 0, 0, 0, 0, 0},
			exp_res:    true,
		},
		{
			data:       "x01abc31def",
			match:      "x02abc01def",
			startIndex: []int{-1, 1, 6, -1, -1, -1},
			endIndex:   []int{0, 3, 8, 0, 0, 0},
			exp_res:    false,
		},
		{
			data:       "x01abc31def*",
			match:      "x02abc01def*",
			startIndex: []int{-1, 1, 6, -1, -1, -1},
			endIndex:   []int{0, 3, 8, 0, 0, 0},
			exp_res:    false,
		},
		{
			data:       "x0201",
			match:      "x0201",
			startIndex: []int{-1, 1, 3, -1, -1, -1},
			endIndex:   []int{0, 3, 5, 0, 0, 0},
			exp_res:    true,
		},
		{
			data:       "x0102abc",
			match:      "x0102*",
			startIndex: []int{-1, 3, 1, -1, -1, -1},
			endIndex:   []int{0, 5, 3, 0, 0, 0},
			exp_res:    true,
		},
		{
			data:       "x0102",
			match:      "x0102*",
			startIndex: []int{-1, 3, 1, -1, -1, -1},
			endIndex:   []int{0, 5, 3, 0, 0, 0},
			exp_res:    true,
		},
		{
			data:       "17",
			match:      "17",
			startIndex: []int{0, -1, -1, -1, -1, -1},
			endIndex:   []int{2, 0, 0, 0, 0, 0},
			exp_res:    true,
		},
		{
			data:       "abcd201703efg*",
			match:      "abcd201702efg*",
			startIndex: []int{4, 8, -1, -1, -1, -1},
			endIndex:   []int{8, 10, 0, 0, 0, 0},
			exp_res:    true,
		},
		{
			data:       "abcd20170201160618*",
			match:      "abcd20170201160619*",
			startIndex: []int{4, 8, 10, 12, 14, 16},
			endIndex:   []int{8, 10, 12, 14, 16, 18},
			exp_res:    false,
		},
	}
	for _, ti := range tests2 {
		valid := validTime(ti.data, ti.match, ti.startIndex, ti.endIndex, false)
		assert.EqualValues(t, ti.exp_res, valid)
	}
}

func Test_equalTime(t *testing.T) {
	tests := []struct {
		data       string
		match      string
		startIndex []int
		endIndex   []int
		exp_res    bool
	}{
		{
			data:       "x02abc01",
			match:      "x02abc01",
			startIndex: []int{-1, 1, 6, -1, -1, -1},
			endIndex:   []int{0, 3, 8, 0, 0, 0},
			exp_res:    true,
		},
		{
			data:       "hhhhh",
			match:      "hhhhh",
			startIndex: []int{-1, -1, -1, -1, -1, -1},
			endIndex:   []int{0, 0, 0, 0, 0, 0},
			exp_res:    true,
		},
		{
			data:       "x01abc31def",
			match:      "x02abc01def",
			startIndex: []int{-1, 1, 6, -1, -1, -1},
			endIndex:   []int{0, 3, 8, 0, 0, 0},
			exp_res:    false,
		},
		{
			data:       "x0201",
			match:      "x0201",
			startIndex: []int{-1, 1, 3, -1, -1, -1},
			endIndex:   []int{0, 3, 5, 0, 0, 0},
			exp_res:    true,
		},
		{
			data:       "17",
			match:      "17",
			startIndex: []int{0, -1, -1, -1, -1, -1},
			endIndex:   []int{2, 0, 0, 0, 0, 0},
			exp_res:    true,
		},
		{
			data:       "abcd201703efg*",
			match:      "abcd201702efg*",
			startIndex: []int{4, 8, -1, -1, -1, -1},
			endIndex:   []int{8, 10, 0, 0, 0, 0},
			exp_res:    false,
		},
		{
			data:       "abcd20170201160618*",
			match:      "abcd20170201160619*",
			startIndex: []int{4, 8, 10, 12, 14, 16},
			endIndex:   []int{8, 10, 12, 14, 16, 18},
			exp_res:    false,
		},
	}
	for _, ti := range tests {
		valid := equalTime(ti.data, ti.match, ti.startIndex, ti.endIndex)
		assert.EqualValues(t, ti.exp_res, valid)
	}
}

func Test_isMatchData(t *testing.T) {
	meta, err := getMeta(MetaDir)
	assert.NoError(t, err)
	defer os.RemoveAll(MetaDir)
	mr := &Reader{
		readBatch:         100,
		meta:              meta,
		dbtype:            "mysql",
		historyAll:        false,
		loop:              true,
		cronSchedule:      false,
		omitDoneDBRecords: true,
	}

	tests := []struct {
		data           string
		matchData      string
		matchStr       string
		exp_ret        bool
		exp_startIndex []int
		exp_endIndex   []int
		exp_timeIndex  []int
	}{
		{
			data:           "x02abc01def",
			matchData:      "xabcdef",
			matchStr:       "x02abc01def",
			exp_ret:        true,
			exp_startIndex: []int{-1, 1, 6, -1, -1, -1},
			exp_endIndex:   []int{0, 3, 8, 0, 0, 0},
			exp_timeIndex:  []int{0, 1, 3, 6, 8, 11},
		},
		{
			data:           "x01abc01def",
			matchData:      "xabcdef",
			matchStr:       "x02abc01def",
			exp_ret:        false,
			exp_startIndex: []int{-1, 1, 6, -1, -1, -1},
			exp_endIndex:   []int{0, 3, 8, 0, 0, 0},
			exp_timeIndex:  []int{0, 1, 3, 6, 8, 11},
		},
		{
			data:           "x02abc01defdef",
			matchData:      "xabcdef",
			matchStr:       "x02abc01def*",
			exp_ret:        true,
			exp_startIndex: []int{-1, 1, 6, -1, -1, -1},
			exp_endIndex:   []int{0, 3, 8, 0, 0, 0},
			exp_timeIndex:  []int{0, 1, 3, 6, 8, 11},
		},
		{
			data:           "abc",
			matchData:      "xabc",
			matchStr:       "x02abc01",
			exp_ret:        false,
			exp_startIndex: []int{-1, 1, 6, -1, -1, -1},
			exp_endIndex:   []int{0, 3, 8, 0, 0, 0},
			exp_timeIndex:  []int{0, 1, 3, 6},
		},
		{
			data:           "x0201",
			matchData:      "x",
			matchStr:       "x0201*",
			exp_ret:        true,
			exp_startIndex: []int{-1, 1, 3, -1, -1, -1},
			exp_endIndex:   []int{0, 3, 5, 0, 0, 0},
			exp_timeIndex:  []int{0, 1},
		},
		{
			data:           "x0102xxxxx",
			matchData:      "x",
			matchStr:       "x0102*",
			exp_ret:        true,
			exp_startIndex: []int{-1, 3, 1, -1, -1, -1},
			exp_endIndex:   []int{0, 5, 3, 0, 0, 0},
			exp_timeIndex:  []int{0, 1},
		},
		{
			data:           "17",
			matchData:      "",
			matchStr:       "17",
			exp_ret:        true,
			exp_startIndex: []int{0, -1, -1, -1, -1, -1},
			exp_endIndex:   []int{2, 0, 0, 0, 0, 0},
			exp_timeIndex:  []int{0, 0},
		},
		{
			data:           "abcd201702efg",
			matchData:      "abcdefg",
			matchStr:       "abcd201702efg*",
			exp_ret:        true,
			exp_startIndex: []int{4, 8, -1, -1, -1, -1},
			exp_endIndex:   []int{8, 10, 0, 0, 0, 0},
			exp_timeIndex:  []int{0, 4, 10, 13},
		},
		{
			data:           "abcd20170201160619abc",
			matchData:      "abcd",
			matchStr:       "abcd20170201160619",
			exp_ret:        false,
			exp_startIndex: []int{4, 8, 10, 12, 14, 16},
			exp_endIndex:   []int{8, 10, 12, 14, 16, 18},
			exp_timeIndex:  []int{0, 4},
		},
		{
			data:           "hhhhh",
			matchData:      "hhhhh",
			matchStr:       "hhhhh",
			exp_ret:        true,
			exp_startIndex: []int{-1, -1, -1, -1, -1, -1},
			exp_endIndex:   []int{0, 0, 0, 0, 0, 0},
			exp_timeIndex:  []int{0, 5},
		},
	}
	for _, ti := range tests {
		ret := mr.isMatchData(DATABASE, "", ti.data, ti.matchData, ti.matchStr, ti.exp_timeIndex, ti.exp_startIndex, ti.exp_endIndex)
		assert.EqualValues(t, ti.exp_ret, ret)
	}
}

func Test_getCheckAll(t *testing.T) {
	mr := &Reader{
		database:    "Test_getCheckHistory",
		dbtype:      "mysql",
		historyAll:  true,
		rawTable:    "*",
		rawDatabase: "*",
	}

	tests := []struct {
		queryType int
		exp_res   bool
	}{
		{
			queryType: TABLE,
			exp_res:   true,
		},
		{
			queryType: COUNT,
			exp_res:   true,
		},
		{
			queryType: DATABASE,
			exp_res:   true,
		},
	}

	for _, test := range tests {
		checkHistory, err := mr.getCheckAll(test.queryType)
		assert.NoError(t, err)
		assert.EqualValues(t, test.exp_res, checkHistory)
	}
}

func Test_getRawSqls(t *testing.T) {
	tests := []struct {
		queryType int
		exp_sqls  string
	}{
		{
			queryType: TABLE,
			exp_sqls:  "Select * From `my_table`;",
		},
		{
			queryType: COUNT,
			exp_sqls:  "Select Count(*) From `my_table`;",
		},
		{
			queryType: DATABASE,
			exp_sqls:  "",
		},
	}

	for _, test := range tests {
		sqls, err := getRawSqls(test.queryType, "my_table")
		assert.NoError(t, err)
		assert.EqualValues(t, test.exp_sqls, sqls)
	}
}

func Test_getConnectStr(t *testing.T) {
	now := time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)

	mr := &Reader{
		database:    "Test_getCheckConnectStr",
		dbtype:      "mysql",
		historyAll:  true,
		rawTable:    "*",
		rawDatabase: "*",
		datasource:  "root:@tcp(127.0.0.1:3306)",
		encoder:     "utf8",
	}
	connectStr, err := mr.getConnectStr("", now)
	assert.NoError(t, err)
	expectMySql := "root:@tcp(127.0.0.1:3306)/?charset=utf8"
	assert.Equal(t, expectMySql, connectStr)

	mrMssql := &Reader{
		database:    "Test_getCheckConnectStr",
		dbtype:      "mssql",
		rawTable:    "*",
		datasource:  `server=localhost\SQLExpress;user id=sa;password=PassWord;port=1433`,
		encoder:     "utf8",
		rawDatabase: "Test_getCheckConnectStr",
	}
	connectStr, err = mrMssql.getConnectStr("", now)
	assert.NoError(t, err)
	expectMsSql := `server=localhost\SQLExpress;user id=sa;password=PassWord;port=1433;database=Test_getCheckConnectStr`
	assert.Equal(t, expectMsSql, connectStr)

	mrPostgressql := &Reader{
		database:    "Test_getCheckConnectStr",
		dbtype:      "postgres",
		rawTable:    "*",
		datasource:  "host=localhost port=5432 connect_timeout=10 user=pqgotest password=123456 sslmode=disable",
		encoder:     "utf8",
		rawDatabase: "Test_getCheckConnectStr",
	}
	connectStr, err = mrPostgressql.getConnectStr("", now)
	assert.NoError(t, err)
	expectPostgresSql := "host=localhost port=5432 connect_timeout=10 user=pqgotest password=123456 sslmode=disable dbname=Test_getCheckConnectStr"
	assert.Equal(t, expectPostgresSql, connectStr)
}

func Test_WriteRecordsFile(t *testing.T) {
	meta, err := reader.NewMeta(MetaDir, MetaDir, "mysql", "logpath", "", 7)
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(MetaDir)

	err = WriteRecordsFile(meta.DoneFilePath, getContent(readRecords))
	assert.NoError(t, err)
}

func Test_restoreRecordsFile(t *testing.T) {
	meta, err := reader.NewMeta(MetaDir, MetaDir, "mysql", "logpath", "", 7)
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(MetaDir)

	tests := []struct {
		set                   DBRecords
		exp_res               DBRecords
		exp_omitDoneDBRecords bool
	}{
		{
			set:                   DBRecords{},
			exp_res:               nil,
			exp_omitDoneDBRecords: true,
		},
		{
			set:                   readRecords,
			exp_res:               readRecords,
			exp_omitDoneDBRecords: false,
		},
		{
			set: DBRecords{
				"db1": TableRecords{
					Table: map[string]TableInfo{
						"db1_tb1":  {size: -1, offset: -1},
						"db1_tb10": {size: -1, offset: -1},
					},
					mutex: sync.RWMutex{},
				},
				"db4": TableRecords{
					Table: map[string]TableInfo{
						"db4_tb10": TableInfo{size: -1, offset: -1},
					},
					mutex: sync.RWMutex{},
				},
			},
			exp_res: DBRecords{
				"db1": TableRecords{
					Table: map[string]TableInfo{
						"db1_tb1":  {size: -1, offset: -1},
						"db1_tb2":  {size: -1, offset: -1},
						"db1_tb3":  {size: -1, offset: -1},
						"db1_tb10": {size: -1, offset: -1},
					},
					mutex: sync.RWMutex{},
				},
				"db2": TableRecords{
					Table: map[string]TableInfo{
						"db2_tb1": TableInfo{size: -1, offset: -1},
						"db2_tb2": TableInfo{size: -1, offset: -1},
						"db2_tb3": TableInfo{size: -1, offset: -1},
						"db2_tb4": TableInfo{size: -1, offset: -1},
						"db2_tb5": TableInfo{size: -1, offset: -1},
					},
					mutex: sync.RWMutex{},
				},
				"db3": TableRecords{
					Table: map[string]TableInfo{
						"db3_tb1": TableInfo{size: -1, offset: -1},
						"db3_tb2": TableInfo{size: -1, offset: -1},
					},
					mutex: sync.RWMutex{},
				},
				"db4": TableRecords{
					Table: map[string]TableInfo{
						"db4_tb10": TableInfo{size: -1, offset: -1},
					},
					mutex: sync.RWMutex{},
				},
			},
			exp_omitDoneDBRecords: false,
		},
		{
			set: DBRecords{
				"db1": TableRecords{
					Table: map[string]TableInfo{
						"db1_tb1":  TableInfo{size: -1, offset: -1},
						"db1_tb10": TableInfo{size: -1, offset: -1},
					},
					mutex: sync.RWMutex{},
				},
				"db4": TableRecords{
					Table: map[string]TableInfo{
						"db4_tb10": TableInfo{size: -1, offset: -1},
					},
					mutex: sync.RWMutex{},
				},
			},
			exp_res: DBRecords{
				"db1": TableRecords{
					Table: map[string]TableInfo{
						"db1_tb1":  TableInfo{size: -1, offset: -1},
						"db1_tb2":  TableInfo{size: -1, offset: -1},
						"db1_tb3":  TableInfo{size: -1, offset: -1},
						"db1_tb10": TableInfo{size: -1, offset: -1},
					},
					mutex: sync.RWMutex{},
				},
				"db2": TableRecords{
					Table: map[string]TableInfo{
						"db2_tb1": TableInfo{size: -1, offset: -1},
						"db2_tb2": TableInfo{size: -1, offset: -1},
						"db2_tb3": TableInfo{size: -1, offset: -1},
						"db2_tb4": TableInfo{size: -1, offset: -1},
						"db2_tb5": TableInfo{size: -1, offset: -1},
					},
					mutex: sync.RWMutex{},
				},
				"db3": TableRecords{
					Table: map[string]TableInfo{
						"db3_tb1": TableInfo{size: -1, offset: -1},
						"db3_tb2": TableInfo{size: -1, offset: -1},
					},
					mutex: sync.RWMutex{},
				},
				"db4": TableRecords{
					Table: map[string]TableInfo{
						"db4_tb10": TableInfo{size: -1, offset: -1},
					},
					mutex: sync.RWMutex{},
				},
			},
			exp_omitDoneDBRecords: false,
		},
		{
			set: DBRecords{
				"db1": TableRecords{
					Table: map[string]TableInfo{
						"db1_tb10": TableInfo{size: -1, offset: -1},
					},
					mutex: sync.RWMutex{},
				},
				"db3": TableRecords{
					Table: map[string]TableInfo{
						"db3_tb10": TableInfo{size: -1, offset: -1},
					},
					mutex: sync.RWMutex{},
				},
			},
			exp_res: DBRecords{
				"db1": TableRecords{
					Table: map[string]TableInfo{
						"db1_tb1":  TableInfo{size: -1, offset: -1},
						"db1_tb2":  TableInfo{size: -1, offset: -1},
						"db1_tb3":  TableInfo{size: -1, offset: -1},
						"db1_tb10": TableInfo{size: -1, offset: -1},
					},
					mutex: sync.RWMutex{},
				},
				"db2": TableRecords{
					Table: map[string]TableInfo{
						"db2_tb1": TableInfo{size: -1, offset: -1},
						"db2_tb2": TableInfo{size: -1, offset: -1},
						"db2_tb3": TableInfo{size: -1, offset: -1},
						"db2_tb4": TableInfo{size: -1, offset: -1},
						"db2_tb5": TableInfo{size: -1, offset: -1},
					},
					mutex: sync.RWMutex{},
				},
				"db3": TableRecords{
					Table: map[string]TableInfo{
						"db3_tb1":  TableInfo{size: -1, offset: -1},
						"db3_tb2":  TableInfo{size: -1, offset: -1},
						"db3_tb10": TableInfo{size: -1, offset: -1},
					},
					mutex: sync.RWMutex{},
				},
				"db4": TableRecords{
					Table: map[string]TableInfo{
						"db4_tb10": TableInfo{size: -1, offset: -1},
					},
					mutex: sync.RWMutex{},
				},
			},
			exp_omitDoneDBRecords: false,
		},
		{
			set: readRecords,
			exp_res: DBRecords{
				"db1": TableRecords{
					Table: map[string]TableInfo{
						"db1_tb1":  TableInfo{size: -1, offset: -1},
						"db1_tb2":  TableInfo{size: -1, offset: -1},
						"db1_tb3":  TableInfo{size: -1, offset: -1},
						"db1_tb10": TableInfo{size: -1, offset: -1},
					},
					mutex: sync.RWMutex{},
				},
				"db2": TableRecords{
					Table: map[string]TableInfo{
						"db2_tb1": TableInfo{size: -1, offset: -1},
						"db2_tb2": TableInfo{size: -1, offset: -1},
						"db2_tb3": TableInfo{size: -1, offset: -1},
						"db2_tb4": TableInfo{size: -1, offset: -1},
						"db2_tb5": TableInfo{size: -1, offset: -1},
					},
					mutex: sync.RWMutex{},
				},
				"db3": TableRecords{
					Table: map[string]TableInfo{
						"db3_tb1":  TableInfo{size: -1, offset: -1},
						"db3_tb2":  TableInfo{size: -1, offset: -1},
						"db3_tb10": TableInfo{size: -1, offset: -1},
					},
					mutex: sync.RWMutex{},
				},
				"db4": TableRecords{
					Table: map[string]TableInfo{
						"db4_tb10": TableInfo{size: -1, offset: -1},
					},
					mutex: sync.RWMutex{},
				},
			},
			exp_omitDoneDBRecords: false,
		},
	}

	for _, test := range tests {
		err = WriteRecordsFile(meta.DoneFilePath, getContent(test.set))
		assert.NoError(t, err)

		var records SyncDBRecords
		_, _, omitDoneDBRecords := records.restoreRecordsFile(meta)
		assert.EqualValues(t, test.exp_omitDoneDBRecords, omitDoneDBRecords)
		assert.EqualValues(t, test.exp_res, records.GetDBRecords())
	}

}

func getContent(readRecords DBRecords) string {
	now := time.Now().String()
	var all string
	for database, tablesRecord := range readRecords {
		var tablesRecordStr string
		for table, tableInfo := range tablesRecord.GetTable() {
			tablesRecordStr += table + "," +
				strconv.FormatInt(tableInfo.size, 10) + "," +
				strconv.FormatInt(tableInfo.size, 10) + "," +
				now + "@"
		}
		all += database + sqlOffsetConnector + tablesRecordStr + "\n"
	}

	return all
}

func getMeta(metaDir string) (*reader.Meta, error) {
	logkitConf := conf.MapConf{
		reader.KeyMetaPath: metaDir,
		reader.KeyFileDone: metaDir,
		reader.KeyMode:     reader.ModeMySQL,
	}
	return reader.NewMetaWithConf(logkitConf)
}

type DataTest struct {
	database    string
	createTable []string
	insertData  []string
}

type CronInfo struct {
	cron           bool
	second         string
	notExecOnStart bool
}

var (
	dbSource   = "root:@tcp(127.0.0.1:3306)"
	connectStr = dbSource + "/?charset=gbk"
	now        = time.Now()
	year       = getDateStr(now.Year())
	month      = getDateStr(int(now.Month()))
	day        = getDateStr(now.Day())

	databasesTest = []DataTest{
		{
			database:    "Test_MySql20180510",
			createTable: []string{"CREATE TABLE runoob_tbl20180510est(runoob_id INT NOT NULL AUTO_INCREMENT,runoob_title VARCHAR(100) NOT NULL,runoob_author VARCHAR(40) NOT NULL,submission_date DATE,PRIMARY KEY ( runoob_id ))ENGINE=InnoDB DEFAULT CHARSET=utf8;"},
			insertData:  []string{"INSERT INTO runoob_tbl20180510est (runoob_title, runoob_author, submission_date) VALUES (\"学习 mysql\", \"教程\", NOW());"},
		},
		{
			database:    "Test_MySql20170610",
			createTable: []string{"CREATE TABLE runoob_tbl20170610est(runoob_id INT NOT NULL AUTO_INCREMENT,runoob_title VARCHAR(100) NOT NULL,runoob_author VARCHAR(40) NOT NULL,submission_date DATE,PRIMARY KEY ( runoob_id ))ENGINE=InnoDB DEFAULT CHARSET=utf8;"},
			insertData:  []string{"INSERT INTO runoob_tbl20170610est (runoob_title, runoob_author, submission_date) VALUES (\"学习 mysql\", \"教程\", NOW());"},
		},
		{
			database:    "Test_MySql20171210",
			createTable: []string{"CREATE TABLE runoob_tbl20171210est(runoob_id INT NOT NULL AUTO_INCREMENT,runoob_title VARCHAR(100) NOT NULL,runoob_author VARCHAR(40) NOT NULL,submission_date DATE,PRIMARY KEY ( runoob_id ))ENGINE=InnoDB DEFAULT CHARSET=utf8;"},
			insertData:  []string{"INSERT INTO runoob_tbl20171210est (runoob_title, runoob_author, submission_date) VALUES (\"学习 mysql\", \"教程\", NOW());"},
		},
		{
			database:    "Test_MySql20170910",
			createTable: []string{"CREATE TABLE runoob_tbl20170910est(runoob_id INT NOT NULL AUTO_INCREMENT,runoob_title VARCHAR(100) NOT NULL,runoob_author VARCHAR(40) NOT NULL,submission_date DATE,PRIMARY KEY ( runoob_id ))ENGINE=InnoDB DEFAULT CHARSET=utf8;"},
			insertData:  []string{"INSERT INTO runoob_tbl20170910est (runoob_title, runoob_author, submission_date) VALUES (\"学习 mysql\", \"教程\", NOW());"},
		},
		{
			database:    "Test_MySql20180110",
			createTable: []string{"CREATE TABLE runoob_tbl20180110est(runoob_id INT NOT NULL AUTO_INCREMENT,runoob_title VARCHAR(100) NOT NULL,runoob_author VARCHAR(40) NOT NULL,submission_date DATE,PRIMARY KEY ( runoob_id ))ENGINE=InnoDB DEFAULT CHARSET=utf8;"},
			insertData:  []string{"INSERT INTO runoob_tbl20180110est (runoob_title, runoob_author, submission_date) VALUES (\"学习 mysql\", \"教程\", NOW());"},
		},
	}
	dayDataTestsLine = len(databasesTest)
	todayDataTests   = []DataTest{
		{
			"Test_MySql" + year + month + day,
			[]string{"CREATE TABLE runoob_tbl" + year + month + day + "est(runoob_id INT NOT NULL AUTO_INCREMENT,runoob_title VARCHAR(100) NOT NULL,runoob_author VARCHAR(40) NOT NULL,submission_date DATE,PRIMARY KEY ( runoob_id ))ENGINE=InnoDB DEFAULT CHARSET=utf8;"},
			[]string{"INSERT INTO runoob_tbl" + year + month + day + "est (runoob_title, runoob_author, submission_date) VALUES (\"学习 mysql\", \"教程\", NOW());"},
		},
	}
	todayDataTestsLine = len(todayDataTests)
)

func TestMySql(t *testing.T) {
	databasesTest = append(databasesTest, todayDataTests...)
	if err := prepareMysql(); err != nil {
		t.Errorf("prepare mysql database failed: %v", err)
	}
	defer func() {
		if err := cleanMysql(); err != nil {
			t.Errorf("clean mysql database failed: %v", err)
		}
	}()
	expectData := Data{
		"runoob_id":       int64(1),
		"runoob_title":    "学习 mysql",
		"runoob_author":   "教程",
		"submission_date": year + "-" + month + "-" + day,
	}

	// test exec on start
	runnerName := "mr"
	mr, err := getMySqlReader(false, false, false, runnerName, CronInfo{})
	defer os.RemoveAll(MetaDir)
	assert.NoError(t, err)
	mrData, ok := mr.(reader.DataReader)
	if !ok {
		t.Error("mysql read should have readdata interface")
	}
	dataLine := 0
	before := time.Now()
	for !batchTimeout(before, 2) {
		data, bytes, err := mrData.ReadData()
		if err != nil {
			t.Error(err)
		}
		if len(data) <= 0 {
			continue
		}
		assert.Equal(t, int64(36), bytes)
		assert.Equal(t, expectData, data)
		dataLine++
	}
	assert.Equal(t, todayDataTestsLine, dataLine)
	mr.SyncMeta()

	// test sync records
	dataLine = 0
	before = time.Now()
	for !batchTimeout(before, 2) {
		data, bytes, err := mrData.ReadData()
		if err != nil {
			t.Error(err)
		}
		if len(data) <= 0 {
			continue
		}
		assert.Equal(t, int64(36), bytes)
		assert.Equal(t, expectData, data)
		dataLine++
	}
	assert.Equal(t, 0, dataLine)
	mr.SyncMeta()
	mr.Close()

	// test exec on start, sql not empty
	runnerName = "mrRawSql"
	mrRawSql, err := getMySqlReader(false, true, false, runnerName, CronInfo{})
	assert.NoError(t, err)
	mrRawSqlData, ok := mrRawSql.(reader.DataReader)
	if !ok {
		t.Error("mysql read should have readdata interface")
	}
	dataLine = 0
	before = time.Now()
	for !batchTimeout(before, 2) {
		data, bytes, err := mrRawSqlData.ReadData()
		if err != nil {
			t.Error(err)
		}
		if len(data) <= 0 {
			continue
		}
		assert.Equal(t, int64(36), bytes)
		assert.Equal(t, expectData, data)
		dataLine++
	}
	assert.Equal(t, todayDataTestsLine, dataLine)
	mrRawSql.SyncMeta()

	// no sync records when raw sql is not empty
	dataLine = 0
	before = time.Now()
	for !batchTimeout(before, 2) {
		data, bytes, err := mrRawSqlData.ReadData()
		if err != nil {
			t.Error(err)
		}
		if len(data) <= 0 {
			continue
		}
		assert.Equal(t, int64(36), bytes)
		assert.Equal(t, expectData, data)
		dataLine++
	}
	assert.Equal(t, 0, dataLine)
	mrRawSql.SyncMeta()
	mrRawSql.Close()

	// test history all
	runnerName = "mrHistoryAll"
	mrHistoryAll, err := getMySqlReader(true, false, false, runnerName, CronInfo{})
	assert.NoError(t, err)
	mrHistoryAllData, ok := mrHistoryAll.(reader.DataReader)
	if !ok {
		t.Error("mysql read should have readdata interface")
	}
	dataLine = 0
	before = time.Now()
	for !batchTimeout(before, 2) {
		data, bytes, err := mrHistoryAllData.ReadData()
		if err != nil {
			t.Error(err)
		}
		if len(data) <= 0 {
			continue
		}
		assert.Equal(t, int64(36), bytes)
		assert.Equal(t, expectData, data)
		dataLine++
	}
	assert.Equal(t, dayDataTestsLine+todayDataTestsLine, dataLine)
	mrHistoryAll.SyncMeta()
	mrHistoryAll.Close()

	// test file done in meta dir
	mrHistoryAll2, err := getMySqlReader(true, false, false, runnerName, CronInfo{})
	assert.NoError(t, err)
	mrHistoryAllData2, ok := mrHistoryAll2.(reader.DataReader)
	if !ok {
		t.Error("mysql read should have readdata interface")
	}
	dataLine = 0
	before = time.Now()
	for !batchTimeout(before, 2) {
		data, _, err := mrHistoryAllData2.ReadData()
		if err != nil {
			t.Error(err)
		}
		if len(data) <= 0 {
			continue
		}
		dataLine++
	}
	assert.Equal(t, 0, dataLine)
	mrHistoryAll2.SyncMeta()
	mrHistoryAll2.Close()

	// test cron
	minDataTestsLine, secondAdd3, err := setMinute(time.Now())
	if err != nil {
		t.Errorf("prepare mysql database failed: %v", err)
	}
	// cron task, not exec on start
	runnerName = "mrCron"
	mrCron, err := getMySqlReader(false, false, false, runnerName, CronInfo{true, secondAdd3, true})
	assert.NoError(t, err)
	mrCronData, ok := mrCron.(reader.DataReader)
	if !ok {
		t.Error("mysql read should have readdata interface")
	}
	dataLine = 0
	before = time.Now()
	log.Infof("before: %v", before)
	for !batchTimeout(before, 5) {
		data, bytes, err := mrCronData.ReadData()
		if err != nil {
			t.Error(err)
		}
		if len(data) <= 0 {
			continue
		}
		assert.Equal(t, int64(36), bytes)
		assert.Equal(t, expectData, data)
		dataLine++
	}
	assert.Equal(t, minDataTestsLine, dataLine)
	mrCron.SyncMeta()
	mrCron.Close()

	now := time.Now()
	minDataTestsLine, secondAdd3, err = setMinute(now)
	if err != nil {
		t.Errorf("prepare mysql database failed: %v", err)
	}
	if now.Second() >= 57 {
		minDataTestsLine++
	}
	// cron task, exec on start
	runnerName = "mrCronExecOnStart"
	mrCronExecOnStart, err := getMySqlReader(false, false, false, runnerName, CronInfo{true, secondAdd3, false})
	assert.NoError(t, err)
	mrCronExecOnStartData, ok := mrCronExecOnStart.(reader.DataReader)
	if !ok {
		t.Error("mysql read should have readdata interface")
	}
	dataLine = 0
	before = time.Now()
	log.Infof("before: %v", before)
	for !batchTimeout(before, 5) {
		data, bytes, err := mrCronExecOnStartData.ReadData()
		if err != nil {
			t.Error(err)
		}
		if len(data) <= 0 {
			continue
		}
		assert.Equal(t, int64(36), bytes)
		assert.Equal(t, expectData, data)
		dataLine++
	}
	assert.Equal(t, minDataTestsLine, dataLine)
	mrCronExecOnStart.SyncMeta()
	mrCronExecOnStart.Close()

	mrCronExecOnStart2, err := getMySqlReader(false, false, false, runnerName, CronInfo{true, secondAdd3, false})
	assert.NoError(t, err)
	mrCronExecOnStartData2, ok := mrCronExecOnStart2.(reader.DataReader)
	if !ok {
		t.Error("mysql read should have readdata interface")
	}
	dataLine = 0
	before = time.Now()
	log.Infof("before: %v", before)
	for !batchTimeout(before, 5) {
		data, bytes, err := mrCronExecOnStartData2.ReadData()
		if err != nil {
			t.Error(err)
		}
		if len(data) <= 0 {
			continue
		}
		assert.Equal(t, int64(36), bytes)
		assert.Equal(t, expectData, data)
		dataLine++
	}
	assert.Equal(t, 0, dataLine)
	mrCronExecOnStart2.SyncMeta()
	mrCronExecOnStart2.Close()

	minDataTestsLine, _, err = setSecond()
	if err != nil {
		t.Errorf("prepare mysql database failed: %v", err)
	}
	// cron task, exec on start
	runnerName = "mrLoopcOnStart"
	mrLoopOnStart, err := getMySqlReader(false, false, true, runnerName, CronInfo{false, "", false})
	assert.NoError(t, err)
	mrLoopOnStartData, ok := mrLoopOnStart.(reader.DataReader)
	if !ok {
		t.Error("mysql read should have readdata interface")
	}
	dataLine = 0
	before = time.Now()
	log.Infof("before: %v", before)
	for !batchTimeout(before, 5) {
		data, bytes, err := mrLoopOnStartData.ReadData()
		if err != nil {
			t.Error(err)
		}
		if len(data) <= 0 {
			continue
		}
		assert.Equal(t, int64(36), bytes)
		assert.Equal(t, expectData, data)
		dataLine++
	}
	assert.Equal(t, minDataTestsLine, dataLine)
	mrLoopOnStart.SyncMeta()

	meta, err := getMeta(path.Join(MetaDir, runnerName))
	assert.NoError(t, err)
	var doneRecords = SyncDBRecords{
		mutex: sync.RWMutex{},
	}
	lastDB, lastTable, omitDoneFile := doneRecords.restoreRecordsFile(meta)
	assert.False(t, omitDoneFile)
	assert.Equal(t, 1, len(doneRecords.records))
	expectDB := "Test_MySql" + year + month + day
	tableRecords := doneRecords.records.GetTableRecords(expectDB)
	assert.Equal(t, 2, len(tableRecords.GetTable()))
	assert.Equal(t, expectDB, lastDB)
	assert.NotEmpty(t, lastTable)

	mrLoopOnStart.Close()

}

func getMySqlReader(historyAll, rawsql, loop bool, runnerName string, cronInfo CronInfo) (reader.Reader, error) {
	readerConf := conf.MapConf{
		"mysql_database":     "Test_MySql@(YYYY)@(MM)@(DD)",
		"mysql_table":        "runoob_tbl@(YYYY)@(MM)@(DD)est",
		"mysql_limit_batch":  "100",
		"mysql_history_all":  "false",
		"mode":               "mysql",
		"mysql_exec_onstart": "true",
		"encoding":           "gbk",
		"mysql_datasource":   dbSource,
		"meta_path":          path.Join(MetaDir, runnerName),
		"file_done":          path.Join(MetaDir, runnerName),
		"runner_name":        runnerName,
	}
	if rawsql {
		readerConf["mysql_table"] = ""
		readerConf["mysql_sql"] = "select * from runoob_tbl" + year + month + day + "est"
	}
	if historyAll {
		readerConf["mysql_history_all"] = "true"
	}
	if cronInfo.cron {
		readerConf["mysql_cron"] = cronInfo.second + " * * * * *"
		readerConf["mysql_database"] = readerConf["mysql_database"] + "@(mm)"
		readerConf["mysql_table"] = "runoob_tbl@(YYYY)@(MM)@(DD)" + "@(mm)" + "est"
	}
	if cronInfo.notExecOnStart {
		readerConf["mysql_exec_onstart"] = "false"
	}
	if loop {
		readerConf["mysql_cron"] = "loop 1s"
		readerConf["mysql_table"] = "runoob_tbl@(YYYY)@(MM)@(DD)@(ss)est"
	}
	mr, err := reader.NewReader(readerConf, true)
	if err != nil {
		return nil, err
	}
	return mr, nil
}

func prepareMysql() error {
	db, err := openSql("mysql", connectStr, "")
	if err != nil {
		return err
	}

	defer db.Close()
	if err = db.Ping(); err != nil {
		return err
	}

	for _, dbInfo := range databasesTest {
		_, err = db.Query("DROP DATABASE IF EXISTS " + dbInfo.database)
		if err != nil {
			return err
		}

		_, err = db.Exec("CREATE DATABASE " + dbInfo.database)
		if err != nil {
			return err
		}

		_, err = db.Exec("USE " + dbInfo.database)
		if err != nil {
			return err
		}

		for _, createTable := range dbInfo.createTable {
			_, err = db.Exec(createTable)
			if err != nil {
				return err
			}
		}

		for _, data := range dbInfo.insertData {
			_, err = db.Exec(data)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func cleanMysql() error {
	db, err := openSql("mysql", connectStr, "")
	if err != nil {
		return err
	}
	for _, dbInfo := range databasesTest {
		_, err := db.Query("DROP DATABASE IF EXISTS " + dbInfo.database)
		if err != nil {
			return err
		}
	}
	return nil
}

func getDateStr(date int) string {
	dateStr := strconv.Itoa(date)
	if date < 10 {
		return "0" + dateStr
	}
	return dateStr
}

func batchTimeout(before time.Time, interval float64) bool {
	// 超过最长的发送间隔
	if time.Now().Sub(before).Seconds() >= interval {
		return true
	}

	return false
}

func setMinute(nowCron time.Time) (int, string, error) {
	var (
		secondAdd3 = getDateStr((nowCron.Second() + 3) % 60)
		minute     = getDateStr(nowCron.Minute())
	)
	if nowCron.Second() >= 57 {
		minute = getDateStr(nowCron.Minute() + 1)
	}
	var minDataTests = []DataTest{
		{
			"Test_MySql" + year + month + day + minute,
			[]string{"CREATE TABLE runoob_tbl" + year + month + day + minute + "est(runoob_id INT NOT NULL AUTO_INCREMENT,runoob_title VARCHAR(100) NOT NULL,runoob_author VARCHAR(40) NOT NULL,submission_date DATE,PRIMARY KEY ( runoob_id ))ENGINE=InnoDB DEFAULT CHARSET=utf8;"},
			[]string{"INSERT INTO runoob_tbl" + year + month + day + minute + "est (runoob_title, runoob_author, submission_date) VALUES (\"学习 mysql\", \"教程\", NOW());"},
		},
	}
	minDataTestsLine := len(minDataTests)
	log.Infof("time now cron: %v, minute: %v, secondAdd3: %v", nowCron, minute, secondAdd3)
	databasesTest = append(databasesTest, minDataTests...)
	if err := cleanMysql(); err != nil {
		return minDataTestsLine, secondAdd3, err
	}
	if err := prepareMysql(); err != nil {
		return minDataTestsLine, secondAdd3, err
	}
	return minDataTestsLine, secondAdd3, nil
}

func setSecond() (int, string, error) {
	var (
		nowCron    = time.Now()
		secondAdd2 = getDateStr((nowCron.Second() + 2) % 60)
		secondAdd4 = getDateStr((nowCron.Second() + 4) % 60)
		minute     = getDateStr(nowCron.Minute())
	)
	var minDataTests = []DataTest{
		{
			"Test_MySql" + year + month + day,
			[]string{"CREATE TABLE runoob_tbl" + year + month + day + secondAdd2 + "est(runoob_id INT NOT NULL AUTO_INCREMENT,runoob_title VARCHAR(100) NOT NULL,runoob_author VARCHAR(40) NOT NULL,submission_date DATE,PRIMARY KEY ( runoob_id ))ENGINE=InnoDB DEFAULT CHARSET=utf8;",
				"CREATE TABLE runoob_tbl" + year + month + day + secondAdd4 + "est(runoob_id INT NOT NULL AUTO_INCREMENT,runoob_title VARCHAR(100) NOT NULL,runoob_author VARCHAR(40) NOT NULL,submission_date DATE,PRIMARY KEY ( runoob_id ))ENGINE=InnoDB DEFAULT CHARSET=utf8;"},
			[]string{"INSERT INTO runoob_tbl" + year + month + day + secondAdd2 + "est (runoob_title, runoob_author, submission_date) VALUES (\"学习 mysql\", \"教程\", NOW());",
				"INSERT INTO runoob_tbl" + year + month + day + secondAdd4 + "est (runoob_title, runoob_author, submission_date) VALUES (\"学习 mysql\", \"教程\", NOW());"},
		},
	}
	log.Infof("time now cron: %v, minute: %v, secondAdd2: %v, secondAdd4: %v", nowCron, minute, secondAdd2, secondAdd4)
	databasesTest = append(databasesTest, minDataTests...)
	if err := cleanMysql(); err != nil {
		return 0, "", err
	}
	if err := prepareMysql(); err != nil {
		return 0, "", err
	}
	return 2, "", nil
}
