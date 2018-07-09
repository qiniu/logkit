package sql

import (
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/reader/test"
	. "github.com/qiniu/logkit/utils/models"
)

var readRecords = DBRecords{
	"db1": {
		"db1_tb1": TableInfo{size: -1, offset: -1},
		"db1_tb2": TableInfo{size: -1, offset: -1},
		"db1_tb3": TableInfo{size: -1, offset: -1},
	},
	"db2": {
		"db2_tb1": TableInfo{size: -1, offset: -1},
		"db2_tb2": TableInfo{size: -1, offset: -1},
		"db2_tb3": TableInfo{size: -1, offset: -1},
		"db2_tb4": TableInfo{size: -1, offset: -1},
		"db2_tb5": TableInfo{size: -1, offset: -1},
	},
	"db3": {
		"db3_tb1": TableInfo{size: -1, offset: -1},
		"db3_tb2": TableInfo{size: -1, offset: -1},
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
	meta, err := getMeta()
	assert.NoError(t, err)
	defer os.RemoveAll(MetaDir)
	database := "TestSQLReaderdatabase"
	mr := &Reader{
		database:  database,
		rawsqls:   "select * from mysql123  ;select * from mysql345;",
		syncSQLs:  []string{"select * from mysql123", "select * from mysql345"},
		readBatch: 100,
		meta:      meta,
		status:    reader.StatusInit,
		offsetKey: "id",
		offsets:   []int64{123, 456},
		dbtype:    "mysql",
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
	meta, err := getMeta()
	assert.NoError(t, err)
	defer os.RemoveAll(MetaDir)
	mr := &Reader{
		readBatch:         100,
		meta:              meta,
		status:            reader.StatusInit,
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
		ret := mr.isMatchData(DATABASE, ti.data, ti.matchData, ti.matchStr, ti.exp_timeIndex, ti.exp_startIndex, ti.exp_endIndex)
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
		set     DBRecords
		exp_res DBRecords
	}{
		{
			set:     readRecords,
			exp_res: readRecords,
		},
		{
			set: DBRecords{
				"db1": {
					"db1_tb1":  TableInfo{size: -1, offset: -1},
					"db1_tb10": TableInfo{size: -1, offset: -1},
				},
				"db4": {
					"db4_tb10": TableInfo{size: -1, offset: -1},
				},
			},
			exp_res: DBRecords{
				"db1": {
					"db1_tb1":  TableInfo{size: -1, offset: -1},
					"db1_tb2":  TableInfo{size: -1, offset: -1},
					"db1_tb3":  TableInfo{size: -1, offset: -1},
					"db1_tb10": TableInfo{size: -1, offset: -1},
				},
				"db2": {
					"db2_tb1": TableInfo{size: -1, offset: -1},
					"db2_tb2": TableInfo{size: -1, offset: -1},
					"db2_tb3": TableInfo{size: -1, offset: -1},
					"db2_tb4": TableInfo{size: -1, offset: -1},
					"db2_tb5": TableInfo{size: -1, offset: -1},
				},
				"db3": {
					"db3_tb1": TableInfo{size: -1, offset: -1},
					"db3_tb2": TableInfo{size: -1, offset: -1},
				},
				"db4": {
					"db4_tb10": TableInfo{size: -1, offset: -1},
				},
			},
		},
		{
			set: DBRecords{
				"db1": {
					"db1_tb1":  TableInfo{size: -1, offset: -1},
					"db1_tb10": TableInfo{size: -1, offset: -1},
				},
				"db4": {
					"db4_tb10": TableInfo{size: -1, offset: -1},
				},
			},
			exp_res: DBRecords{
				"db1": {
					"db1_tb1":  TableInfo{size: -1, offset: -1},
					"db1_tb2":  TableInfo{size: -1, offset: -1},
					"db1_tb3":  TableInfo{size: -1, offset: -1},
					"db1_tb10": TableInfo{size: -1, offset: -1},
				},
				"db2": {
					"db2_tb1": TableInfo{size: -1, offset: -1},
					"db2_tb2": TableInfo{size: -1, offset: -1},
					"db2_tb3": TableInfo{size: -1, offset: -1},
					"db2_tb4": TableInfo{size: -1, offset: -1},
					"db2_tb5": TableInfo{size: -1, offset: -1},
				},
				"db3": {
					"db3_tb1": TableInfo{size: -1, offset: -1},
					"db3_tb2": TableInfo{size: -1, offset: -1},
				},
				"db4": {
					"db4_tb10": TableInfo{size: -1, offset: -1},
				},
			},
		},
		{
			set: DBRecords{
				"db1": {
					"db1_tb10": TableInfo{size: -1, offset: -1},
				},
				"db3": {
					"db3_tb10": TableInfo{size: -1, offset: -1},
				},
			},
			exp_res: DBRecords{
				"db1": {
					"db1_tb1":  TableInfo{size: -1, offset: -1},
					"db1_tb2":  TableInfo{size: -1, offset: -1},
					"db1_tb3":  TableInfo{size: -1, offset: -1},
					"db1_tb10": TableInfo{size: -1, offset: -1},
				},
				"db2": {
					"db2_tb1": TableInfo{size: -1, offset: -1},
					"db2_tb2": TableInfo{size: -1, offset: -1},
					"db2_tb3": TableInfo{size: -1, offset: -1},
					"db2_tb4": TableInfo{size: -1, offset: -1},
					"db2_tb5": TableInfo{size: -1, offset: -1},
				},
				"db3": {
					"db3_tb1":  TableInfo{size: -1, offset: -1},
					"db3_tb2":  TableInfo{size: -1, offset: -1},
					"db3_tb10": TableInfo{size: -1, offset: -1},
				},
				"db4": {
					"db4_tb10": TableInfo{size: -1, offset: -1},
				},
			},
		},
		{
			set: readRecords,
			exp_res: DBRecords{
				"db1": {
					"db1_tb1":  TableInfo{size: -1, offset: -1},
					"db1_tb2":  TableInfo{size: -1, offset: -1},
					"db1_tb3":  TableInfo{size: -1, offset: -1},
					"db1_tb10": TableInfo{size: -1, offset: -1},
				},
				"db2": {
					"db2_tb1": TableInfo{size: -1, offset: -1},
					"db2_tb2": TableInfo{size: -1, offset: -1},
					"db2_tb3": TableInfo{size: -1, offset: -1},
					"db2_tb4": TableInfo{size: -1, offset: -1},
					"db2_tb5": TableInfo{size: -1, offset: -1},
				},
				"db3": {
					"db3_tb1":  TableInfo{size: -1, offset: -1},
					"db3_tb2":  TableInfo{size: -1, offset: -1},
					"db3_tb10": TableInfo{size: -1, offset: -1},
				},
				"db4": {
					"db4_tb10": TableInfo{size: -1, offset: -1},
				},
			},
		},
	}

	for _, test := range tests {
		err = WriteRecordsFile(meta.DoneFilePath, getContent(test.set))
		assert.NoError(t, err)

		var records DBRecords
		_, _, omitDoneDBRecords := records.restoreRecordsFile(meta)
		assert.EqualValues(t, false, omitDoneDBRecords)
		assert.EqualValues(t, test.exp_res, records)
	}

}

func getContent(readRecords DBRecords) string {
	now := time.Now().String()
	var all string
	for database, tablesRecord := range readRecords {
		var tablesRecordStr string
		for table, tableInfo := range tablesRecord {
			tablesRecordStr += table + "," +
				strconv.FormatInt(tableInfo.size, 10) + "," +
				strconv.FormatInt(tableInfo.size, 10) + "," +
				now + "@"
		}
		all += database + sqlOffsetConnector + tablesRecordStr + "\n"
	}

	return all
}

func getMeta() (*reader.Meta, error) {
	logkitConf := conf.MapConf{
		reader.KeyMetaPath: MetaDir,
		reader.KeyFileDone: MetaDir,
		reader.KeyMode:     reader.ModeMySQL,
	}
	return reader.NewMetaWithConf(logkitConf)
}

var testdbs = []string{"20180101", "20180102", "20180103"}
var tabse = []string{"0601", "0602", "0603"}

var connectStr = "root:123456@tcp(127.0.0.1:3306)/"

func prepareDBS(t *testing.T) {
	db, err := openSql("mysql", connectStr, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, v := range testdbs {
		_, err := db.Exec("create database IF NOT EXISTS `" + v + "`")
		if err != nil {
			t.Fatal(err)
		}
	}
}

//create tables and insert data
/*
func prepareDatas(t *testing.T) {
	for _, v := range testdbs {
		newc := connectStr + v

	}

}
*/

func cleanData(t *testing.T) {
	db, err := openSql("mysql", connectStr, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	for _, v := range testdbs {
		_, err := db.Exec("drop database `" + v + "`")
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestMysqlRead(t *testing.T) {
	prepareDBS(t)
	defer cleanData(t)

}
