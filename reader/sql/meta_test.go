package sql

import (
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/reader/test"
)

var ReadRecords = DBRecords{
	"db1": TableRecords{
		Table: map[string]TableInfo{
			"db1_tb1": {Size: -1, Offset: -1},
			"db1_tb2": {Size: -1, Offset: -1},
			"db1_tb3": {Size: -1, Offset: -1},
		},
	},
	"db2": TableRecords{
		Table: map[string]TableInfo{
			"db2_tb1": {Size: -1, Offset: -1},
			"db2_tb2": {Size: -1, Offset: -1},
			"db2_tb3": {Size: -1, Offset: -1},
			"db2_tb4": {Size: -1, Offset: -1},
			"db2_tb5": {Size: -1, Offset: -1},
		},
	},
	"db3": TableRecords{
		Table: map[string]TableInfo{
			"db3_tb1": {Size: -1, Offset: -1},
			"db3_tb2": {Size: -1, Offset: -1},
		},
	},
}

func Test_WriteRecordsFile(t *testing.T) {
	meta, err := reader.NewMeta(MetaDir, MetaDir, "mysql", "logpath", "", 7)
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(MetaDir)

	err = WriteRecordsFile(meta.DoneFilePath, GetContent(ReadRecords))
	assert.NoError(t, err)
}

func TestSyncDBRecords_RestoreRecordsFileestoreRecordsFile(t *testing.T) {
	meta, err := reader.NewMeta(MetaDir, MetaDir, "mysql", "logpath", "", 7)
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(MetaDir)

	tests := []struct {
		set                  DBRecords
		expRes               DBRecords
		expOmitDoneDBRecords bool
	}{
		{
			set:                  DBRecords{},
			expRes:               nil,
			expOmitDoneDBRecords: true,
		},
		{
			set:                  ReadRecords,
			expRes:               ReadRecords,
			expOmitDoneDBRecords: false,
		},
		{
			set: DBRecords{
				"db1": TableRecords{
					Table: map[string]TableInfo{
						"db1_tb1":  {Size: -1, Offset: -1},
						"db1_tb10": {Size: -1, Offset: -1},
					},
					Mutex: sync.RWMutex{},
				},
				"db4": TableRecords{
					Table: map[string]TableInfo{
						"db4_tb10": {Size: -1, Offset: -1},
					},
					Mutex: sync.RWMutex{},
				},
			},
			expRes: DBRecords{
				"db1": TableRecords{
					Table: map[string]TableInfo{
						"db1_tb1":  {Size: -1, Offset: -1},
						"db1_tb2":  {Size: -1, Offset: -1},
						"db1_tb3":  {Size: -1, Offset: -1},
						"db1_tb10": {Size: -1, Offset: -1},
					},
					Mutex: sync.RWMutex{},
				},
				"db2": TableRecords{
					Table: map[string]TableInfo{
						"db2_tb1": {Size: -1, Offset: -1},
						"db2_tb2": {Size: -1, Offset: -1},
						"db2_tb3": {Size: -1, Offset: -1},
						"db2_tb4": {Size: -1, Offset: -1},
						"db2_tb5": {Size: -1, Offset: -1},
					},
					Mutex: sync.RWMutex{},
				},
				"db3": TableRecords{
					Table: map[string]TableInfo{
						"db3_tb1": {Size: -1, Offset: -1},
						"db3_tb2": {Size: -1, Offset: -1},
					},
					Mutex: sync.RWMutex{},
				},
				"db4": TableRecords{
					Table: map[string]TableInfo{
						"db4_tb10": {Size: -1, Offset: -1},
					},
					Mutex: sync.RWMutex{},
				},
			},
			expOmitDoneDBRecords: false,
		},
		{
			set: DBRecords{
				"db1": TableRecords{
					Table: map[string]TableInfo{
						"db1_tb1":  {Size: -1, Offset: -1},
						"db1_tb10": {Size: -1, Offset: -1},
					},
					Mutex: sync.RWMutex{},
				},
				"db4": TableRecords{
					Table: map[string]TableInfo{
						"db4_tb10": {Size: -1, Offset: -1},
					},
					Mutex: sync.RWMutex{},
				},
			},
			expRes: DBRecords{
				"db1": TableRecords{
					Table: map[string]TableInfo{
						"db1_tb1":  {Size: -1, Offset: -1},
						"db1_tb2":  {Size: -1, Offset: -1},
						"db1_tb3":  {Size: -1, Offset: -1},
						"db1_tb10": {Size: -1, Offset: -1},
					},
					Mutex: sync.RWMutex{},
				},
				"db2": TableRecords{
					Table: map[string]TableInfo{
						"db2_tb1": {Size: -1, Offset: -1},
						"db2_tb2": {Size: -1, Offset: -1},
						"db2_tb3": {Size: -1, Offset: -1},
						"db2_tb4": {Size: -1, Offset: -1},
						"db2_tb5": {Size: -1, Offset: -1},
					},
					Mutex: sync.RWMutex{},
				},
				"db3": TableRecords{
					Table: map[string]TableInfo{
						"db3_tb1": {Size: -1, Offset: -1},
						"db3_tb2": {Size: -1, Offset: -1},
					},
					Mutex: sync.RWMutex{},
				},
				"db4": TableRecords{
					Table: map[string]TableInfo{
						"db4_tb10": {Size: -1, Offset: -1},
					},
					Mutex: sync.RWMutex{},
				},
			},
			expOmitDoneDBRecords: false,
		},
		{
			set: DBRecords{
				"db1": TableRecords{
					Table: map[string]TableInfo{
						"db1_tb10": {Size: -1, Offset: -1},
					},
					Mutex: sync.RWMutex{},
				},
				"db3": TableRecords{
					Table: map[string]TableInfo{
						"db3_tb10": {Size: -1, Offset: -1},
					},
					Mutex: sync.RWMutex{},
				},
			},
			expRes: DBRecords{
				"db1": TableRecords{
					Table: map[string]TableInfo{
						"db1_tb1":  {Size: -1, Offset: -1},
						"db1_tb2":  {Size: -1, Offset: -1},
						"db1_tb3":  {Size: -1, Offset: -1},
						"db1_tb10": {Size: -1, Offset: -1},
					},
					Mutex: sync.RWMutex{},
				},
				"db2": TableRecords{
					Table: map[string]TableInfo{
						"db2_tb1": {Size: -1, Offset: -1},
						"db2_tb2": {Size: -1, Offset: -1},
						"db2_tb3": {Size: -1, Offset: -1},
						"db2_tb4": {Size: -1, Offset: -1},
						"db2_tb5": {Size: -1, Offset: -1},
					},
					Mutex: sync.RWMutex{},
				},
				"db3": TableRecords{
					Table: map[string]TableInfo{
						"db3_tb1":  {Size: -1, Offset: -1},
						"db3_tb2":  {Size: -1, Offset: -1},
						"db3_tb10": {Size: -1, Offset: -1},
					},
					Mutex: sync.RWMutex{},
				},
				"db4": TableRecords{
					Table: map[string]TableInfo{
						"db4_tb10": {Size: -1, Offset: -1},
					},
					Mutex: sync.RWMutex{},
				},
			},
			expOmitDoneDBRecords: false,
		},
		{
			set: ReadRecords,
			expRes: DBRecords{
				"db1": TableRecords{
					Table: map[string]TableInfo{
						"db1_tb1":  {Size: -1, Offset: -1},
						"db1_tb2":  {Size: -1, Offset: -1},
						"db1_tb3":  {Size: -1, Offset: -1},
						"db1_tb10": {Size: -1, Offset: -1},
					},
					Mutex: sync.RWMutex{},
				},
				"db2": TableRecords{
					Table: map[string]TableInfo{
						"db2_tb1": {Size: -1, Offset: -1},
						"db2_tb2": {Size: -1, Offset: -1},
						"db2_tb3": {Size: -1, Offset: -1},
						"db2_tb4": {Size: -1, Offset: -1},
						"db2_tb5": {Size: -1, Offset: -1},
					},
					Mutex: sync.RWMutex{},
				},
				"db3": TableRecords{
					Table: map[string]TableInfo{
						"db3_tb1":  {Size: -1, Offset: -1},
						"db3_tb2":  {Size: -1, Offset: -1},
						"db3_tb10": {Size: -1, Offset: -1},
					},
					Mutex: sync.RWMutex{},
				},
				"db4": TableRecords{
					Table: map[string]TableInfo{
						"db4_tb10": {Size: -1, Offset: -1},
					},
					Mutex: sync.RWMutex{},
				},
			},
			expOmitDoneDBRecords: false,
		},
	}

	for _, test := range tests {
		err = WriteRecordsFile(meta.DoneFilePath, GetContent(test.set))
		assert.NoError(t, err)

		var Records SyncDBRecords
		_, _, omitDoneDBRecords := Records.RestoreRecordsFile(meta)
		assert.EqualValues(t, test.expOmitDoneDBRecords, omitDoneDBRecords)
		assert.EqualValues(t, test.expRes, Records.GetDBRecords())
	}
}

func Test_RestoreMeta(t *testing.T) {
	meta, err := reader.NewMeta(MetaDir, MetaDir, "mysql", "logpath", "", 7)
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(MetaDir)

	_, _, err = meta.ReadOffset()
	if err == nil {
		t.Error("Offset must be nil")
	}
	encodeSQLs := make([]string, 0)
	syncSQLs := []string{"SELECT * FROM A", "SELECT * FROM B", "SELECT * FROM C"}
	offsets := []int64{1, 2, 3}
	for _, sql := range syncSQLs {
		encodeSQLs = append(encodeSQLs, strings.Replace(sql, " ", "@", -1))
	}
	for _, Offset := range offsets {
		encodeSQLs = append(encodeSQLs, strconv.FormatInt(Offset, 10))
	}
	all := strings.Join(encodeSQLs, SqlOffsetConnector)
	err = meta.WriteOffset(all, int64(len(syncSQLs)))
	if err != nil {
		t.Error(err)
	}
	file, Offset, err := meta.ReadOffset()
	assert.Nil(t, err)
	if file != all {
		t.Error("sql meta Offset should be " + all)
	}
	if Offset != 3 {
		t.Error("file Offset should be 3")
	}

	var mgld time.Duration
	rawSqls := "SELECT * FROM A; SELECT * FROM B; SELECT * FROM C;"
	actualOffsets, actualSqls, omitMeta := RestoreMeta(meta, rawSqls, mgld)
	assert.EqualValues(t, false, omitMeta)
	assert.EqualValues(t, offsets, actualOffsets)
	assert.EqualValues(t, syncSQLs, actualSqls)

	rawSqls = "SELECT * FROM A; SELECT * FROM B; SELECT * FROM D;"
	actualOffsets, actualSqls, omitMeta = RestoreMeta(meta, rawSqls, mgld)
	assert.EqualValues(t, false, omitMeta)
	assert.EqualValues(t, offsets, actualOffsets)
	assert.EqualValues(t, actualSqls, actualSqls)

	rawSqls = "SELECT * FROM A; SELECT * FROM B;"
	actualOffsets, actualSqls, omitMeta = RestoreMeta(meta, rawSqls, mgld)
	assert.EqualValues(t, true, omitMeta)
	assert.EqualValues(t, 0, len(actualOffsets))
	assert.EqualValues(t, []string{"SELECT * FROM A", "SELECT * FROM B"}, actualSqls)
}

func GetContent(ReadRecords DBRecords) string {
	now := time.Now().String()
	var all string
	for database, tablesRecord := range ReadRecords {
		var tablesRecordStr string
		for table, tableInfo := range tablesRecord.GetTable() {
			tablesRecordStr += table + "," +
				strconv.FormatInt(tableInfo.Size, 10) + "," +
				strconv.FormatInt(tableInfo.Size, 10) + "," +
				now + "@"
		}
		all += database + SqlOffsetConnector + tablesRecordStr + "\n"
	}

	return all
}
