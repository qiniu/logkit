package mysql

import (
	"fmt"
	"os"
	"path"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/reader/config"
	. "github.com/qiniu/logkit/reader/sql"
	"github.com/qiniu/logkit/reader/sql/datagen"
	. "github.com/qiniu/logkit/reader/test"
	"github.com/qiniu/logkit/utils/models"
)

func getMeta(metaDir string) (*reader.Meta, error) {
	logkitConf := conf.MapConf{
		KeyMetaPath: metaDir,
		KeyFileDone: metaDir,
		KeyMode:     ModeMySQL,
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
	submissionDate, _ := time.ParseInLocation("2006-01-02", year+"-"+month+"-"+day, time.Local)
	expectData := models.Data{
		"runoob_id":       int64(1),
		"runoob_title":    "学习 mysql",
		"runoob_author":   "教程",
		"submission_date": submissionDate,
	}

	runnerName := "mrOnce"
	err := os.Setenv("TestMySql_dbSource", dbSource)
	assert.NoError(t, err)
	defer os.Unsetenv("TestMySql_dbSource")
	mr, err := reader.NewReader(conf.MapConf{
		"mysql_database":     "Test_MySql20180510",
		"mysql_table":        "runoob_tbl20180510est",
		"mysql_limit_batch":  "100",
		"mysql_history_all":  "true",
		"mode":               "mysql",
		"mysql_exec_onstart": "true",
		"encoding":           "gbk",
		"mysql_datasource":   "${TestMySql_dbSource}",
		"meta_path":          path.Join(MetaDir, runnerName),
		"file_done":          path.Join(MetaDir, runnerName),
		"runner_name":        runnerName,
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(MetaDir)
	r, ok := mr.(reader.DataReader)
	if !ok {
		t.Error("mysql read should have readdata interface")
	}
	assert.NoError(t, mr.(*MysqlReader).Start())

	dataLine := 0
	before := time.Now()
	for !batchTimeout(before, 2) {
		data, bytes, err := r.ReadData()
		if err != nil {
			t.Error(err)
		}
		if len(data) <= 0 {
			continue
		}
		assert.Equal(t, int64(46), bytes)
		assert.Equal(t, expectData, data)
		dataLine++
	}
	assert.Equal(t, 1, dataLine)

	// test exec on start
	runnerName = "mr"
	mr, err = getMySqlReader(false, false, false, runnerName, CronInfo{})
	assert.NoError(t, err)
	r, ok = mr.(reader.DataReader)
	if !ok {
		t.Error("mysql read should have readdata interface")
	}
	assert.NoError(t, mr.(*MysqlReader).Start())

	dataLine = 0
	before = time.Now()
	for !batchTimeout(before, 2) {
		data, bytes, err := r.ReadData()
		if err != nil {
			t.Error(err)
		}
		if len(data) <= 0 {
			continue
		}
		assert.Equal(t, int64(46), bytes)
		assert.Equal(t, expectData, data)
		dataLine++
	}
	assert.Equal(t, todayDataTestsLine, dataLine)
	mr.SyncMeta()

	// test sync Records
	dataLine = 0
	before = time.Now()
	for !batchTimeout(before, 2) {
		data, bytes, err := r.ReadData()
		if err != nil {
			t.Error(err)
		}
		if len(data) <= 0 {
			continue
		}
		assert.Equal(t, int64(46), bytes)
		assert.Equal(t, expectData, data)
		dataLine++
	}
	assert.Equal(t, 0, dataLine)
	mr.Close()

	// test exec on start, sql not empty
	runnerName = "mrRawSql"
	mrRawSql, err := getMySqlReader(false, true, false, runnerName, CronInfo{})
	assert.NoError(t, err)
	mrRawSqlData, ok := mrRawSql.(reader.DataReader)
	if !ok {
		t.Error("mysql read should have readdata interface")
	}
	assert.NoError(t, mrRawSql.(*MysqlReader).Start())

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
		assert.Equal(t, int64(46), bytes)
		assert.Equal(t, expectData, data)
		dataLine++
	}
	assert.Equal(t, todayDataTestsLine, dataLine)
	mrRawSql.SyncMeta()

	// no sync Records when raw sql is not empty
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
		assert.Equal(t, int64(46), bytes)
		assert.Equal(t, expectData, data)
		dataLine++
	}
	assert.Equal(t, 0, dataLine)
	mrRawSql.Close()

	// test history all
	runnerName = "mrHistoryAll"
	mrHistoryAll, err := getMySqlReader(true, false, false, runnerName, CronInfo{})
	assert.NoError(t, err)
	mrHistoryAllData, ok := mrHistoryAll.(reader.DataReader)
	if !ok {
		t.Error("mysql read should have readdata interface")
	}
	assert.NoError(t, mrHistoryAll.(*MysqlReader).Start())

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
		assert.Equal(t, int64(46), bytes)
		assert.Equal(t, expectData, data)
		dataLine++
	}
	assert.Equal(t, dayDataTestsLine+todayDataTestsLine, dataLine)
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
	assert.NoError(t, mrCron.(*MysqlReader).Start())

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
		assert.Equal(t, int64(46), bytes)
		assert.Equal(t, expectData, data)
		dataLine++
	}
	assert.Equal(t, minDataTestsLine, dataLine)
	mrCron.Close()

	// cron task, exec on start
	now := time.Now()
	minDataTestsLine, secondAdd3, err = setMinute(now)
	if err != nil {
		t.Errorf("prepare mysql database failed: %v", err)
	}
	if now.Second() >= 57 {
		minDataTestsLine++
	}
	runnerName = "mrCronExecOnStart"
	mrCronExecOnStart, err := getMySqlReader(false, false, false, runnerName, CronInfo{true, secondAdd3, false})
	assert.NoError(t, err)
	mrCronExecOnStartData, ok := mrCronExecOnStart.(reader.DataReader)
	if !ok {
		t.Error("mysql read should have readdata interface")
	}
	assert.NoError(t, mrCronExecOnStart.(*MysqlReader).Start())

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
		assert.Equal(t, int64(46), bytes)
		assert.Equal(t, expectData, data)
		dataLine++
	}
	assert.Equal(t, minDataTestsLine, dataLine)
	mrCronExecOnStart.Close()

	mrCronExecOnStart2, err := getMySqlReader(false, false, false, runnerName, CronInfo{true, secondAdd3, false})
	assert.NoError(t, err)
	mrCronExecOnStartData2, ok := mrCronExecOnStart2.(reader.DataReader)
	if !ok {
		t.Error("mysql read should have readdata interface")
	}
	assert.NoError(t, mrCronExecOnStart2.(*MysqlReader).Start())

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
		assert.Equal(t, int64(46), bytes)
		assert.Equal(t, expectData, data)
		dataLine++
	}
	assert.Equal(t, 0, dataLine)
	mrCronExecOnStart2.Close()

	// cron task, exec on start
	minDataTestsLine, _, err = setSecond()
	if err != nil {
		t.Errorf("prepare mysql database failed: %v", err)
	}
	runnerName = "mrLoopcOnStart"
	mrLoopOnStart, err := getMySqlReader(false, false, true, runnerName, CronInfo{false, "", false})
	assert.NoError(t, err)
	mrLoopOnStartData, ok := mrLoopOnStart.(reader.DataReader)
	if !ok {
		t.Error("mysql read should have readdata interface")
	}
	assert.NoError(t, mrLoopOnStart.(*MysqlReader).Start())

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
		assert.Equal(t, int64(46), bytes)
		assert.Equal(t, expectData, data)
		dataLine++
	}
	assert.Equal(t, minDataTestsLine, dataLine)
	mrLoopOnStart.SyncMeta()

	meta, err := getMeta(path.Join(MetaDir, runnerName))
	assert.NoError(t, err)
	var doneRecords = SyncDBRecords{
		Mutex: sync.RWMutex{},
	}
	lastDB, lastTable, omitDoneFile := doneRecords.RestoreRecordsFile(meta)
	assert.False(t, omitDoneFile)
	assert.Equal(t, 1, len(doneRecords.Records))
	expectDB := "Test_MySql" + year + month + day
	tableRecords := doneRecords.Records.GetTableRecords(expectDB)
	assert.Equal(t, 2, len(tableRecords.GetTable()))
	assert.Equal(t, expectDB, lastDB)
	assert.NotEmpty(t, lastTable)
	mrLoopOnStart.Close()
}

func TestMysqlWithTimestampInt(t *testing.T) {
	database := "TestMysqlWithTimestampInt"
	databasesTest = append(databasesTest, todayDataTests...)
	if err := dropAndCreateDB(1, database); err != nil {
		t.Errorf("prepare mysql database failed: %v", err)
	}
	defer func() {
		if err := dropDB(1, database); err != nil {
			t.Errorf("clean mysql database failed: %v", err)
		}
	}()

	os.MkdirAll(MetaDir+"/TestMysqlWithTimestampInt/", 0777)
	defer os.RemoveAll(MetaDir)
	tablename := "testTime"
	runnerName := "mrTime"
	mr, err := reader.NewReader(conf.MapConf{
		"mysql_database":       database + "1",
		"mode":                 "mysql",
		"mysql_exec_onstart":   "true",
		"encoding":             "gbk",
		"mysql_datasource":     dbSource,
		"mysql_timestamp_key":  "timestamp",
		"mysql_start_time":     "0",
		"mysql_batch_intervel": "1",
		"mysql_timestamp_int":  "true",
		"mysql_cron":           "loop 1s",
		"mysql_sql":            "select * from " + tablename,
		"meta_path":            path.Join(MetaDir, runnerName),
		"file_done":            path.Join(MetaDir, runnerName),
		"runner_name":          runnerName,
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	r, ok := mr.(reader.DataReader)
	if !ok {
		t.Error("mysql read should have readdata interface")
	}
	assert.NoError(t, mr.(*MysqlReader).Start())
	totalnum := 10000
	go datagen.GenerateMysqlData(dbSource+"/"+database+"1"+"?charset=gbk", tablename, int64(totalnum), 100*time.Millisecond, time.Hour, time.Now().Add(-100*time.Hour), false)
	dataLine := 0
	before := time.Now()
	var actualData []models.Data
	for !batchTimeout(before, 60) {
		data, _, err := r.ReadData()
		if err != nil {
			continue
		}
		if len(data) <= 0 {
			continue
		}
		actualData = append(actualData, data)
		dataLine++
		if dataLine >= totalnum {
			break
		}
	}
	assert.Equal(t, totalnum, dataLine)
}

func TestMysqlWithTimestamp(t *testing.T) {
	database := "TestMysqlWithTimestamp"
	databasesTest = append(databasesTest, todayDataTests...)
	if err := dropAndCreateDB(1, database); err != nil {
		t.Errorf("prepare mysql database failed: %v", err)
	}
	defer func() {
		if err := dropDB(1, database); err != nil {
			t.Errorf("clean mysql database failed: %v", err)
		}
	}()

	os.MkdirAll(MetaDir+"/TestMysqlWithTimestamp/", 0777)
	defer os.RemoveAll(MetaDir)
	tablename := "testTime"
	runnerName := "mrTime"
	mr, err := reader.NewReader(conf.MapConf{
		"mysql_database":       database + "1",
		"mode":                 "mysql",
		"mysql_exec_onstart":   "true",
		"encoding":             "gbk",
		"mysql_datasource":     dbSource,
		"mysql_timestamp_key":  "submission_date",
		"mysql_start_time":     "2018-10-01 15:04:05",
		"mysql_batch_intervel": "30m",
		"mysql_cron":           "loop 1s",
		"mysql_sql":            "select * from " + tablename,
		"meta_path":            path.Join(MetaDir, runnerName),
		"file_done":            path.Join(MetaDir, runnerName),
		"runner_name":          runnerName,
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	r, ok := mr.(reader.DataReader)
	if !ok {
		t.Error("mysql read should have readdata interface")
	}
	assert.NoError(t, mr.(*MysqlReader).Start())
	totalNum := 10000
	go datagen.GenerateMysqlData(dbSource+"/"+database+"1"+"?charset=gbk", tablename, int64(totalNum), 100*time.Millisecond, time.Hour, time.Now().Add(-100*time.Hour), false)
	dataLine := 0
	before := time.Now()
	var actualData []models.Data
	for !batchTimeout(before, 60) {
		data, _, err := r.ReadData()
		if err != nil {
			continue
		}
		if len(data) <= 0 {
			continue
		}
		actualData = append(actualData, data)
		dataLine++
		if dataLine >= totalNum {
			break
		}
	}
	assert.Equal(t, totalNum, dataLine)
}

func TestMysqlWithTimestampStr(t *testing.T) {
	database := "TestMysqlWithTimestampStr"
	databasesTest = append(databasesTest, todayDataTests...)
	if err := dropAndCreateDB(1, database); err != nil {
		t.Errorf("prepare mysql database failed: %v", err)
	}
	defer func() {
		if err := dropDB(1, database); err != nil {
			t.Errorf("clean mysql database failed: %v", err)
		}
	}()

	os.MkdirAll(MetaDir+"/TestMysqlWithTimestampStr/", 0777)
	defer os.RemoveAll(MetaDir)
	tablename := "testTime"
	runnerName := "mrTime"
	mr, err := reader.NewReader(conf.MapConf{
		"mysql_database":      database + "1",
		"mode":                "mysql",
		"mysql_exec_onstart":  "true",
		"encoding":            "gbk",
		"mysql_datasource":    dbSource,
		"mysql_timestamp_key": "submission_date",
		"mysql_start_time":    "mytest20181001150405",
		"mysql_cron":          "loop 1s",
		"mysql_sql":           "select * from " + tablename,
		"meta_path":           path.Join(MetaDir, runnerName),
		"file_done":           path.Join(MetaDir, runnerName),
		"runner_name":         runnerName,
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	r, ok := mr.(reader.DataReader)
	if !ok {
		t.Error("mysql read should have readdata interface")
	}
	assert.NoError(t, mr.(*MysqlReader).Start())
	totalNum := 10000
	go datagen.GenerateMysqlData(dbSource+"/"+database+"1"+"?charset=gbk", tablename, int64(totalNum), 100*time.Millisecond, time.Hour, time.Now().Add(-100*time.Hour), true)
	dataLine := 0
	before := time.Now()
	var actualData []models.Data
	for !batchTimeout(before, 60) {
		data, _, err := r.ReadData()
		if err != nil {
			continue
		}
		if len(data) <= 0 {
			continue
		}
		actualData = append(actualData, data)
		dataLine++
		if dataLine >= totalNum {
			break
		}
	}
	assert.Equal(t, totalNum, dataLine)
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
	db, err := OpenSql("mysql", connectStr)
	if err != nil {
		return err
	}
	defer db.Close()

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

func dropAndCreateDB(id int, database string) error {
	db, err := OpenSql("mysql", connectStr)
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Query("DROP DATABASE IF EXISTS " + database + strconv.Itoa(id))
	if err != nil {
		return err
	}
	_, err = db.Exec("CREATE DATABASE " + database + strconv.Itoa(id))
	if err != nil {
		return err
	}
	return nil
}

func dropDB(id int, database string) error {
	db, err := OpenSql("mysql", connectStr)
	if err != nil {
		return err
	}
	defer db.Close()

	_, err = db.Query("DROP DATABASE IF EXISTS " + database + strconv.Itoa(id))
	if err != nil {
		return err
	}
	return nil
}

func cleanMysql() error {
	db, err := OpenSql("mysql", connectStr)
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

func Test_getConnectStr(t *testing.T) {
	now := time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC)

	mr := &MysqlReader{
		database:    "Test_getCheckConnectStr",
		historyAll:  true,
		rawTable:    "*",
		rawDatabase: "*",
		datasource:  "root:@tcp(127.0.0.1:3306)",
		encoder:     "utf8",
	}
	connectStr := mr.getConnectStr("", now)
	expectMySql := "root:@tcp(127.0.0.1:3306)/?charset=utf8"
	assert.Equal(t, expectMySql, connectStr)
}

func Test_getCheckAll(t *testing.T) {
	mr := &MysqlReader{
		database:    "Test_getCheckHistory",
		historyAll:  true,
		rawTable:    "*",
		rawDatabase: "*",
	}

	tests := []struct {
		queryType int
		expRes    bool
	}{
		{
			queryType: TABLE,
			expRes:    true,
		},
		{
			queryType: COUNT,
			expRes:    true,
		},
		{
			queryType: DATABASE,
			expRes:    true,
		},
	}

	for _, test := range tests {
		checkHistory, err := mr.getAll(test.queryType)
		assert.NoError(t, err)
		assert.EqualValues(t, test.expRes, checkHistory)
	}
}

func Test_isMatchData(t *testing.T) {
	meta, err := getMeta(MetaDir)
	assert.NoError(t, err)
	defer os.RemoveAll(MetaDir)
	mr := &MysqlReader{
		readBatch:         100,
		meta:              meta,
		historyAll:        false,
		isLoop:            true,
		cronSchedule:      false,
		omitDoneDBRecords: true,
	}

	tests := []struct {
		data           string
		magicRemainStr string
		magicRet       string
		expRet         bool
		expTimeStart   []int
		exptimeEnd     []int
		expRemainIndex []int
	}{
		{
			data:           "x02abc01def",
			magicRemainStr: "xabcdef",
			magicRet:       "x02abc01def",
			expRet:         true,
			expTimeStart:   []int{-1, 1, 6, -1, -1, -1},
			exptimeEnd:     []int{0, 3, 8, 0, 0, 0},
			expRemainIndex: []int{0, 1, 3, 6, 8, 11},
		},
		{
			data:           "x01abc01def",
			magicRemainStr: "xabcdef",
			magicRet:       "x02abc01def",
			expRet:         true,
			expTimeStart:   []int{-1, 1, 6, -1, -1, -1},
			exptimeEnd:     []int{0, 3, 8, 0, 0, 0},
			expRemainIndex: []int{0, 1, 3, 6, 8, 11},
		},
		{
			data:           "x02abc01defdef",
			magicRemainStr: "xabcdef",
			magicRet:       "x02abc01def*",
			expRet:         true,
			expTimeStart:   []int{-1, 1, 6, -1, -1, -1},
			exptimeEnd:     []int{0, 3, 8, 0, 0, 0},
			expRemainIndex: []int{0, 1, 3, 6, 8, 11},
		},
		{
			data:           "abc",
			magicRemainStr: "xabc",
			magicRet:       "x02abc01",
			expRet:         false,
			expTimeStart:   []int{-1, 1, 6, -1, -1, -1},
			exptimeEnd:     []int{0, 3, 8, 0, 0, 0},
			expRemainIndex: []int{0, 1, 3, 6},
		},
		{
			data:           "x0201",
			magicRemainStr: "x",
			magicRet:       "x0201*",
			expRet:         true,
			expTimeStart:   []int{-1, 1, 3, -1, -1, -1},
			exptimeEnd:     []int{0, 3, 5, 0, 0, 0},
			expRemainIndex: []int{0, 1},
		},
		{
			data:           "x0102xxxxx",
			magicRemainStr: "x",
			magicRet:       "x0102*",
			expRet:         true,
			expTimeStart:   []int{-1, 3, 1, -1, -1, -1},
			exptimeEnd:     []int{0, 5, 3, 0, 0, 0},
			expRemainIndex: []int{0, 1},
		},
		{
			data:           "17",
			magicRemainStr: "",
			magicRet:       "17",
			expRet:         true,
			expTimeStart:   []int{0, -1, -1, -1, -1, -1},
			exptimeEnd:     []int{2, 0, 0, 0, 0, 0},
			expRemainIndex: []int{0, 0},
		},
		{
			data:           "abcd201702efg",
			magicRemainStr: "abcdefg",
			magicRet:       "abcd201702efg*",
			expRet:         true,
			expTimeStart:   []int{4, 8, -1, -1, -1, -1},
			exptimeEnd:     []int{8, 10, 0, 0, 0, 0},
			expRemainIndex: []int{0, 4, 10, 13},
		},
		{
			data:           "abcd20170201160619abc",
			magicRemainStr: "abcd",
			magicRet:       "abcd20170201160619",
			expRet:         false,
			expTimeStart:   []int{4, 8, 10, 12, 14, 16},
			exptimeEnd:     []int{8, 10, 12, 14, 16, 18},
			expRemainIndex: []int{0, 4},
		},
		{
			data:           "hhhhh",
			magicRemainStr: "hhhhh",
			magicRet:       "hhhhh",
			expRet:         true,
			expTimeStart:   []int{-1, -1, -1, -1, -1, -1},
			exptimeEnd:     []int{0, 0, 0, 0, 0, 0},
			expRemainIndex: []int{0, 5},
		},
	}
	for _, ti := range tests {
		Ret := mr.compareData(DATABASE, "", ti.data, ti.magicRemainStr, &MagicRes{
			Ret:         ti.magicRet,
			RemainIndex: ti.expRemainIndex,
			TimeStart:   ti.expTimeStart,
			TimeEnd:     ti.exptimeEnd,
		})
		assert.EqualValues(t, ti.expRet, Ret)
	}
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

func TestSQLReader(t *testing.T) {
	meta, err := getMeta(MetaDir)
	assert.NoError(t, err)
	defer os.RemoveAll(MetaDir)
	database := "TestSQLReaderdatabase"
	mr := &MysqlReader{
		rawDatabase: database,
		database:    database,
		rawSQLs:     "select * from mysql123  ;select * from mysql345;",
		syncSQLs:    []string{"select * from mysql123", "select * from mysql345"},
		readBatch:   100,
		meta:        meta,
		offsetKey:   "id",
		offsets:     []int64{123, 456},
	}
	assert.Equal(t, "MYSQL_"+database, mr.Source())

	// 测试meta备份和恢复
	mr.SyncMeta()
	gotoffsets, gotsqls, omit := RestoreMeta(meta, mr.rawSQLs, 0)
	assert.EqualValues(t, mr.offsets, gotoffsets, "got offsets error")
	assert.EqualValues(t, mr.syncSQLs, gotsqls, "got sqls error")
	assert.EqualValues(t, false, omit)
	assert.EqualValues(t, "MYSQL_Reader:"+mr.database+"_"+models.Hash(mr.rawSQLs), mr.Name())

	// 测试更新Offset
	expoffsets := []int64{123, 0, 0}
	testsqls := []string{"select * from mysql123", "select * from mysql789", "select x from xx"}
	mr.updateOffsets(testsqls)
	assert.EqualValues(t, expoffsets, mr.offsets)
	mr.syncSQLs = testsqls

	//测试getSQL
	gotSQL := mr.getSQL(2, mr.syncSQLs[2])
	assert.EqualValues(t, testsqls[2]+" WHERE id >= 0 AND id < 100;", gotSQL)
	mr.offsetKey = ""
	gotSQL = mr.getSQL(0, mr.syncSQLs[0])
	assert.EqualValues(t, testsqls[0], gotSQL)

	assert.Equal(t, models.StatsInfo{}, mr.Status())
}

func Test_getRawSQLs(t *testing.T) {
	r1 := &MysqlReader{}
	mysqltests := []struct {
		queryType int
		expSQLs   string
	}{
		{
			queryType: TABLE,
			expSQLs:   "Select * From `my_table`;",
		},
		{
			queryType: COUNT,
			expSQLs:   "Select Count(*) From `my_table`;",
		},
		{
			queryType: DATABASE,
			expSQLs:   "",
		},
	}

	for _, test := range mysqltests {
		sqls, err := r1.getRawSqls(test.queryType, "my_table")
		assert.NoError(t, err)
		assert.EqualValues(t, test.expSQLs, sqls)
	}
}
