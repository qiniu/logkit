package postgres

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/reader/config"
	. "github.com/qiniu/logkit/reader/sql"
	"github.com/qiniu/logkit/reader/sql/datagen"
	. "github.com/qiniu/logkit/reader/test"
	"github.com/qiniu/logkit/utils/models"
)

type DataTest struct {
	database    string
	createTable []string
	insertData  []string
}

var (
	pgDbSource     = "host=127.0.0.1 port=5432 connect_timeout=10 user=postgres password=lyt sslmode=disable"
	pgDatabaseTest = DataTest{
		database:    "test_postgresdb",
		createTable: []string{"CREATE TABLE person (id int4,name varchar,age int4,salary float4,delete  bool,create_time timestamp(6))WITH (OIDS=FALSE);"},
		insertData: []string{
			"INSERT INTO person VALUES ('1', '小王', null, '5000.3', 't', '2017-09-04 11:26:17');",  //test integer null
			"INSERT INTO person VALUES ('2', '小明', '28',  null, 'f', '2018-03-20 11:22:17');",     //test float  null
			"INSERT INTO person VALUES ('3', '小张', '28', '5000.5', null, '2018-10-10 11:23:17');", //test bool null
		},
	}
)

//for postgres
func TestLocation(t *testing.T) {
	tm1, _ := time.ParseInLocation(time.RFC3339, "2017-09-04T11:26:17Z", time.FixedZone("+0000", 0))
	tm1 = time.Date(2017, 9, 4, 11, 26, 17, 0, time.FixedZone("+0000", 0))
	fmt.Println(tm1.String())
}

func TestPostgres(t *testing.T) {
	if err := preparePostgres(0); err != nil {
		t.Fatalf("prepare postgres database failed: %v", err)
	}
	tm1 := time.Date(2017, 9, 4, 11, 26, 17, 0, time.FixedZone("+0000", 0))
	tm2 := time.Date(2018, 3, 20, 11, 22, 17, 0, time.FixedZone("+0000", 0))
	tm3 := time.Date(2018, 10, 10, 11, 23, 17, 0, time.FixedZone("+0000", 0))
	expectDatas := []models.Data{
		{
			"id":          int64(1),
			"name":        "小王",
			"age":         int64(0),
			"salary":      5000.2998046875,
			"delete":      true,
			"create_time": tm1,
		},
		{
			"id":          int64(2),
			"name":        "小明",
			"age":         int64(28),
			"salary":      float64(0),
			"delete":      false,
			"create_time": tm2,
		},
		{
			"id":          int64(3),
			"name":        "小张",
			"age":         int64(28),
			"salary":      5000.5,
			"delete":      false,
			"create_time": tm3,
		},
	}

	runnerName := "TestPostgres"
	mr, err := reader.NewReader(conf.MapConf{
		"postgres_database":     pgDatabaseTest.database + "0",
		"postgres_limit_batch":  "100",
		"mode":                  "postgres",
		"postgres_exec_onstart": "true",
		"postgres_datasource":   pgDbSource,
		"postgres_sql":          "select * from person",
		"meta_path":             path.Join(MetaDir, runnerName),
		"file_done":             path.Join(MetaDir, runnerName),
		"runner_name":           runnerName,
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(MetaDir)
	r, ok := mr.(reader.DataReader)
	if !ok {
		t.Error("postgres read should have readdata interface")
	}
	assert.NoError(t, mr.(*PostgresReader).Start())

	dataLine := 0
	before := time.Now()
	var actualData []models.Data
	for !batchTimeout(before, 2) {
		data, _, err := r.ReadData()
		if err != nil {
			t.Error(err)
		}
		if len(data) <= 0 {
			continue
		}
		actualData = append(actualData, data)
		dataLine++

	}
	assert.Equal(t, 3, dataLine)

	for k, v := range actualData {
		delete(expectDatas[k], "create_time")
		delete(v, "create_time")
		assert.Equal(t, expectDatas[k], v)
	}
}

func TestPostgresWithOffset(t *testing.T) {
	if err := preparePostgres(1); err != nil {
		t.Fatalf("prepare postgres database failed: %v", err)
	}
	tm1 := time.Date(2017, 9, 4, 11, 26, 17, 0, time.FixedZone("+0000", 0))
	tm2 := time.Date(2018, 3, 20, 11, 22, 17, 0, time.FixedZone("+0000", 0))
	tm3 := time.Date(2018, 10, 10, 11, 23, 17, 0, time.FixedZone("+0000", 0))
	expectDatas := []models.Data{
		{
			"id":          int64(1),
			"name":        "小王",
			"age":         int64(0),
			"salary":      5000.2998046875,
			"delete":      true,
			"create_time": tm1,
		},
		{
			"id":          int64(2),
			"name":        "小明",
			"age":         int64(28),
			"salary":      float64(0),
			"delete":      false,
			"create_time": tm2,
		},
		{
			"id":          int64(3),
			"name":        "小张",
			"age":         int64(28),
			"salary":      5000.5,
			"delete":      false,
			"create_time": tm3,
		},
	}

	os.MkdirAll(MetaDir+"/TestPostgres/", 0777)
	err := ioutil.WriteFile("meta/TestPostgres/file.meta", []byte("select@*@from@person##2\t1\n"), 0777)
	assert.NoError(t, err)

	runnerName := "TestPostgres"
	mr, err := reader.NewReader(conf.MapConf{
		"postgres_database":     pgDatabaseTest.database + "1",
		"postgres_limit_batch":  "100",
		"mode":                  "postgres",
		"postgres_offset_key":   "id",
		"postgres_exec_onstart": "true",
		"postgres_datasource":   pgDbSource,
		"postgres_sql":          "select * from person",
		"meta_path":             path.Join(MetaDir, runnerName),
		"file_done":             path.Join(MetaDir, runnerName),
		"runner_name":           runnerName,
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(MetaDir)
	r, ok := mr.(reader.DataReader)
	if !ok {
		t.Error("postgres read should have readdata interface")
	}
	assert.NoError(t, mr.(*PostgresReader).Start())

	dataLine := 0
	before := time.Now()
	var actualData []models.Data
	for !batchTimeout(before, 2) {
		data, _, err := r.ReadData()
		if err != nil {
			t.Error(err)
		}
		if len(data) <= 0 {
			continue
		}
		actualData = append(actualData, data)
		dataLine++

	}
	assert.Equal(t, 2, dataLine)

	for i := 0; i < len(actualData); i++ {
		delete(expectDatas[i+1], "create_time")
		delete(actualData[i], "create_time")
		assert.Equal(t, expectDatas[i+1], actualData[i])
	}
}

func dropAndCreateDB(id int) error {
	db, err := OpenSql(ModePostgreSQL, pgDbSource)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.Query("DROP DATABASE IF EXISTS " + pgDatabaseTest.database + strconv.Itoa(id))
	if err != nil {
		return err
	}
	_, err = db.Exec("CREATE DATABASE " + pgDatabaseTest.database + strconv.Itoa(id))
	if err != nil {
		return err
	}
	return nil
}

//使用相同数据库会由于排他锁而无法访问 故加一个随机数以区分
func preparePostgres(random int) error {
	if err := dropAndCreateDB(random); err != nil {
		return err
	}

	db, err := OpenSql(ModePostgreSQL, pgDbSource+" dbname="+pgDatabaseTest.database+strconv.Itoa(random))
	if err != nil {
		return err
	}
	for _, createTable := range pgDatabaseTest.createTable {
		_, err = db.Exec(createTable)
		if err != nil {
			return err
		}
	}
	for _, data := range pgDatabaseTest.insertData {
		_, err = db.Exec(data)
		if err != nil {
			return err
		}
	}
	return nil
}

func TestPostgresWithTimestampInt(t *testing.T) {
	t.Parallel()
	if err := dropAndCreateDB(2); err != nil {
		t.Fatalf("prepare postgres database failed: %v", err)
	}

	os.MkdirAll(MetaDir+"/TestPostgresWithTimestamp/", 0777)
	defer os.RemoveAll(MetaDir)
	tablename := "testtime1"
	runnerName := "TestPostgresWithTimestamp"
	mr, err := reader.NewReader(conf.MapConf{
		"postgres_database": pgDatabaseTest.database + "2",
		"mode":              "postgres",
		"postgres_timestamp_key":  "timestamp",
		"postgres_exec_onstart":   "true",
		"postgres_start_time":     "0",
		"postgres_batch_intervel": "1000",
		"postgres_timestamp_int":  "true",
		"postgres_cron":           "loop 1s",
		"postgres_datasource":     pgDbSource,
		"postgres_sql":            "select * from " + tablename,
		"meta_path":               path.Join(MetaDir, runnerName),
		"file_done":               path.Join(MetaDir, runnerName),
		"runner_name":             runnerName,
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	r, ok := mr.(reader.DataReader)
	if !ok {
		t.Error("postgres read should have readdata interface")
	}
	assert.NoError(t, mr.(*PostgresReader).Start())
	totalnum := 10000
	go datagen.GeneratePostgresData(pgDbSource+" dbname="+pgDatabaseTest.database+"2", tablename, int64(totalnum), 100*time.Millisecond, time.Hour, time.Now().Add(-100*time.Hour))
	dataLine := 0
	before := time.Now()
	var actualData []models.Data
	for !batchTimeout(before, 120) {
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

func TestPostgresWithTimestampS(t *testing.T) {
	t.Parallel()
	if err := dropAndCreateDB(3); err != nil {
		t.Fatalf("prepare postgres database failed: %v", err)
	}

	os.MkdirAll(MetaDir+"/TestPostgresWithTimestampS/", 0777)
	defer os.RemoveAll(MetaDir)
	tablename := "testtime2"
	runnerName := "TestPostgresWithTimestampS"
	mr, err := reader.NewReader(conf.MapConf{
		"postgres_database": pgDatabaseTest.database + "3",
		"mode":              "postgres",
		"postgres_timestamp_key":  "create_time",
		"postgres_exec_onstart":   "true",
		"postgres_start_time":     "2018-10-01 15:04:05",
		"postgres_batch_intervel": "30m",
		"postgres_cron":           "loop 1s",
		"postgres_datasource":     pgDbSource,
		"postgres_sql":            "select * from " + tablename,
		"meta_path":               path.Join(MetaDir, runnerName),
		"file_done":               path.Join(MetaDir, runnerName),
		"runner_name":             runnerName,
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	r, ok := mr.(reader.DataReader)
	if !ok {
		t.Error("postgres read should have readdata interface")
	}
	assert.NoError(t, mr.(*PostgresReader).Start())
	totalnum := 10000
	go datagen.GeneratePostgresData(pgDbSource+" dbname="+pgDatabaseTest.database+"3", tablename, int64(totalnum), 100*time.Millisecond, time.Hour, time.Now().Add(-100*time.Hour))
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

func batchTimeout(before time.Time, interval float64) bool {
	// 超过最长的发送间隔
	if time.Now().Sub(before).Seconds() >= interval {
		return true
	}

	return false
}

func Test_getConnectStr(t *testing.T) {
	mrPostgressql := &PostgresReader{
		database:    "Test_getCheckConnectStr",
		datasource:  "host=localhost port=5432 connect_timeout=10 user=pqgotest password=123456 sslmode=disable",
		rawDatabase: "Test_getCheckConnectStr",
	}
	connectStr := mrPostgressql.getConnectStr()
	expectPostgresSql := "host=localhost port=5432 connect_timeout=10 user=pqgotest password=123456 sslmode=disable dbname=Test_getCheckConnectStr"
	assert.Equal(t, expectPostgresSql, connectStr)
}
