package datagen

import (
	"database/sql"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Pallinder/go-randomdata"
	"github.com/lib/pq"
	"github.com/qiniu/log"
)

func openSql(dbtype, connectStr string) (db *sql.DB, err error) {
	db, err = sql.Open(dbtype, connectStr)
	if err != nil {
		return nil, fmt.Errorf("open %v failed: %v", dbtype, err)
	}
	return db, nil
}

func getSqlDb(dbType, dbSource string) (db *sql.DB, err error) {
	db, err = openSql(dbType, dbSource)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(); err != nil {
		return nil, err
	}
	return db, nil
}

func prepareSql(dbType, dbSource string) (db *sql.DB, err error) {
	db, err = openSql(dbType, dbSource)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	defer db.Close()

	return getSqlDb(dbType, dbSource)
}

func GeneratePostgresData(dataSource, table string, totalnumber int64, sleepDuration, timeaddDuration time.Duration, startTime time.Time) {
	db, err := prepareSql("postgres", dataSource)
	if err != nil {
		log.Error(err)
		return
	}
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE ` + table + ` (id int4,timestamp bigint,realtm varchar,email varchar, city varchar, useragent varchar,age int4,salary float4,delete  bool,create_time timestamp(6))WITH (OIDS=FALSE);`)
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		log.Error(err)
		return
	}
	var datanum int64
	tm := startTime
	var xx int
	for {
		var wg = new(sync.WaitGroup)
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go insertPG(db, table, datanum, tm, wg)
			datanum += 500
		}
		wg.Wait()
		fmt.Println(datanum, " of data inserted ", time.Now().String())
		if totalnumber > 0 && datanum >= totalnumber {
			break
		}
		if sleepDuration > 0 {
			time.Sleep(sleepDuration)
		}
		xx++
		if xx%10 == 0 {
			tm = tm.Add(timeaddDuration)
		}
	}
	fmt.Println(datanum, " finish inserted")
}

func insertPG(db *sql.DB, table string, datanum int64, tm time.Time, wg *sync.WaitGroup) {
	defer wg.Done()
	txn, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := txn.Prepare(pq.CopyIn(table, "id", "timestamp", "realtm", "email", "city", "useragent", "age", "salary", "delete", "create_time"))
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 500; i++ {
		_, err = stmt.Exec(strconv.FormatInt(datanum+int64(i)+1, 10), tm.Unix(), time.Now().Format(time.RFC3339Nano), randomdata.Email(), randomdata.City(), randomdata.UserAgentString(), strconv.Itoa(rand.Intn(100)), strconv.FormatFloat(rand.Float64(), 'f', -1, 64), randomdata.Boolean(), tm.Add(-8*time.Hour).Format("2006-01-02 15:04:00"))
		if err != nil {
			log.Fatal(err)
		}
	}

	_, err = stmt.Exec()
	if err != nil {
		log.Fatal(err)
	}

	err = stmt.Close()
	if err != nil {
		log.Fatal(err)
	}

	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}
}

func GenerateMysqlData(dataSource, table string, totalNumber int64, sleepDuration, timeAddDuration time.Duration, startTime time.Time, timeStr bool) {
	db, err := prepareSql("mysql", dataSource)
	if err != nil {
		log.Error(err)
		return
	}
	defer db.Close()

	if !timeStr {
		_, err = db.Exec(`CREATE TABLE ` + table + ` (id INT NOT NULL AUTO_INCREMENT,timestamp INT, title VARCHAR(100) NOT NULL,author VARCHAR(40) NOT NULL,submission_date TIMESTAMP,PRIMARY KEY ( id ))ENGINE=InnoDB DEFAULT CHARSET=utf8;`)
		if err != nil && !strings.Contains(err.Error(), "already exists") {
			log.Error(err)
			return
		}
	} else {
		_, err = db.Exec(`CREATE TABLE ` + table + ` (id INT NOT NULL AUTO_INCREMENT,timestamp INT, title VARCHAR(100) NOT NULL,author VARCHAR(40) NOT NULL,submission_date VARCHAR(100),PRIMARY KEY ( id ))ENGINE=InnoDB DEFAULT CHARSET=utf8;`)
		if err != nil && !strings.Contains(err.Error(), "already exists") {
			log.Error(err)
			return
		}
	}
	var dataNum int64
	tm := startTime
	var xx int
	for {
		var wg = new(sync.WaitGroup)
		for i := 0; i < 4; i++ {
			wg.Add(1)
			if timeStr {
				go insertStrMysql(db, table, dataNum, tm, wg)
			} else {
				go insertMysql(db, table, dataNum, tm, wg)
			}
			dataNum += 500
		}
		wg.Wait()
		fmt.Println(dataNum, " of data inserted ", time.Now().String())
		if totalNumber > 0 && dataNum >= totalNumber {
			break
		}
		if sleepDuration > 0 {
			time.Sleep(sleepDuration)
		}
		xx++
		if xx%2 == 0 {
			tm = tm.Add(timeAddDuration)
		}
	}
	fmt.Println(dataNum, " finish inserted")
}

func insertMysql(db *sql.DB, table string, dataNum int64, tm time.Time, wg *sync.WaitGroup) {
	defer wg.Done()
	txn, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 500; i++ {
		sql := `INSERT INTO ` + table + ` (id, timestamp, title, author, submission_date) VALUES (` + strconv.FormatInt(dataNum+int64(i)+1, 10) + `, ` + strconv.FormatInt(tm.Unix(), 10) + `, "` + randomdata.Address() + `", "` + randomdata.Email() + `", "` + tm.Add(-8*time.Hour).Format("2006-01-02 15:04:00") + `");`
		_, err = txn.Exec(sql)
		if err != nil {
			log.Fatal(err)
		}
	}

	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}
}

func insertStrMysql(db *sql.DB, table string, dataNum int64, tm time.Time, wg *sync.WaitGroup) {
	defer wg.Done()
	txn, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 500; i++ {
		execSql := `INSERT INTO ` + table + ` (id, timestamp, title, author, submission_date) VALUES (` + strconv.FormatInt(dataNum+int64(i)+1, 10) + `, ` + strconv.FormatInt(tm.Unix(), 10) + `, "` + randomdata.Address() + `", "` + randomdata.Email() + `", "` + tm.Add(-8*time.Hour).Format("2006-01-02 15:04:05") + `");`
		_, err = txn.Exec(execSql)
		if err != nil {
			log.Fatal(err)
		}
	}

	err = txn.Commit()
	if err != nil {
		log.Fatal(err)
	}
}
