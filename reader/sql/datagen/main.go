package main

import (
	"database/sql"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb" //mssql 驱动
	_ "github.com/go-sql-driver/mysql"   //mysql 驱动
	"github.com/lib/pq"                  //postgres 驱动
	"github.com/qiniu/log"
	utilsos "github.com/qiniu/logkit/utils/os"

	"sync"

	"github.com/Pallinder/go-randomdata"
)

const (
	NextVersion = "v1.0.0"
)

const usage = `datagen generate sql data for test

Usage:

  datagen [commands|flags]

The commands & flags are:

  -v                   print the version to stdout.
  -h <host>            specify database host
  -p <port>			   specify database port
  -db <database>	   specify database name
  -t <type>  		   specify database type: mysql or postgres
  -u <username>		   database username
  -password <password> database password
  -table <tablename>   database tablename
  -num <total data>    total data number, 0 for forever

Examples:

  # start to gen data to database
  datagen -h 127.0.0.1 -p 3306 -t postgres

`

var (
	fversion     = flag.Bool("v", false, "print the version to stdout")
	host         = flag.String("h", "127.0.0.1", "specify database host")
	port         = flag.String("p", "5432", "specify database port")
	databaseType = flag.String("t", "postgres", "specify database type mysql or postgres")
	database     = flag.String("db", "testdb", "specify database name")
	username     = flag.String("u", "test", "database username")
	password     = flag.String("password", "abc123", "database password")
	table        = flag.String("table", "test13", "database table name")
	totalnumber  = flag.Int64("num", 0, "total data number, 0 for forever")
)

func usageExit(rc int) {
	fmt.Println(usage)
	os.Exit(rc)
}

func main() {
	flag.Usage = func() { usageExit(0) }
	flag.Parse()
	switch {
	case *fversion:
		fmt.Println("datagen version: ", NextVersion)
		osInfo := utilsos.GetOSInfo()
		fmt.Println("Hostname: ", osInfo.Hostname)
		fmt.Println("Core: ", osInfo.Core)
		fmt.Println("OS: ", osInfo.OS)
		fmt.Println("Platform: ", osInfo.Platform)
		return
	}
	switch *databaseType {
	case "mysql":
		fmt.Println("mysql is not supported now")
	case "postgres":
		fmt.Println("Start to generate data to ", *host, *port, *username, *database, *table)
		generatePostgresData(*host, *port, *username, *password, *database, *table)
	default:
		fmt.Println("no db type choosed, exit...")
	}
}

func openSql(dbtype, connectStr string) (db *sql.DB, err error) {
	db, err = sql.Open(dbtype, connectStr)
	if err != nil {
		return nil, fmt.Errorf("open %v failed: %v", dbtype, err)
	}
	return db, nil
}

func getPostgresDb(dbsource string) (db *sql.DB, err error) {
	db, err = openSql("postgres", dbsource)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(); err != nil {
		return nil, err
	}
	return db, nil
}

func preparePostgres(pgDbSource, databasename string) (db *sql.DB, err error) {
	db, err = openSql("postgres", pgDbSource)
	if err != nil {
		log.Error(err)
		return nil, err
	}
	defer db.Close()

	return getPostgresDb(pgDbSource + " dbname=" + databasename)
}

func generatePostgresData(host, port, username, password, database, table string) {
	datasource := "sslmode=disable host=" + host + " port=" + port + " dbname=" + database + " user=" + username + " password=" + password
	db, err := preparePostgres(datasource, database)
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
	tm := time.Now().Add(-100 * time.Hour)
	var xx int
	for {
		var wg = new(sync.WaitGroup)
		for i := 0; i < 4; i++ {
			wg.Add(1)
			go insert(db, table, datanum, tm, wg)
			datanum += 500
		}
		wg.Wait()
		fmt.Println(datanum, " of data inserted ", time.Now().String())
		if *totalnumber > 0 && datanum >= *totalnumber {
			break
		}
		time.Sleep(100 * time.Millisecond)
		xx++
		if xx%3 == 0 {
			tm = tm.Add(time.Hour)
		}
	}
	fmt.Println(datanum, " finish inserted")
}

func insert(db *sql.DB, table string, datanum int64, tm time.Time, wg *sync.WaitGroup) {
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
