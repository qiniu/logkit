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
	_ "github.com/lib/pq"                //postgres 驱动
	"github.com/qiniu/log"
	utilsos "github.com/qiniu/logkit/utils/os"

	"github.com/Pallinder/go-randomdata"
)

const (
	NextVersion = "v1.0.0"
)

const usage = `datagen generate sql data for test

Usage:

  datagen [commands|flags]

The commands & flags are:

  -v                 print the version to stdout.
  -h <host>          specify database host
  -p <port>			 specify database port
  -db <database>	 specify database name
  -t <type>  		 specify database type: mysql or postgres
  -u <username>		 database username
  -p <password>		 database password
  -table <tablename> database tablename

Examples:

  # start to gen data to database
  datagen -h 127.0.0.1 -p 3306 -t postgres

`

var (
	fversion     = flag.Bool("v", false, "print the version to stdout")
	host         = flag.String("h", "127.0.0.1", "specify database host")
	port         = flag.String("p", "5432", "specify database port")
	databaseType = flag.String("t", "postgres", "specify database type mysql or postgres")
	database     = flag.String("db", "testdb2", "specify database name")
	username     = flag.String("u", "test", "database username")
	password     = flag.String("password", "", "database password")
	table        = flag.String("table", "test5", "database table name")
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

	_, err = db.Exec(`CREATE TABLE ` + table + ` (id int4,realtm varchar,email varchar, city varchar, useragent varchar,age int4,salary float4,delete  bool,create_time timestamp(6))WITH (OIDS=FALSE);`)
	if err != nil && !strings.Contains(err.Error(), "already exists") {
		log.Error(err)
		return
	}
	datanum := 1
	ii := 0
	tm := time.Date(2018, 11, 1, 0, 0, 0, 0, time.Local)
	for {
		dt := `INSERT INTO ` + table + ` VALUES ('` + strconv.Itoa(datanum) + `', '` + time.Now().Format(time.RFC3339Nano) + `', '` + randomdata.Email() + `', '` + randomdata.City() + `','` + randomdata.UserAgentString() + `', ` + strconv.Itoa(rand.Intn(100)) + `, '` + strconv.FormatFloat(rand.Float64(), 'f', -1, 64) + `', 't', '` + tm.Format("2006-01-02 15:04:05") + `');`
		_, err = db.Exec(dt)
		if err != nil {
			log.Error(err)
			return
		}
		ii++
		if datanum%10000 == 0 {
			fmt.Println(datanum, " of data inserted")
			if ii%2 == 0 {
				tm = tm.Add(30 * time.Minute)
			}
		}
		if datanum == 1000000 {
			break
		}
		datanum++
	}
	fmt.Println(datanum, " finish inserted")
}
