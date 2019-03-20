package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	_ "github.com/denisenkom/go-mssqldb" //mssql 驱动
	_ "github.com/go-sql-driver/mysql"   //mysql 驱动

	//postgres 驱动
	"github.com/qiniu/logkit/reader/sql/datagen"
	utilsos "github.com/qiniu/logkit/utils/os"
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
		fmt.Printf("Start to generate data to %s %s %s %s %s ", *host, *port, *username, *database, *table)
		if *totalnumber <= 0 {
			fmt.Printf("will generate data forever\n")
		} else {
			fmt.Printf("will generate total %v data\n", *totalnumber)
		}
		datasource := *username + ":" + *password + "@tcp(" + *host + ":" + *port + ")" + "/" + *database + "?charset="
		datagen.GenerateMysqlData(datasource, *table, *totalnumber, 100*time.Millisecond, time.Hour, time.Now().Add(-100*time.Hour))
	case "postgres":
		fmt.Printf("Start to generate data to %s %s %s %s %s ", *host, *port, *username, *database, *table)
		if *totalnumber <= 0 {
			fmt.Printf("will generate data forever\n")
		} else {
			fmt.Printf("will generate total %v data\n", *totalnumber)
		}
		datasource := "sslmode=disable host=" + *host + " port=" + *port + " dbname=" + *database + " user=" + *username + " password=" + *password
		datagen.GeneratePostgresData(datasource, *table, *totalnumber, 100*time.Millisecond, time.Hour, time.Now().Add(-100*time.Hour))
	default:
		fmt.Println("no db type choosed, exit...")
	}
}
