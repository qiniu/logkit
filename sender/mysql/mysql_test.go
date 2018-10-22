package mysql

import (
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/utils/models"
)

func TestMySQLSender(t *testing.T) {
	dbname := "db1"
	tabname := "table1"
	connstr := "root:@tcp(127.0.0.1:3306)/"

	db, err := sql.Open("mysql", connstr+"?multiStatements=true")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	statments := fmt.Sprintf("DROP DATABASE IF EXISTS %s; CREATE DATABASE %s; USE %s; CREATE TABLE %s(name VARCHAR(100) NOT NULL, uid VARCHAR(40) NOT NULL, age INT)", dbname, dbname, dbname, tabname)
	if _, err = db.Exec(statments); err != nil {
		t.Fatal(err)
	}
	defer func() {
		statments = fmt.Sprintf("DROP TABLE %s; DROP DATABASE %s;", tabname, dbname)
		if _, err = db.Exec(statments); err != nil {
			t.Error(err)
		}
	}()

	err = os.Setenv("MySQL_DATASOURCE", connstr+dbname)
	assert.NoError(t, err)
	defer os.Unsetenv("MySQL_DATASOURCE")

	conf := conf.MapConf{
		sender.KeyMySQLDataSource: "${MySQL_DATASOURCE}",
		sender.KeyMySQLTable:      tabname,
		sender.KeyMaxSendRate:     "10000",
	}
	sender, err := NewSender(conf)
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()

	nr := 100
	data := make([]models.Data, 0, nr)
	for i := 0; i < nr; i++ {
		data = append(data, models.Data{
			"name": fmt.Sprintf("annonym %d", i),
			"uid":  strconv.FormatInt(rand.Int63(), 10),
			"age":  rand.Int31n(int32(nr)),
		})
	}
	if err := sender.Send(data); err != nil {
		t.Error(err)
	}

	rows, err := db.Query("SELECT COUNT(*) FROM " + tabname)
	if err != nil {
		t.Error(err)
	}
	defer rows.Close()

	if rows.Next() {
		count := 0
		if err = rows.Scan(&count); err != nil {
			log.Error(err)
		}
		if count != nr {
			log.Errorf("unexpect sended results, got %d, want %d", count, nr)
		}
	}
}
