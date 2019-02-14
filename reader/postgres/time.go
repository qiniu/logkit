package postgres

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/qiniu/log"

	. "github.com/qiniu/logkit/reader/sql"
)

var PostgresTimeFormat = []string{"2006-01-02 15:04:05.000000", "2006-01-02 15:04:05", "2006-1-2 15:4:5.000000", "2006-1-2 15:4:5"}

const PgtimeFormat = "2006-01-02 15:04:05.000000"

func ParsePostgresDatetime(str string) (time.Time, error) {
	for _, format := range PostgresTimeFormat {
		startTime, err := time.ParseInLocation(format, str, time.Local)
		if err == nil {
			return startTime, nil
		}
	}
	return time.Time{}, fmt.Errorf("can not parse %s as such format %s", str, strings.Join(PostgresTimeFormat, ","))
}

//这个query只有一行
func queryNumber(tsql string, db *sql.DB) (int64, error) {
	rows, err := db.Query(tsql)
	if err != nil {
		log.Error(err)
		return 0, err
	}
	defer rows.Close()
	var scanArgs = []interface{}{new(interface{})}
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			log.Error(err)
			return 0, err
		}
		if len(scanArgs) < 1 {
			return 0, errors.New("no data found")
		}

		number, err := ConvertLong(scanArgs[0])
		if err != nil {
			log.Error(err)
		}
		return number, nil
	}
	return 0, errors.New("no data found")
}
