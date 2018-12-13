package sql

import (
	"fmt"
	"strings"
	"time"
)

var PostgresTimeFormat = []string{"2006-01-02 15:04:05.000000", "2006-01-02 15:04:05", "2006-1-2 15:4:5.000000", "2006-1-2 15:4:5"}

const pgtimeFormat = "2006-01-02 15:04:05.000000"

func parsePostgresDatetime(str string) (time.Time, error) {
	for _, format := range PostgresTimeFormat {
		startTime, err := time.ParseInLocation(format, str, time.Local)
		if err == nil {
			return startTime, nil
		}
	}
	return time.Time{}, fmt.Errorf("can not parse %s as such format %s", str, strings.Join(PostgresTimeFormat, ","))
}
