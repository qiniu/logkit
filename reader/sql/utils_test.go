package sql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestUpdateSql(t *testing.T) {
	rawsqls := "select * from mysql123  ;select * from mysql345;"
	syncSQLs := []string{"select * from mysql123", "select * from mysql345"}
	got := UpdateSqls(rawsqls, time.Now())
	assert.EqualValues(t, syncSQLs, got)
}
