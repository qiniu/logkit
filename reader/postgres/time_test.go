package postgres

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParsePostgresDatetime(t *testing.T) {
	tests := []string{"2018-10-1 12:2:3", "2018-10-01 12:02:03", "2018-10-01 12:02:03.000000"}
	var t0 time.Time
	for _, v := range tests {
		tm, err := ParsePostgresDatetime(v)
		if err != nil {
			t.Error(err)
		}
		if t0.Unix() <= 0 {
			t0 = tm
		}
		assert.Equal(t, t0, tm)
	}
}
