package system

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_formatUptime(t *testing.T) {
	t.Parallel()
	uptime := uint64(time.Now().Unix()) - uint64(time.Now().Add(-2*24*time.Hour-10*time.Hour-2*time.Minute).Unix())
	assert.EqualValues(t, "2 days, 10:02", formatUptime(uptime))
}

func Test_getNumNetCard(t *testing.T) {
	t.Parallel()
	number := getNumNetCard()
	if number <= 0 {
		t.Fatalf("net card number should greater than 0")
	}
	t.Log("get net card number: ", number)
}
