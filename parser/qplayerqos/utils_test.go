package qplayerqos

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBuildTime(t *testing.T) {
	testCases := []struct {
		str  string
		time time.Time
	}{
		{
			str:  "1605605666",
			time: time.Date(2020, 11, 17, 9, 34, 26, 0, time.UTC),
		},
		{
			str:  "1605605666233",
			time: time.Date(2020, 11, 17, 9, 34, 26, 233*1000*1000, time.UTC),
		},
		{
			str:  "1605605666233874",
			time: time.Date(2020, 11, 17, 9, 34, 26, 233874*1000, time.UTC),
		},
		{
			str:  "1605605666233874999",
			time: time.Date(2020, 11, 17, 9, 34, 26, 233874999, time.UTC),
		},
		{
			// zero time
			str: "abcdefg",
		},
	}
	for i, testCase := range testCases {
		actualTime := buildTime(testCase.str)
		if testCase.time.IsZero() {
			assert.True(t, actualTime.IsZero(), fmt.Sprintf("test case %d should not parse into time", i))
		} else {
			assert.Equal(t, testCase.time.UnixNano(), actualTime.UnixNano(), fmt.Sprintf("test case %d: time should be equal", i))
		}
	}

}
