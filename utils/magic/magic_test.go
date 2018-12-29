package magic

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGoMagic(t *testing.T) {
	now, _ := time.Parse(time.RFC3339, "2017-02-01T16:06:19+08:00")
	tests := []struct {
		data string
		exp  string
	}{
		{
			data: "select x@(MM)@(DD) from dbtable@(hh)-@(mm)",
			exp:  "select x0201 from dbtable16-06",
		},
		{
			data: "select x@(M)@(D) from dbtable@(h)-@(m)",
			exp:  "select x21 from dbtable16-6",
		},
		{
			data: "@(YY)",
			exp:  "17",
		},
		{
			data: "@(YYYY)@(MM)",
			exp:  "201702",
		},
		{
			data: "hhhhh",
			exp:  "hhhhh",
		},
	}
	for _, ti := range tests {
		got := GoMagic(ti.data, now)
		assert.EqualValues(t, ti.exp, got)
	}
}

func TestConvertMagic(t *testing.T) {
	now1, _ := time.Parse(time.RFC3339, "2017-04-01T06:06:09+08:00")
	now2, _ := time.Parse(time.RFC3339, "2017-11-11T16:16:29+08:00")
	tests := []struct {
		data string
		exp1 string
		exp2 string
	}{
		{
			data: "YYYY",
			exp1: "2017",
			exp2: "2017",
		},
		{
			data: "YY",
			exp1: "17",
			exp2: "17",
		},
		{
			data: "MM",
			exp1: "04",
			exp2: "11",
		},
		{
			data: "M",
			exp1: "4",
			exp2: "11",
		},
		{
			data: "D",
			exp1: "1",
			exp2: "11",
		},
		{
			data: "DD",
			exp1: "01",
			exp2: "11",
		},
		{
			data: "hh",
			exp1: "06",
			exp2: "16",
		},
		{
			data: "h",
			exp1: "6",
			exp2: "16",
		},
		{
			data: "mm",
			exp1: "06",
			exp2: "16",
		},
		{
			data: "m",
			exp1: "6",
			exp2: "16",
		},
		{
			data: "ss",
			exp1: "09",
			exp2: "29",
		},
		{
			data: "s",
			exp1: "9",
			exp2: "29",
		},
	}
	for _, ti := range tests {
		got := convertMagic(ti.data, now1)
		assert.EqualValues(t, ti.exp1, got)
		got = convertMagic(ti.data, now2)
		assert.EqualValues(t, ti.exp2, got)
	}
}
