package sql

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_checkMagic(t *testing.T) {
	tests := []struct {
		data string
		exp  bool
	}{
		{
			data: "x@(DD)@(MM)*",
			exp:  true,
		},
		{
			data: "@(YY)",
			exp:  true,
		},
		{
			data: "abcd@(YYYY)@(M)efg*",
			exp:  false,
		},
		{
			data: "abcd@(YYYY)@(MM)@(DD)@(hh)@(mm)@(ss)efg*",
			exp:  true,
		},
		{
			data: "hhhhh",
			exp:  true,
		},
	}
	for _, ti := range tests {
		got := CheckMagic(ti.data)
		assert.EqualValues(t, ti.exp, got)
	}
}

func TestGoMagicIndex(t *testing.T) {
	now, _ := time.Parse(time.RFC3339, "2017-02-01T16:06:19+08:00")
	tests := []struct {
		data          string
		expRet        string
		expStartIndex []int
		expEndIndex   []int
		expTimeIndex  []int
	}{
		{
			data:          "x@(MM)abc@(DD)def",
			expRet:        "x02abc01def",
			expStartIndex: []int{-1, 1, 6, -1, -1, -1},
			expEndIndex:   []int{0, 3, 8, 0, 0, 0},
			expTimeIndex:  []int{0, 1, 3, 6, 8, 11},
		},
		{
			data:          "x@(MM)abc@(DD)def*",
			expRet:        "x02abc01def*",
			expStartIndex: []int{-1, 1, 6, -1, -1, -1},
			expEndIndex:   []int{0, 3, 8, 0, 0, 0},
			expTimeIndex:  []int{0, 1, 3, 6, 8, 11},
		},
		{
			data:          "x@(MM)abc@(DD)",
			expRet:        "x02abc01",
			expStartIndex: []int{-1, 1, 6, -1, -1, -1},
			expEndIndex:   []int{0, 3, 8, 0, 0, 0},
			expTimeIndex:  []int{0, 1, 3, 6},
		},
		{
			data:          "x@(MM)@(DD)",
			expRet:        "x0201",
			expStartIndex: []int{-1, 1, 3, -1, -1, -1},
			expEndIndex:   []int{0, 3, 5, 0, 0, 0},
			expTimeIndex:  []int{0, 1},
		},
		{
			data:          "x@(DD)@(MM)*",
			expRet:        "x0102*",
			expStartIndex: []int{-1, 3, 1, -1, -1, -1},
			expEndIndex:   []int{0, 5, 3, 0, 0, 0},
			expTimeIndex:  []int{0, 1},
		},
		{
			data:          "@(YY)",
			expRet:        "17",
			expStartIndex: []int{0, -1, -1, -1, -1, -1},
			expEndIndex:   []int{2, 0, 0, 0, 0, 0},
			expTimeIndex:  []int{0, 0},
		},
		{
			data:          "abcd@(YYYY)@(MM)efg*",
			expRet:        "abcd201702efg*",
			expStartIndex: []int{4, 8, -1, -1, -1, -1},
			expEndIndex:   []int{8, 10, 0, 0, 0, 0},
			expTimeIndex:  []int{0, 4, 10, 13},
		},
		{
			data:          "abcd@(YYYY)@(MM)@(DD)@(hh)@(mm)@(ss)*",
			expRet:        "abcd20170201160619*",
			expStartIndex: []int{4, 8, 10, 12, 14, 16},
			expEndIndex:   []int{8, 10, 12, 14, 16, 18},
			expTimeIndex:  []int{0, 4},
		},
		{
			data:          "hhhhh",
			expRet:        "hhhhh",
			expStartIndex: []int{-1, -1, -1, -1, -1, -1},
			expEndIndex:   []int{0, 0, 0, 0, 0, 0},
			expTimeIndex:  []int{0, 5},
		},
	}
	for _, ti := range tests {
		magicRes, err := GoMagicIndex(ti.data, now)
		assert.NoError(t, err)
		assert.EqualValues(t, ti.expRet, magicRes.Ret)
		assert.EqualValues(t, ti.expStartIndex, magicRes.TimeStart)
		assert.EqualValues(t, ti.expEndIndex, magicRes.TimeEnd)
		assert.EqualValues(t, ti.expTimeIndex, magicRes.RemainIndex)
	}

	errData := "x@(M)@(DD)"
	expRet := "x@(M)@(DD)"
	magicRes, err := GoMagicIndex(errData, now)
	assert.Error(t, err)
	assert.EqualValues(t, expRet, magicRes.Ret)
}

func TestConvertMagicIndex(t *testing.T) {
	now1, _ := time.Parse(time.RFC3339, "2017-04-01T06:06:09+08:00")
	now2, _ := time.Parse(time.RFC3339, "2017-11-11T16:16:29+08:00")
	tests := []struct {
		data     string
		exp1     string
		exp2     string
		expIndex int
	}{
		{
			data:     "YYYY",
			exp1:     "2017",
			exp2:     "2017",
			expIndex: YEAR,
		},
		{
			data:     "YY",
			exp1:     "17",
			exp2:     "17",
			expIndex: YEAR,
		},
		{
			data:     "MM",
			exp1:     "04",
			exp2:     "11",
			expIndex: MONTH,
		},
		{
			data:     "DD",
			exp1:     "01",
			exp2:     "11",
			expIndex: DAY,
		},
		{
			data:     "hh",
			exp1:     "06",
			exp2:     "16",
			expIndex: HOUR,
		},
		{
			data:     "mm",
			exp1:     "06",
			exp2:     "16",
			expIndex: MINUTE,
		},
		{
			data:     "m",
			exp1:     "",
			exp2:     "",
			expIndex: -1,
		},
		{
			data:     "ss",
			exp1:     "09",
			exp2:     "29",
			expIndex: SECOND,
		},
		{
			data:     "s",
			exp1:     "",
			exp2:     "",
			expIndex: -1,
		},
	}
	for _, ti := range tests {
		got, gotIndex := ConvertMagicIndex(ti.data, now1)
		assert.Equal(t, ti.exp1, got)
		assert.Equal(t, ti.expIndex, gotIndex)
		got, gotIndex = ConvertMagicIndex(ti.data, now2)
		assert.Equal(t, ti.exp2, got)
		assert.Equal(t, ti.expIndex, gotIndex)
	}
}
