package sql

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_CompareTime(t *testing.T) {
	tests := []struct {
		data       string
		match      string
		startIndex []int
		endIndex   []int
		expRes     bool
	}{
		{
			data:       "x02abc01",
			match:      "x02abc01",
			startIndex: []int{-1, 1, 6, -1, -1, -1},
			endIndex:   []int{0, 3, 8, 0, 0, 0},
			expRes:     true,
		},
		{
			data:       "hhhhh",
			match:      "hhhhh",
			startIndex: []int{-1, -1, -1, -1, -1, -1},
			endIndex:   []int{0, 0, 0, 0, 0, 0},
			expRes:     true,
		},
		{
			data:       "x01abc31def",
			match:      "x02abc01def",
			startIndex: []int{-1, 1, 6, -1, -1, -1},
			endIndex:   []int{0, 3, 8, 0, 0, 0},
			expRes:     true,
		},
		{
			data:       "x01abc31def*",
			match:      "x02abc01def*",
			startIndex: []int{-1, 1, 6, -1, -1, -1},
			endIndex:   []int{0, 3, 8, 0, 0, 0},
			expRes:     true,
		},
		{
			data:       "x0201",
			match:      "x0201",
			startIndex: []int{-1, 1, 3, -1, -1, -1},
			endIndex:   []int{0, 3, 5, 0, 0, 0},
			expRes:     true,
		},
		{
			data:       "x0102abc",
			match:      "x0102*",
			startIndex: []int{-1, 3, 1, -1, -1, -1},
			endIndex:   []int{0, 5, 3, 0, 0, 0},
			expRes:     true,
		},
		{
			data:       "x0102",
			match:      "x0102*",
			startIndex: []int{-1, 3, 1, -1, -1, -1},
			endIndex:   []int{0, 5, 3, 0, 0, 0},
			expRes:     true,
		},
		{
			data:       "17",
			match:      "17",
			startIndex: []int{0, -1, -1, -1, -1, -1},
			endIndex:   []int{2, 0, 0, 0, 0, 0},
			expRes:     true,
		},
		{
			data:       "abcd201703efg*",
			match:      "abcd201702efg*",
			startIndex: []int{4, 8, -1, -1, -1, -1},
			endIndex:   []int{8, 10, 0, 0, 0, 0},
			expRes:     false,
		},
		{
			data:       "abcd20170201160618*",
			match:      "abcd20170201160619*",
			startIndex: []int{4, 8, 10, 12, 14, 16},
			endIndex:   []int{8, 10, 12, 14, 16, 18},
			expRes:     true,
		},
	}
	for _, ti := range tests {
		valid := CompareTime(ti.data, ti.match, ti.startIndex, ti.endIndex, true)
		assert.EqualValues(t, ti.expRes, valid)
	}

	tests2 := []struct {
		data       string
		match      string
		startIndex []int
		endIndex   []int
		expRes     bool
	}{
		{
			data:       "x02abc01",
			match:      "x02abc01",
			startIndex: []int{-1, 1, 6, -1, -1, -1},
			endIndex:   []int{0, 3, 8, 0, 0, 0},
			expRes:     true,
		},
		{
			data:       "hhhhh",
			match:      "hhhhh",
			startIndex: []int{-1, -1, -1, -1, -1, -1},
			endIndex:   []int{0, 0, 0, 0, 0, 0},
			expRes:     true,
		},
		{
			data:       "x01abc31def",
			match:      "x02abc01def",
			startIndex: []int{-1, 1, 6, -1, -1, -1},
			endIndex:   []int{0, 3, 8, 0, 0, 0},
			expRes:     false,
		},
		{
			data:       "x01abc31def*",
			match:      "x02abc01def*",
			startIndex: []int{-1, 1, 6, -1, -1, -1},
			endIndex:   []int{0, 3, 8, 0, 0, 0},
			expRes:     false,
		},
		{
			data:       "x0201",
			match:      "x0201",
			startIndex: []int{-1, 1, 3, -1, -1, -1},
			endIndex:   []int{0, 3, 5, 0, 0, 0},
			expRes:     true,
		},
		{
			data:       "x0102abc",
			match:      "x0102*",
			startIndex: []int{-1, 3, 1, -1, -1, -1},
			endIndex:   []int{0, 5, 3, 0, 0, 0},
			expRes:     true,
		},
		{
			data:       "x0102",
			match:      "x0102*",
			startIndex: []int{-1, 3, 1, -1, -1, -1},
			endIndex:   []int{0, 5, 3, 0, 0, 0},
			expRes:     true,
		},
		{
			data:       "17",
			match:      "17",
			startIndex: []int{0, -1, -1, -1, -1, -1},
			endIndex:   []int{2, 0, 0, 0, 0, 0},
			expRes:     true,
		},
		{
			data:       "abcd201703efg*",
			match:      "abcd201702efg*",
			startIndex: []int{4, 8, -1, -1, -1, -1},
			endIndex:   []int{8, 10, 0, 0, 0, 0},
			expRes:     true,
		},
		{
			data:       "abcd20170201160618*",
			match:      "abcd20170201160619*",
			startIndex: []int{4, 8, 10, 12, 14, 16},
			endIndex:   []int{8, 10, 12, 14, 16, 18},
			expRes:     false,
		},
	}
	for _, ti := range tests2 {
		valid := CompareTime(ti.data, ti.match, ti.startIndex, ti.endIndex, false)
		assert.EqualValues(t, ti.expRes, valid)
	}
}

func Test_EqualTime(t *testing.T) {
	tests := []struct {
		data       string
		match      string
		startIndex []int
		endIndex   []int
		expRes     bool
	}{
		{
			data:       "x02abc01",
			match:      "x02abc01",
			startIndex: []int{-1, 1, 6, -1, -1, -1},
			endIndex:   []int{0, 3, 8, 0, 0, 0},
			expRes:     true,
		},
		{
			data:       "hhhhh",
			match:      "hhhhh",
			startIndex: []int{-1, -1, -1, -1, -1, -1},
			endIndex:   []int{0, 0, 0, 0, 0, 0},
			expRes:     true,
		},
		{
			data:       "x01abc31def",
			match:      "x02abc01def",
			startIndex: []int{-1, 1, 6, -1, -1, -1},
			endIndex:   []int{0, 3, 8, 0, 0, 0},
			expRes:     false,
		},
		{
			data:       "x0201",
			match:      "x0201",
			startIndex: []int{-1, 1, 3, -1, -1, -1},
			endIndex:   []int{0, 3, 5, 0, 0, 0},
			expRes:     true,
		},
		{
			data:       "17",
			match:      "17",
			startIndex: []int{0, -1, -1, -1, -1, -1},
			endIndex:   []int{2, 0, 0, 0, 0, 0},
			expRes:     true,
		},
		{
			data:       "abcd201703efg*",
			match:      "abcd201702efg*",
			startIndex: []int{4, 8, -1, -1, -1, -1},
			endIndex:   []int{8, 10, 0, 0, 0, 0},
			expRes:     false,
		},
		{
			data:       "abcd20170201160618*",
			match:      "abcd20170201160619*",
			startIndex: []int{4, 8, 10, 12, 14, 16},
			endIndex:   []int{8, 10, 12, 14, 16, 18},
			expRes:     false,
		},
	}
	for _, ti := range tests {
		valid := EqualTime(ti.data, ti.match, ti.startIndex, ti.endIndex)
		assert.EqualValues(t, ti.expRes, valid)
	}
}

func Test_CompareRemainStr(t *testing.T) {
	tests := []struct {
		origin    string
		match     string
		matchData string
		timeIndex []int
		expectRes bool
	}{
		{
			origin:    "x02abc01def",
			match:     "xabcdef",
			matchData: "x02abc01def",
			timeIndex: []int{0, 1, 3, 6, 8, 11},
			expectRes: true,
		},
		{
			origin:    "x02abc01defdef",
			match:     "xabcdef",
			matchData: "x02abc01def*",
			timeIndex: []int{0, 1, 3, 6, 8, 12},
			expectRes: true,
		},
		{
			origin:    "x02abc01",
			match:     "xabc",
			matchData: "x02abc01",
			timeIndex: []int{0, 1, 3, 6},
			expectRes: true,
		},
		{
			origin:    "x0201",
			match:     "x",
			matchData: "x0201",
			timeIndex: []int{0, 1},
			expectRes: true,
		},
		{
			origin:    "x0102*",
			match:     "x*",
			matchData: "x0102*",
			timeIndex: []int{0, 1, 5, 6},
			expectRes: true,
		},
		{
			origin:    "17",
			match:     "",
			matchData: "17",
			timeIndex: []int{0, 0},
			expectRes: true,
		},
		{
			origin:    "abcd201702efg*",
			match:     "xabcdef",
			matchData: "abcd201702efg*",
			timeIndex: []int{0, 4, 10, 14},
			expectRes: false,
		},
		{
			origin:    "abcd20170201160619*",
			match:     "xabcdef",
			matchData: "abcd20170201160619*",
			timeIndex: []int{0, 4, 18, 19},
			expectRes: false,
		},
		{
			origin:    "abcd20170201160619ef",
			match:     "abcd",
			matchData: "abcd20170201160619",
			timeIndex: []int{0, 4},
			expectRes: false,
		},
		{
			origin:    "hhhhh",
			match:     "hhhhh",
			matchData: "hhhhh",
			timeIndex: []int{0, 5},
			expectRes: true,
		},
		{
			origin:    "hhhh",
			match:     "hhhhh",
			matchData: "hhhhh",
			timeIndex: []int{0, 5},
			expectRes: false,
		},
	}
	for _, ti := range tests {
		remain := CompareRemainStr(ti.origin, ti.match, ti.matchData, ti.timeIndex)
		assert.Equal(t, ti.expectRes, remain)
	}
}

func Test_GetRemainStr(t *testing.T) {
	tests := []struct {
		origin       string
		timeIndex    []int
		expectRemain string
	}{
		{
			origin:       "x02abc01def",
			timeIndex:    []int{0, 1, 3, 6, 8, 11},
			expectRemain: "xabcdef",
		},
		{
			origin:       "x02abc01def*",
			timeIndex:    []int{0, 1, 3, 6, 8, 11},
			expectRemain: "xabcdef",
		},
		{
			origin:       "x02abc01",
			timeIndex:    []int{0, 1, 3, 6},
			expectRemain: "xabc",
		},
		{
			origin:       "x0201",
			timeIndex:    []int{0, 1},
			expectRemain: "x",
		},
		{
			origin:       "x0102*",
			timeIndex:    []int{0, 1},
			expectRemain: "x",
		},
		{
			origin:       "17",
			timeIndex:    []int{0, 0},
			expectRemain: "",
		},
		{
			origin:       "abcd201702efg*",
			timeIndex:    []int{0, 4, 10, 13},
			expectRemain: "abcdefg",
		},
		{
			origin:       "abcd20170201160619*",
			timeIndex:    []int{0, 4},
			expectRemain: "abcd",
		},
		{
			origin:       "hhhhh",
			timeIndex:    []int{0, 5},
			expectRemain: "hhhhh",
		},
	}
	for _, ti := range tests {
		remain := GetRemainStr(ti.origin, ti.timeIndex)
		assert.Equal(t, ti.expectRemain, remain)
	}
}
