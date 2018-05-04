package mutate

import (
	"testing"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/stretchr/testify/assert"
)

func TestArrayExpand(t *testing.T) {
	ae := &ArrayExpand{
		Key: "myword",
	}
	data, err := ae.Transform([]Data{
		{"myword": []interface{}{"a", "b", "c", "d", "e"}},
		{"myword": []interface{}{1, 2, 3, 4, 5}},
	})
	assert.NoError(t, err)
	exp := []Data{
		{
			"myword":  []interface{}{"a", "b", "c", "d", "e"},
			"myword0": "a",
			"myword1": "b",
			"myword2": "c",
			"myword3": "d",
			"myword4": "e",
		},
		{
			"myword":  []interface{}{1, 2, 3, 4, 5},
			"myword0": 1,
			"myword1": 2,
			"myword2": 3,
			"myword3": 4,
			"myword4": 5,
		},
	}
	assert.Equal(t, len(exp), len(data))
	for i, ex := range exp {
		da := data[i]
		for k, v := range ex {
			d, exist := da[k]
			assert.Equal(t, true, exist)
			assert.Equal(t, v, d)
		}
	}
	assert.Equal(t, ae.Stage(), transforms.StageAfterParser)
	assert.Equal(t, StatsInfo{Success: 2}, ae.stats)
}

func TestArrayExpandError(t *testing.T) {
	ae := &ArrayExpand{
		Key: "myword",
	}
	data, err := ae.Transform([]Data{
		{"myword": "aaaaaaaaaaaaaaaa"},
		{"myword": []interface{}{1, 2, 3, 4, 5}},
	})
	assert.Error(t, err)
	exp := []Data{
		{
			"myword": "aaaaaaaaaaaaaaaa",
		},
		{
			"myword":  []interface{}{1, 2, 3, 4, 5},
			"myword0": 1,
			"myword1": 2,
			"myword2": 3,
			"myword3": 4,
			"myword4": 5,
		},
	}
	assert.Equal(t, len(exp), len(data))
	for i, ex := range exp {
		da := data[i]
		for k, v := range ex {
			d, exist := da[k]
			assert.Equal(t, true, exist)
			assert.Equal(t, v, d)
		}
	}
	ae.stats.LastError = ""
	assert.Equal(t, ae.Stage(), transforms.StageAfterParser)
	assert.Equal(t, StatsInfo{Success: 1, Errors: 1}, ae.stats)
}

func TestArrayExpandIgnore(t *testing.T) {
	ae := &ArrayExpand{
		Key: "myword",
	}
	data, err := ae.Transform([]Data{
		{"myword": "aaaaaaaaaaaaaaaa"},
		{
			"myword":  []interface{}{1, 2, 3, 4, 5},
			"myword0": "aaa",
		},
		{
			"myword":  []interface{}{1, 2, 3, 4, 5},
			"myword0": "aaa",
			"myword1": "aaa",
			"myword2": "aaa",
			"myword3": "aaa",
		},
		{
			"myword":    []interface{}{1, 2, 3, 4, 5},
			"myword0":   "aaa",
			"myword0_0": "aaa",
			"myword0_1": "aaa",
			"myword0_2": "aaa",
			"myword0_3": "aaa",
			"myword0_4": "aaa",
			"myword0_5": "aaa",
		},
	})
	assert.Error(t, err)
	exp := []Data{
		{
			"myword": "aaaaaaaaaaaaaaaa",
		},
		{
			"myword":    []interface{}{1, 2, 3, 4, 5},
			"myword0":   "aaa",
			"myword0_0": 1,
			"myword1":   2,
			"myword2":   3,
			"myword3":   4,
			"myword4":   5,
		},
		{
			"myword":    []interface{}{1, 2, 3, 4, 5},
			"myword0":   "aaa",
			"myword1":   "aaa",
			"myword2":   "aaa",
			"myword3":   "aaa",
			"myword0_0": 1,
			"myword1_0": 2,
			"myword2_0": 3,
			"myword3_0": 4,
			"myword4":   5,
		},
		{
			"myword":    []interface{}{1, 2, 3, 4, 5},
			"myword0":   "aaa",
			"myword0_0": "aaa",
			"myword0_1": "aaa",
			"myword0_2": "aaa",
			"myword0_3": "aaa",
			"myword0_4": "aaa",
			"myword0_5": "aaa",
			"myword1":   2,
			"myword2":   3,
			"myword3":   4,
			"myword4":   5,
		},
	}
	assert.Equal(t, len(exp), len(data))
	for i, ex := range exp {
		da := data[i]
		for k, v := range ex {
			d, exist := da[k]
			assert.Equal(t, true, exist)
			assert.Equal(t, v, d)
		}
	}
	ae.stats.LastError = ""
	assert.Equal(t, ae.Stage(), transforms.StageAfterParser)
	assert.Equal(t, StatsInfo{Success: 3, Errors: 1}, ae.stats)
}

func TestArrayExpandEveryType(t *testing.T) {
	ae := &ArrayExpand{
		Key: "myword",
	}
	data, err := ae.Transform([]Data{
		{"myword": "aaaaaaaaaaaaaaaaaa"},
		{"myword": []int{1, 2, 3, 4, 5}},
		{"myword": []int8{1, 2, 3, 4, 5}},
		{"myword": []int16{1, 2, 3, 4, 5}},
		{"myword": []int64{1, 2, 3, 4, 5}},
		{"myword": []uint{1, 2, 3, 4, 5}},
		{"myword": []uint8{1, 2, 3, 4, 5}},
		{"myword": []uint16{1, 2, 3, 4, 5}},
		{"myword": []uint32{1, 2, 3, 4, 5}},
		{"myword": []uint64{1, 2, 3, 4, 5}},
		{"myword": []bool{true, true, false, true, false}},
		{"myword": []string{"1", "2", "3", "4", "x"}},
		{"myword": []float32{1.0, 2.0, 0.3, 0.45, 5.1}},
		{"myword": []float64{1.0, 2.0, 0.3, 0.45, 5.1}},
		{"myword": []complex64{1 + 5i, 2, 3 + 2i, 4, 5}},
		{"myword": []complex128{1, 2 + 9i, 3, 4, 5 + 6i}},
		{"myword": []interface{}{1, 2, 3, 4, 5}},
		{"myword": []interface{}{"1", "2", "3", "4", "x"}},
		{"myword": []interface{}{1.0, 2.0, 3.0, 4.0, 5.0}},
		{"myword": []interface{}{true, false, true, false, false}},
		{"myword": []interface{}{true, 1, 0.1, "a", 0.11}},
		{"myword": []byte{1, 2, 3, 4, 5}},
		{"myword": []rune{1, 2, 3, 4, 5}},
	})
	assert.Error(t, err)
	exp := []Data{
		{"myword": "aaaaaaaaaaaaaaaaaa"},
		{
			"myword":  []int{1, 2, 3, 4, 5},
			"myword0": int(1),
			"myword1": int(2),
			"myword2": int(3),
			"myword3": int(4),
			"myword4": int(5),
		},
		{
			"myword":  []int8{1, 2, 3, 4, 5},
			"myword0": int8(1),
			"myword1": int8(2),
			"myword2": int8(3),
			"myword3": int8(4),
			"myword4": int8(5),
		},
		{
			"myword":  []int16{1, 2, 3, 4, 5},
			"myword0": int16(1),
			"myword1": int16(2),
			"myword2": int16(3),
			"myword3": int16(4),
			"myword4": int16(5),
		},
		{
			"myword":  []int64{1, 2, 3, 4, 5},
			"myword0": int64(1),
			"myword1": int64(2),
			"myword2": int64(3),
			"myword3": int64(4),
			"myword4": int64(5),
		},
		{
			"myword":  []uint{1, 2, 3, 4, 5},
			"myword0": uint(1),
			"myword1": uint(2),
			"myword2": uint(3),
			"myword3": uint(4),
			"myword4": uint(5),
		},
		{
			"myword":  []uint8{1, 2, 3, 4, 5},
			"myword0": uint8(1),
			"myword1": uint8(2),
			"myword2": uint8(3),
			"myword3": uint8(4),
			"myword4": uint8(5),
		},
		{
			"myword":  []uint16{1, 2, 3, 4, 5},
			"myword0": uint16(1),
			"myword1": uint16(2),
			"myword2": uint16(3),
			"myword3": uint16(4),
			"myword4": uint16(5),
		},
		{
			"myword":  []uint32{1, 2, 3, 4, 5},
			"myword0": uint32(1),
			"myword1": uint32(2),
			"myword2": uint32(3),
			"myword3": uint32(4),
			"myword4": uint32(5),
		},
		{
			"myword":  []uint64{1, 2, 3, 4, 5},
			"myword0": uint64(1),
			"myword1": uint64(2),
			"myword2": uint64(3),
			"myword3": uint64(4),
			"myword4": uint64(5),
		},
		{
			"myword":  []bool{true, true, false, true, false},
			"myword0": true,
			"myword1": true,
			"myword2": false,
			"myword3": true,
			"myword4": false,
		},
		{
			"myword":  []string{"1", "2", "3", "4", "x"},
			"myword0": "1",
			"myword1": "2",
			"myword2": "3",
			"myword3": "4",
			"myword4": "x",
		},
		{
			"myword":  []float32{1.0, 2.0, 0.3, 0.45, 5.1},
			"myword0": float32(1.0),
			"myword1": float32(2.0),
			"myword2": float32(0.3),
			"myword3": float32(0.45),
			"myword4": float32(5.1),
		},
		{
			"myword":  []float64{1.0, 2.0, 0.3, 0.45, 5.1},
			"myword0": float64(1.0),
			"myword1": float64(2.0),
			"myword2": float64(0.3),
			"myword3": float64(0.45),
			"myword4": float64(5.1),
		},
		{
			"myword":  []complex64{1 + 5i, 2, 3 + 2i, 4, 5},
			"myword0": complex64(1 + 5i),
			"myword1": complex64(2),
			"myword2": complex64(3 + 2i),
			"myword3": complex64(4),
			"myword4": complex64(5),
		},
		{
			"myword":  []complex128{1, 2 + 9i, 3, 4, 5 + 6i},
			"myword0": complex128(1),
			"myword1": complex128(2 + 9i),
			"myword2": complex128(3),
			"myword3": complex128(4),
			"myword4": complex128(5 + 6i),
		},
		{
			"myword":  []interface{}{1, 2, 3, 4, 5},
			"myword0": 1,
			"myword1": 2,
			"myword2": 3,
			"myword3": 4,
			"myword4": 5,
		},
		{
			"myword":  []interface{}{"1", "2", "3", "4", "x"},
			"myword0": "1",
			"myword1": "2",
			"myword2": "3",
			"myword3": "4",
			"myword4": "x",
		},
		{
			"myword":  []interface{}{1.0, 2.0, 3.0, 4.0, 5.0},
			"myword0": 1.0,
			"myword1": 2.0,
			"myword2": 3.0,
			"myword3": 4.0,
			"myword4": 5.0,
		},
		{
			"myword":  []interface{}{true, false, true, false, false},
			"myword0": true,
			"myword1": false,
			"myword2": true,
			"myword3": false,
			"myword4": false,
		},
		{
			"myword":  []interface{}{true, 1, 0.1, "a", 0.11},
			"myword0": true,
			"myword1": 1,
			"myword2": 0.1,
			"myword3": "a",
			"myword4": 0.11,
		},
		{
			"myword":  []byte{1, 2, 3, 4, 5},
			"myword0": byte(1),
			"myword1": byte(2),
			"myword2": byte(3),
			"myword3": byte(4),
			"myword4": byte(5),
		},
		{
			"myword":  []rune{1, 2, 3, 4, 5},
			"myword0": rune(1),
			"myword1": rune(2),
			"myword2": rune(3),
			"myword3": rune(4),
			"myword4": rune(5),
		},
	}
	assert.Equal(t, len(exp), len(data))
	for i, ex := range exp {
		da := data[i]
		for k, v := range ex {
			d, exist := da[k]
			assert.Equal(t, true, exist)
			assert.Equal(t, v, d)
		}
	}
	ae.stats.LastError = ""
	assert.Equal(t, ae.Stage(), transforms.StageAfterParser)
	assert.Equal(t, StatsInfo{Success: 22, Errors: 1}, ae.stats)
}

func TestArrayExpandMultiKey(t *testing.T) {
	ae := &ArrayExpand{
		Key: "multi.myWord",
	}
	data, err := ae.Transform([]Data{
		{"multi": map[string]interface{}{"myWord": []interface{}{"a", "b", "c", "d", "e"}}},
		{"multi": map[string]interface{}{"myWord": []interface{}{1, 2, 3, 4, 5}}},
	})
	assert.NoError(t, err)
	exp := []Data{
		{
			"multi": map[string]interface{}{
				"myWord":  []interface{}{"a", "b", "c", "d", "e"},
				"myWord0": "a",
				"myWord1": "b",
				"myWord2": "c",
				"myWord3": "d",
				"myWord4": "e",
			},
		},
		{
			"multi": map[string]interface{}{
				"myWord":  []interface{}{1, 2, 3, 4, 5},
				"myWord0": 1,
				"myWord1": 2,
				"myWord2": 3,
				"myWord3": 4,
				"myWord4": 5,
			},
		},
	}
	assert.Equal(t, len(exp), len(data))
	for i, ex := range exp {
		da := data[i]["multi"].(map[string]interface{})
		for k, v := range ex["multi"].(map[string]interface{}) {
			d, exist := da[k]
			assert.Equal(t, true, exist)
			assert.Equal(t, v, d)
		}
	}
	assert.Equal(t, ae.Stage(), transforms.StageAfterParser)
	assert.Equal(t, StatsInfo{Success: 2}, ae.stats)
}
