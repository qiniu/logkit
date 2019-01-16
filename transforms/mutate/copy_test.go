package mutate

import (
	"errors"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

func TestCopyTransformer(t *testing.T) {
	// simple copy
	copy := &Copy{
		Key: "old",
		New: "new",
	}
	testDatas := []Data{{"old": map[string]interface{}{"key1": "old_value"}}, {"old": "old_value", "new": "new_value"}}
	data, err := copy.Transform(testDatas)
	assert.EqualValues(t, err, errors.New("find total 1 erorrs in transform copy, last error info is the key new already exists"))
	exp := []Data{{"old": map[string]interface{}{"key1": "old_value"}, "new": map[string]interface{}{"key1": "old_value"}}, {"old": "old_value", "new": "new_value"}}
	assert.Equal(t, exp, data)
	assert.Equal(t, copy.Stage(), transforms.StageAfterParser)
	replace := &Replacer{
		Key: "old.key1",
		Old: "old_value",
		New: "new_value",
	}
	replace.Init()
	actual, err := replace.Transform(data)
	exp = []Data{{"old": map[string]interface{}{"key1": "new_value"}, "new": map[string]interface{}{"key1": "old_value"}}, {"old": "old_value", "new": "new_value"}}
	assert.EqualValues(t, exp, actual)

	// no override explicitly
	copy2 := &Copy{
		Key:      "old.key1",
		New:      "new.key2",
		Override: false,
	}
	data2, err := copy2.Transform([]Data{{"old": map[string]interface{}{"key1": "old_value"}}})
	assert.Nil(t, err)
	exp2 := []Data{{"old": map[string]interface{}{"key1": "old_value"}, "new": map[string]interface{}{"key2": "old_value"}}}
	assert.Equal(t, exp2, data2)

	// override explicitly
	copy3 := &Copy{
		Key:      "old",
		New:      "new.key1",
		Override: true,
	}
	data3, err := copy3.Transform([]Data{{"old": "old_value", "new": map[string]interface{}{"key1": 1}}})
	assert.NoError(t, err)
	exp3 := []Data{{"old": "old_value", "new": map[string]interface{}{"key1": "old_value"}}}
	assert.Equal(t, exp3, data3)
}

func Test_copyData(t *testing.T) {
	data := map[string]interface{}{
		"a":   "a",
		"b":   1,
		"c":   nil,
		"g":   uint64(9223372036854776000),
		"h":   uint64(9223372036851822000),
		"i":   uint64(1),
		"j":   []interface{}{int(1), uint64(9223372036854776000), uint64(9223372036851822000)},
		"k":   []uint64{uint64(1), uint64(9223372036854776000), uint64(9223372036851822000)},
		"l":   []interface{}{uint64(9223372036854776000), int(1), uint64(9223372036851822000)},
		"e":   []interface{}{},
		"xxx": "",
		"f":   map[string]interface{}{},
		"d": map[string]interface{}{
			"a_1": "a",
			"b1":  1,
			"c_1": nil,
			"e1":  []interface{}{1, 2, "3"},
			"d1": map[string]interface{}{
				"a2":  "a",
				"b_2": 1,
				"xxx": "",
				"c_2": nil,
				"e2":  []string{"a", "b", "c"},
				"d_2": map[string]interface{}{
					"a_3": "a",
					"b_3": 1,
					"c3":  nil,
					"e3":  []int{1, 2, 3, 4, 5},
					"d_3": map[string]interface{}{
						"a4": "a",
						"b4": 1,
						"c4": nil,
						"d4": map[string]interface{}{},
					},
					"f3": map[string]interface{}{
						"a4": "a",
						"b4": 1,
						"c4": nil,
						"d4": map[string]interface{}{
							"a5": "a",
							"b5": 1,
							"c5": nil,
							"d5": []string{"1", "2", "3", "4"},
						},
					},
					"g3": map[string]interface{}{
						"a4": "a",
						"b4": 1,
						"c4": nil,
						"d4": map[string]interface{}{
							"a5": "a",
							"b5": 1,
							"c5": nil,
							"d5": map[string]interface{}{
								"a6": "a",
								"b6": 1,
								"c6": nil,
								"d6": []int{1, 2, 3, 4, 5},
							},
						},
					},
				},
			},
		},
	}
	expData := map[string]interface{}{
		"a": "a",
		"b": 1,
		"i": int64(1),
		"h": int64(9223372036851822000),
		"g": int64(-9223372036854775616),
		"k": []int64{int64(1), int64(-9223372036854775616), int64(9223372036851822000)},
		"l": []interface{}{int64(-9223372036854775616), int(1), int64(9223372036851822000)},
		"j": []interface{}{int(1), uint64(9223372036854776000), uint64(9223372036851822000)},
		"d": map[string]interface{}{
			"a_1": "a",
			"b1":  1,
			"e1":  []interface{}{1, 2, "3"},
			"d1": map[string]interface{}{
				"a2":  "a",
				"b_2": 1,
				"e2":  []string{"a", "b", "c"},
				"d_2": map[string]interface{}{
					"a_3": "a",
					"b_3": 1,
					"e3":  []int{1, 2, 3, 4, 5},
					"d_3": map[string]interface{}{
						"a4": "a",
						"b4": 1,
					},
					"f3": map[string]interface{}{
						"a4": "a",
						"b4": 1,
						"d4": `{"a5":"a","b5":1,"c5":null,"d5":["1","2","3","4"]}`,
					},
					"g3": map[string]interface{}{
						"a4": "a",
						"b4": 1,
						"d4": `{"a5":"a","b5":1,"c5":null,"d5":{"a6":"a","b6":1,"c6":null,"d6":[1,2,3,4,5]}}`,
					},
				},
			},
		},
	}
	gotData := copyData(data, 1)
	if !reflect.DeepEqual(expData, gotData) {
		t.Fatalf("test error exp %v, but got %v", expData, gotData)
	}
}
