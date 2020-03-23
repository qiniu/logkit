package mutate

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	bench     []Data
	testDatas = []Data{
		{"a": "key1"},
		{"b": "key2"},
		{"c": "key3"},
	}
)

// now: Benchmark_Transform-4   	20000000	        69.7 ns/op
func Benchmark_Transform(b *testing.B) {
	t := Rename{}

	var m []Data
	for n := 0; n < b.N; n++ {
		m, _ = t.Transform(testDatas)
	}
	bench = m
}

func TestRenameTransformer(t *testing.T) {
	// rename plain field
	rename := &Rename{
		Key:        "ts",
		NewKeyName: "@timestamp",
	}
	data, err := rename.Transform([]Data{{"ts": "stamp1"}, {"ts": "stamp2"}})
	assert.NoError(t, err)
	exp := []Data{{"@timestamp": "stamp1"}, {"@timestamp": "stamp2"}}
	assert.Equal(t, exp, data)
	assert.Equal(t, rename.Stage(), transforms.StageAfterParser)

	// rename nested field to current place
	rename2 := &Rename{
		Key:        "a.ts",
		NewKeyName: "a.@timestamp",
	}
	data2, err := rename2.Transform([]Data{{"a": map[string]interface{}{"ts": "stamp1"}}})
	assert.NoError(t, err)
	exp2 := []Data{{"a": map[string]interface{}{"@timestamp": "stamp1"}}}
	assert.Equal(t, exp2, data2)

	// rename nested field to new place
	rename3 := &Rename{
		Key:        "a.ts",
		NewKeyName: "b.@timestamp",
	}
	data3, err := rename3.Transform([]Data{{"b": "test"}, {"a": map[string]interface{}{"ts": "stamp1"}}})
	assert.Error(t, err)
	exp3 := []Data{{"b": "test"}, {"a": map[string]interface{}{}, "b": map[string]interface{}{"@timestamp": "stamp1"}}}
	assert.Equal(t, exp3, data3)
}
