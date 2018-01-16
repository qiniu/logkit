package mutate

import (
	"testing"

	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/transforms"

	"github.com/stretchr/testify/assert"
)

func TestRenameTransformer(t *testing.T) {
	// rename plain field
	rename := &Rename{
		Key:        "ts",
		NewKeyName: "@timestamp",
	}
	data, err := rename.Transform([]sender.Data{{"ts": "stamp1"}, {"ts": "stamp2"}})
	assert.NoError(t, err)
	exp := []sender.Data{{"@timestamp": "stamp1"}, {"@timestamp": "stamp2"}}
	assert.Equal(t, exp, data)
	assert.Equal(t, rename.Stage(), transforms.StageAfterParser)

	// rename nested field to current place
	rename2 := &Rename{
		Key:        "a.ts",
		NewKeyName: "a.@timestamp",
	}
	data2, err := rename2.Transform([]sender.Data{{"a": map[string]interface{}{"ts": "stamp1"}}, {"ts": "stamp2"}})
	assert.NoError(t, err)
	exp2 := []sender.Data{{"a": map[string]interface{}{"@timestamp": "stamp1"}}, {"ts": "stamp2"}}
	assert.Equal(t, exp2, data2)

	// rename nested field to new place
	rename3 := &Rename{
		Key:        "a.ts",
		NewKeyName: "b.@timestamp",
	}
	data3, err := rename3.Transform([]sender.Data{{"a": map[string]interface{}{"ts": "stamp1"}}})
	assert.NoError(t, err)
	exp3 := []sender.Data{{"a": map[string]interface{}{}, "b": map[string]interface{}{"@timestamp": "stamp1"}}}
	assert.Equal(t, exp3, data3)
}
