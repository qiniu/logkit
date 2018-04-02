package mutate

import (
	"testing"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/stretchr/testify/assert"
)

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
	data3, err := rename3.Transform([]Data{{"a": map[string]interface{}{"ts": "stamp1"}}})
	assert.NoError(t, err)
	exp3 := []Data{{"a": map[string]interface{}{}, "b": map[string]interface{}{"@timestamp": "stamp1"}}}
	assert.Equal(t, exp3, data3)
}
