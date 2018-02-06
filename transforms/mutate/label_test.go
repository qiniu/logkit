package mutate

import (
	"testing"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/stretchr/testify/assert"
)

func TestLabelTransformer(t *testing.T) {
	// simple label
	label := &Label{
		Key:   "new_key",
		Value: "new_value",
	}
	data, err := label.Transform([]Data{{}, {"old_key": "old_value"}})
	assert.NoError(t, err)
	exp := []Data{{"new_key": "new_value"}, {"old_key": "old_value", "new_key": "new_value"}}
	assert.Equal(t, exp, data)
	assert.Equal(t, label.Stage(), transforms.StageAfterParser)

	// no override explicitly
	label2 := &Label{
		Key:      "new_key",
		Value:    "new_value",
		Override: false,
	}
	data2, err := label2.Transform([]Data{{"new_key": "old_value"}})
	assert.NotNil(t, err)
	assert.Equal(t, int64(1), label2.Stats().Errors)
	exp2 := []Data{{"new_key": "old_value"}}
	assert.Equal(t, exp2, data2)

	// override explicitly
	label3 := &Label{
		Key:      "new_key",
		Value:    "new_value",
		Override: true,
	}
	data3, err := label3.Transform([]Data{{"new_key": "old_value"}})
	assert.NoError(t, err)
	exp3 := []Data{{"new_key": "new_value"}}
	assert.Equal(t, exp3, data3)
}
