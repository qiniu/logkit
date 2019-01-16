package mutate

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

func TestCopyTransformer(t *testing.T) {
	// simple label
	copy := &Copy{
		Key: "old",
		New: "new",
	}
	data, err := copy.Transform([]Data{{"old": "old_value"}, {"old": "old_value", "new": "new_value"}})
	assert.EqualValues(t, err, errors.New("find total 1 erorrs in transform copy, last error info is the key new already exists"))
	exp := []Data{{"old": "old_value", "new": "old_value"}, {"old": "old_value", "new": "new_value"}}
	assert.Equal(t, exp, data)
	assert.Equal(t, copy.Stage(), transforms.StageAfterParser)

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
