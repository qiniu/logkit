package mutate

import (
	"testing"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/stretchr/testify/assert"
)

func TestDiscardTransformer(t *testing.T) {
	dis := &Discarder{
		Key: "myword",
	}
	data, err := dis.Transform([]Data{{"myword": "hello x1 y2 x1nihao", "abc": "x1 y2"}, {"myword": "x1x.x.x11", "abc": "x1"}})
	assert.NoError(t, err)
	exp := []Data{
		{"abc": "x1 y2"},
		{"abc": "x1"}}
	assert.Equal(t, exp, data)

	assert.Equal(t, dis.Stage(), transforms.StageAfterParser)

	dis2 := &Discarder{
		Key: "multi.myword",
	}
	data2, err2 := dis2.Transform([]Data{{"multi": map[string]interface{}{"myword": "hello x1 y2 x1nihao", "abc": "x1 y2"}}, {"multi": map[string]interface{}{"myword": "x1x.x.x11", "abc": "x1"}}})
	assert.NoError(t, err2)
	exp2 := []Data{
		{"multi": map[string]interface{}{"abc": "x1 y2"}},
		{"multi": map[string]interface{}{"abc": "x1"}}}
	assert.Equal(t, exp2, data2)
	assert.Equal(t, dis2.Stage(), transforms.StageAfterParser)
}
