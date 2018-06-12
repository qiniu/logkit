package mutate

import (
	"testing"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/stretchr/testify/assert"
)

func TestPickTransformer(t *testing.T) {
	pick := &Pick{
		Key: "myword",
	}
	data, err := pick.Transform([]Data{{"myword": "hello x1 y2 x1nihao", "abc": "x1 y2"}, {"myword": "x1x.x.x11", "abc": "x1"}})
	assert.NoError(t, err)
	exp := []Data{
		{"myword": "hello x1 y2 x1nihao"},
		{"myword": "x1x.x.x11"}}
	assert.Equal(t, exp, data)

	assert.Equal(t, pick.Stage(), transforms.StageAfterParser)

	pick2 := &Pick{
		Key: "multi.myword",
	}
	data2, err2 := pick2.Transform([]Data{{"multi": map[string]interface{}{"myword": "hello x1 y2 x1nihao", "abc": "x1 y2"}},
		{"multi": map[string]interface{}{"myword": "x1x.x.x11", "abc": "x1"}}})
	assert.NoError(t, err2)
	exp2 := []Data{
		{"multi": map[string]interface{}{"myword": "hello x1 y2 x1nihao"}},
		{"multi": map[string]interface{}{"myword": "x1x.x.x11"}}}
	assert.Equal(t, exp2, data2)
	assert.Equal(t, pick2.Stage(), transforms.StageAfterParser)

	pick = &Pick{
		Key: "myword,abc",
	}
	data, err = pick.Transform([]Data{{"abc": "x1 y2", "xxx": 123, "myword": "hello x1 y2 x1nihao"}, {"abc": "x1", "nihao": "abc", "myword": "x1x.x.x11"}})
	assert.NoError(t, err)
	exp = []Data{{"myword": "hello x1 y2 x1nihao", "abc": "x1 y2"}, {"abc": "x1", "myword": "x1x.x.x11"}}
	assert.Equal(t, exp, data)

	pick = &Pick{
		Key:       "myword",
		StageTime: "before_parser",
	}
	datas, err := pick.RawTransform([]string{"mywordxxx", "xxx"})
	assert.NoError(t, err)
	assert.Equal(t, []string{"mywordxxx"}, datas)
	assert.Equal(t, transforms.StageBeforeParser, pick.Stage())
}
