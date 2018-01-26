package mutate

import (
	"testing"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/stretchr/testify/assert"
)

func TestSplitTransformer(t *testing.T) {
	gsub := &Spliter{
		Key:         "myword",
		SeperateKey: " ",
		ArraryName:  "result",
	}
	data, err := gsub.Transform([]Data{{"myword": "hello x1 y2 x1nihao", "abc": "x1 y2"}, {"myword": "x1x x x11", "abc": "x1"}})
	assert.NoError(t, err)
	exp := []Data{
		{"myword": "hello x1 y2 x1nihao", "abc": "x1 y2", "result": []string{"hello", "x1", "y2", "x1nihao"}},
		{"myword": "x1x x x11", "abc": "x1", "result": []string{"x1x", "x", "x11"}}}
	assert.Equal(t, exp, data)

	assert.Equal(t, gsub.Stage(), transforms.StageAfterParser)

	gsub2 := &Spliter{
		Key:         "multi.myword",
		SeperateKey: " ",
		ArraryName:  "result",
	}
	data2, err2 := gsub2.Transform([]Data{{"multi": map[string]interface{}{"myword": "hello x1 y2 x1nihao", "abc": "x1 y2"}}, {"multi": map[string]interface{}{"myword": "x1x x x11", "abc": "x1"}}})
	assert.NoError(t, err2)
	exp2 := []Data{
		{"multi": map[string]interface{}{"myword": "hello x1 y2 x1nihao", "abc": "x1 y2", "result": []string{"hello", "x1", "y2", "x1nihao"}}},
		{"multi": map[string]interface{}{"myword": "x1x x x11", "abc": "x1", "result": []string{"x1x", "x", "x11"}}}}
	assert.Equal(t, exp2, data2)

	assert.Equal(t, gsub2.Stage(), transforms.StageAfterParser)
}
