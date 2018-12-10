package mutate

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
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

	gsub3 := &Spliter{
		Key:         "multi.myword",
		SeperateKey: " ",
	}
	_, err3 := gsub3.Transform([]Data{
		{"multi": map[string]interface{}{"myword": "hello x1 y2 x1nihao", "abc": "x1 y2"}},
	})
	assert.NotNil(t, err3)
	expectErr := "find total 1 erorrs in transform split, last error info is array name is empty string,can't use as array field key name"
	assert.EqualValues(t, expectErr, err3.Error())
	assert.Equal(t, expectErr, gsub3.stats.LastError)
}
