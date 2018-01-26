package mutate

import (
	"testing"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/stretchr/testify/assert"
)

func TestConvertTransformer(t *testing.T) {
	gsub := &Converter{
		DSL: "myword array(long)",
	}
	data, err := gsub.Transform([]Data{{"myword": []string{"123", "456"}, "abc": "x1 y2"}, {"myword": []string{"321", "654"}, "abc": "x1"}})
	assert.NoError(t, err)
	exp := []Data{
		{"myword": []interface{}{int64(123), int64(456)}, "abc": "x1 y2"},
		{"myword": []interface{}{int64(321), int64(654)}, "abc": "x1"}}
	assert.Equal(t, exp, data)
	gsub2 := &Converter{
		DSL: "myword",
	}
	newd, err := gsub2.Transform([]Data{{"myword": 123, "abc": "x1 y2"}, {"myword": 654, "abc": "x1"}})
	assert.NoError(t, err)
	exp = []Data{
		{"myword": "123", "abc": "x1 y2"},
		{"myword": "654", "abc": "x1"}}
	assert.Equal(t, exp, newd)
	assert.Equal(t, gsub.Stage(), transforms.StageAfterParser)

	gsub3 := &Converter{
		DSL: "multi.myword array(long)",
	}
	data3, err3 := gsub3.Transform([]Data{{"multi": map[string]interface{}{"myword": []string{"123", "456"}, "abc": "x1 y2"}}, {"multi": map[string]interface{}{"myword": []string{"321", "654"}, "abc": "x1"}}})
	assert.NoError(t, err3)
	exp3 := []Data{
		{"multi": map[string]interface{}{"myword": []interface{}{int64(123), int64(456)}, "abc": "x1 y2"}},
		{"multi": map[string]interface{}{"myword": []interface{}{int64(321), int64(654)}, "abc": "x1"}},
	}
	assert.Equal(t, exp3, data3)
}
