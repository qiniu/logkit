package mutate

import (
	"testing"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/stretchr/testify/assert"
)

func TestReplaceTransformer(t *testing.T) {
	gsub := &Replacer{
		Key: "myword",
		Old: "x1",
		New: "y2",
	}
	gsub.Init()
	data, err := gsub.Transform([]Data{{"myword": "hello x1 y2 x1nihao", "abc": "x1 y2"}, {"myword": "x1x.x.x11", "abc": "x1"}})
	assert.NoError(t, err)
	exp := []Data{
		{"myword": "hello y2 y2 y2nihao", "abc": "x1 y2"},
		{"myword": "y2x.x.y21", "abc": "x1"}}
	assert.Equal(t, exp, data)
	gsub2 := &Replacer{
		Key: "myword",
		Old: `\x`,
		New: `\\x`,
	}
	gsub2.Init()
	newd, err := gsub2.RawTransform([]string{`\x0A`, "hello"})
	assert.NoError(t, err)
	expdata := []string{`\\x0A`, "hello"}
	assert.Equal(t, expdata, newd)
	assert.Equal(t, gsub.Stage(), transforms.StageBeforeParser)

	gsub3 := &Replacer{
		Key:   "multi.myword",
		Old:   "\\d",
		New:   "0",
		Regex: true,
	}
	gsub3.Init()
	data3, err3 := gsub3.Transform([]Data{{"multi": map[string]interface{}{"myword": "hello x1 y2 x1nihao", "abc": "x1 y2"}}, {"multi": map[string]interface{}{"myword": "x1x.x.x11", "abc": "x1"}}})
	assert.NoError(t, err3)
	exp3 := []Data{
		{"multi": map[string]interface{}{"myword": "hello x0 y0 x0nihao", "abc": "x1 y2"}},
		{"multi": map[string]interface{}{"myword": "x0x.x.x00", "abc": "x1"}}}
	assert.Equal(t, exp3, data3)
}
