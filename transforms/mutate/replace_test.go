package mutate

import (
	"testing"

	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/transforms"
	"github.com/stretchr/testify/assert"
)

func TestReplaceTransformer(t *testing.T) {
	gsub := &Replacer{
		Key: "myword",
		Old: "x1",
		New: "y2",
	}
	gsub.Init()
	data, err := gsub.Transform([]sender.Data{{"myword": "hello x1 y2 x1nihao", "abc": "x1 y2"}, {"myword": "x1x.x.x11", "abc": "x1"}})
	assert.NoError(t, err)
	exp := []sender.Data{
		{"myword": "hello y2 y2 y2nihao", "abc": "x1 y2"},
		{"myword": "y2x.x.y21", "abc": "x1"}}
	assert.Equal(t, exp, data)
	gsub2 := &Replacer{
		Key: "myword",
		Old: `\\x`,
		New: `\\x`,
	}
	gsub2.Init()
	newd, err := gsub2.RawTransform([]string{`\x0A`, "hello"})
	assert.NoError(t, err)
	expdata := []string{`\\x0A`, "hello"}
	assert.Equal(t, expdata, newd)
	assert.Equal(t, gsub.Stage(), transforms.StageBeforeParser)
}
