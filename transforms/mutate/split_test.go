package mutate

import (
	"testing"

	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/transforms"
	"github.com/stretchr/testify/assert"
)

func TestSplitTransformer(t *testing.T) {
	gsub := &Spliter{
		Key:         "myword",
		SeperateKey: " ",
		ArraryName:  "result",
	}
	data, err := gsub.Transform([]sender.Data{{"myword": "hello x1 y2 x1nihao", "abc": "x1 y2"}, {"myword": "x1x x x11", "abc": "x1"}})
	assert.NoError(t, err)
	exp := []sender.Data{
		{"myword": "hello x1 y2 x1nihao", "abc": "x1 y2", "result": []string{"hello", "x1", "y2", "x1nihao"}},
		{"myword": "x1x x x11", "abc": "x1", "result": []string{"x1x", "x", "x11"}}}
	assert.Equal(t, exp, data)

	assert.Equal(t, gsub.Stage(), transforms.StageAfterParser)
}
