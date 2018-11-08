package mutate

import (
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/qiniu/logkit/utils/models"
)

func TestTrimTransformer(t *testing.T) {
	t.Parallel()
	tr := &Trim{
		Key:        "myword",
		Characters: "1",
		Place:      Both,
	}
	data, err := tr.Transform([]Data{{"myword": "1111hello x1 y2 x1nihao"}, {"myword": "x1x.x.x11"}})
	assert.NoError(t, err)
	exp := []Data{
		{"myword": "hello x1 y2 x1nihao"},
		{"myword": "x1x.x.x"}}
	assert.Equal(t, exp, data)

	tr = &Trim{
		Key:        "myword",
		Characters: "hello",
		Place:      Prefix,
	}
	data, err = tr.Transform([]Data{{"myword": "hellohello x1 y2 x1nihao"}, {"myword": "x1x.x.x11"}})
	assert.NoError(t, err)
	exp = []Data{
		{"myword": " x1 y2 x1nihao"},
		{"myword": "x1x.x.x11"}}
	assert.Equal(t, exp, data)

	tr = &Trim{
		Key:        "myword",
		Characters: "\"",
		Place:      Suffix,
	}
	data, err = tr.Transform([]Data{{"myword": "hello x1 y2 x1nihao"}, {"myword": `x1x.x.x11"""""`}})
	assert.NoError(t, err)
	exp = []Data{
		{"myword": "hello x1 y2 x1nihao"},
		{"myword": "x1x.x.x11"}}
	assert.Equal(t, exp, data)
}
