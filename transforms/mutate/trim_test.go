package mutate

import (
	"testing"

	. "github.com/qiniu/logkit/utils/models"

	"github.com/stretchr/testify/assert"
)

func TestTrimTransformer(t *testing.T) {
	tr := &Trim{
		Key:        "myword",
		Characters: "1",
		Place:      Both,
	}
	data, err := tr.Transform([]Data{{"myword": "1hello x1 y2 x1nihao"}, {"myword": "x1x.x.x11"}})
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
	data, err = tr.Transform([]Data{{"myword": "hello x1 y2 x1nihao"}, {"myword": "x1x.x.x11"}})
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
	data, err = tr.Transform([]Data{{"myword": "hello x1 y2 x1nihao"}, {"myword": `x1x.x.x11"`}})
	assert.NoError(t, err)
	exp = []Data{
		{"myword": "hello x1 y2 x1nihao"},
		{"myword": "x1x.x.x11"}}
	assert.Equal(t, exp, data)
}
