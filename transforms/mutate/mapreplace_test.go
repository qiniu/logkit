package mutate

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

func TestMapReplaceTransformer(t *testing.T) {
	t.Parallel()
	gsub := &MapReplacer{
		Key: "myword",
		Map: "x1 y1,x2 y2",
	}
	err := gsub.Init()
	assert.NoError(t, err)
	data, err := gsub.Transform([]Data{{"myword": "x1"}, {"myword": "x12"}})
	assert.NoError(t, err)
	exp := []Data{
		{"myword": "y1"},
		{"myword": "x12"}}
	assert.Equal(t, exp, data)

	jdata, err := json.Marshal(map[string]string{"abc": "123", "456": "xyz"})
	assert.NoError(t, err)
	dir := "TestMapReplaceTransformer"
	err = os.Mkdir(dir, 0755)
	assert.NoError(t, err)
	defer os.RemoveAll(dir)
	file := "test.json"
	err = ioutil.WriteFile(filepath.Join(dir, file), jdata, 0666)
	assert.NoError(t, err)

	gsub3 := &MapReplacer{
		Key:     "myword",
		MapFile: filepath.Join(dir, file),
	}
	err = gsub3.Init()
	assert.NoError(t, err)
	data3, err := gsub3.Transform([]Data{{"myword": "abc"}, {"myword": "456"}})
	assert.NoError(t, err)
	exp3 := []Data{
		{"myword": "123"},
		{"myword": "xyz"}}
	assert.Equal(t, exp3, data3)

	gsub4 := &MapReplacer{
		Key:     "myword",
		MapFile: filepath.Join(dir, file),
		New:     "myword_new",
	}
	err = gsub4.Init()
	assert.NoError(t, err)
	data4, err := gsub4.Transform([]Data{{"myword": "abc"}, {"myword": "456"}})
	assert.NoError(t, err)
	exp4 := []Data{
		{"myword": "abc", "myword_new": "123"},
		{"myword": "456", "myword_new": "xyz"}}
	assert.Equal(t, exp4, data4)

	gsub5 := &MapReplacer{
		Key: "myword",
		Map: "x1 y1,x2 y2",
		New: "myword_new",
	}
	err = gsub5.Init()
	assert.NoError(t, err)
	data5, err := gsub5.Transform([]Data{{"myword": "x1"}, {"myword": "x12"}})
	assert.NoError(t, err)
	exp5 := []Data{
		{"myword": "x1", "myword_new": "y1"},
		{"myword": "x12"}}
	assert.Equal(t, exp5, data5)

	gsub6 := &MapReplacer{
		Key: "myword",
		Map: "x1 y1,x2 y2",
		New: "myword",
	}
	err = gsub6.Init()
	assert.NoError(t, err)
	data6, err := gsub6.Transform([]Data{{"myword": "x1"}, {"myword": "x12"}})
	assert.NoError(t, err)
	exp6 := []Data{
		{"myword": "y1"},
		{"myword": "x12"}}
	assert.Equal(t, exp6, data6)
}

func TestMapReplaceTransformerWithConvert(t *testing.T) {
	t.Parallel()
	gsub := &MapReplacer{
		Key: "myword",
		Map: "1 y1,2 y2",
	}
	err := gsub.Init()
	assert.NoError(t, err)
	data, err := gsub.Transform([]Data{{"myword": 1}, {"myword": 2}})
	assert.NoError(t, err)
	exp := []Data{
		{"myword": "y1"},
		{"myword": "y2"}}
	assert.Equal(t, exp, data)
	assert.Equal(t, transforms.StageAfterParser, gsub.Stage())

	gsub = &MapReplacer{
		Key: "myword",
		Map: "1 y1,2 2",
		New: "myword_new",
	}
	err = gsub.Init()
	assert.NoError(t, err)
	data, err = gsub.Transform([]Data{{"myword": 1}, {"myword": 2}})
	assert.NoError(t, err)
	exp = []Data{
		{"myword": 1, "myword_new": "y1"},
		{"myword": 2, "myword_new": "2"}}
	assert.Equal(t, exp, data)
	assert.Equal(t, transforms.StageAfterParser, gsub.Stage())
}

func TestMapReplaceConvert(t *testing.T) {
	t.Parallel()
	gsub := &MapReplacer{
		Key: "myword",
		Map: "1 y1,2 2",
	}
	err := gsub.Init()
	assert.NoError(t, err)
	setValue, set := gsub.convert("myword")
	assert.False(t, set)
	assert.Equal(t, "myword", setValue)

	setValue, set = gsub.convert("1")
	assert.True(t, set)
	assert.Equal(t, "y1", setValue)

	setValue, set = gsub.convert("2")
	assert.True(t, set)
	assert.Equal(t, "2", setValue)
}
