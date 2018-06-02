package mutate

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
	"github.com/stretchr/testify/assert"
)

func TestMapReplaceTransformer(t *testing.T) {
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
}

func TestMapReplaceTransformerWithConvert(t *testing.T) {
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
}
