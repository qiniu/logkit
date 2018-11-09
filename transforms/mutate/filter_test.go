package mutate

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

func TestFilterTransformer(t *testing.T) {
	t.Parallel()
	filter := &Filter{
		Key: "myword",
	}
	data, err := filter.Transform([]Data{{"myword": "hello x1 y2 x1nihao", "abc": "x1 y2"}, {"abc": "x1"}})
	assert.NotNil(t, err)
	exp := []Data{{"abc": "x1"}}
	assert.Equal(t, exp, data)
	assert.Equal(t, filter.Stage(), transforms.StageAfterParser)

	dis2 := &Filter{
		Key: "multi.myword",
	}
	data2, err2 := dis2.Transform([]Data{{"multi": map[string]interface{}{"myword": "hello x1 y2 x1nihao", "abc": "x1 y2"}}, {"multi": map[string]interface{}{"myword": "x1x.x.x11", "abc": "x1"}}})
	assert.NoError(t, err2)
	exp2 := []Data{}
	assert.Equal(t, exp2, data2)
	assert.Equal(t, dis2.Stage(), transforms.StageAfterParser)

	filter = &Filter{
		Key: "myword,abc",
	}
	data, err = filter.Transform([]Data{{"abc": "x1 y2", "xxx": 123}, {"myword": "x1x.x.x11", "nihao": "abc"}, {"nihao": "abc"}})
	assert.NotNil(t, err)
	exp = []Data{{"nihao": "abc"}}
	assert.Equal(t, exp, data)

	filter = &Filter{
		Key:           "myword",
		RemovePattern: `.*\[DEBUG\]\[.*`,
	}
	datas, err := filter.Transform([]Data{{"myword": "xxx[DEBUG][qiniu.com/]"}, {"myword": "xxx"}})
	assert.NoError(t, err)
	assert.Equal(t, []Data{{"myword": "xxx"}}, datas)
	assert.Equal(t, transforms.StageAfterParser, filter.Stage())
}
