package mutate

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

func TestConcatTransformer(t *testing.T) {
	t.Parallel()
	concat := &Concat{
		Key: "myword1,myword2",
		New: "myword",
	}
	data, err := concat.Transform([]Data{{"myword1": "20110218", "myword2": "190102"}, {"abc": "x1"}})
	assert.Error(t, err)
	exp := []Data{{"myword1": "20110218", "myword2": "190102", "myword": "20110218190102"}, {"abc": "x1"}}
	assert.Equal(t, exp, data)
	assert.Equal(t, concat.Stage(), transforms.StageAfterParser)

	concat = &Concat{
		Key: "myword1,myword2,myword3",
		New: "myword",
	}
	data, err = concat.Transform([]Data{{"myword1": "20110218", "myword2": "1901", "myword3": "02"}, {"abc": "x1"}})
	assert.Error(t, err)
	exp = []Data{{"myword1": "20110218", "myword2": "1901", "myword3": "02", "myword": "20110218190102"}, {"abc": "x1"}}
	assert.Equal(t, exp, data)
	assert.Equal(t, concat.Stage(), transforms.StageAfterParser)
}
