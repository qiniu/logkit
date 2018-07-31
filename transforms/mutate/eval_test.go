package mutate

import (
	"testing"
	"github.com/qiniu/logkit/utils/models"
	"github.com/stretchr/testify/assert"
)

func TestEval_Transform(t *testing.T) {
	eval := Eval{
		Key: "c=${a}+${b},s.s3=${s1}+${s2}",
	}

	datas := []models.Data{
		models.Data{"a": 123, "b": 234, "s1": "s11111", "s2": "s222222"},
		models.Data{"a": 2222, "b": 1111, "s1": "22222", "s2": "33333"},
	}

	dest, err := eval.Transform(datas)
	assert.NoError(t, err)
	assert.Equal(t, 357, dest[0]["c"])
	assert.Equal(t, 3333, dest[1]["c"])
	assert.Equal(t, map[string]interface{}{"s3": "s11111s222222"}, dest[0]["s"])
	assert.Equal(t, map[string]interface{}{"s3": "2222233333"}, dest[1]["s"])


	eval = Eval{
		Key: "c=${a},s.s3=${s1}",
	}
	datas = []models.Data{
		models.Data{"a": 123, "b": 234, "s1": "s11111", "s2": "s222222"},
		models.Data{"a": 2222, "b": 1111, "s1": "22222", "s2": "33333"},
	}

	dest, err = eval.Transform(datas)
	assert.NoError(t, err)
	assert.Equal(t, 123, dest[0]["c"])
	assert.Equal(t, 2222, dest[1]["c"])
	assert.Equal(t, map[string]interface{}{"s3": "s11111"}, dest[0]["s"])
	assert.Equal(t, map[string]interface{}{"s3": "22222"}, dest[1]["s"])

}
