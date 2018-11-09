package mutate

import (
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/qiniu/logkit/utils/models"
)

func TestTransform(t *testing.T) {
	c1 := &Case{
		Key:  "raw",
		Mode: ModeLower,
	}
	c2 := &Case{
		Key:  "raw",
		Mode: ModeUpper,
	}
	cases := []struct {
		log []Data
		exp []Data
	}{
		{
			log: []Data{
				{
					"raw": "logkit!!!,LOGKIT!!!",
				},
			},
			exp: []Data{
				{
					"raw": "logkit!!!,logkit!!!",
				},
			},
		},
		{
			log: []Data{
				{
					"raw": "logkit!!!,LOGKIT!!!",
				},
			},
			exp: []Data{
				{
					"raw": "LOGKIT!!!,LOGKIT!!!",
				},
			},
		},
	}
	newVal, err := c1.Transform(cases[0].log)
	assert.NoError(t, err)
	assert.Equal(t, cases[0].exp, newVal)
	newVal, err = c2.Transform(cases[1].log)
	assert.NoError(t, err)
	assert.Equal(t, cases[1].exp, newVal)
}
