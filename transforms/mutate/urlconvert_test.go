package mutate

import (
	"testing"

	"github.com/stretchr/testify/assert"

	. "github.com/qiniu/logkit/utils/models"
)

func TestURLConvert_Transform(t *testing.T) {
	tests := []struct {
		log  []Data
		mode string
		exp  []Data
	}{
		{
			log: []Data{
				{
					"raw": "http://qiniu.com/?a=fdafds",
				},
			},
			mode: ModeEncode,
			exp: []Data{
				{
					"raw": "http%3A%2F%2Fqiniu.com%2F%3Fa%3Dfdafds",
				},
			},
		},
		{
			log: []Data{
				{
					"raw": "qiniu.com",
				},
			},
			mode: ModeEncode,
			exp: []Data{
				{
					"raw": "qiniu.com",
				},
			},
		},
		{
			log: []Data{
				{
					"raw": "http%3A%2F%2Fqiniu.com%2F%3Fa%3Dfdafds",
				},
			},
			mode: ModeDecode,
			exp: []Data{
				{
					"raw": "http://qiniu.com/?a=fdafds",
				},
			},
		},
		{
			log: []Data{
				{
					"raw": "qiniu.com",
				},
			},
			mode: ModeDecode,
			exp: []Data{
				{
					"raw": "qiniu.com",
				},
			},
		},
	}

	urlConvert := &URLConvert{
		Key: "raw",
	}

	for _, test := range tests {
		urlConvert.Mode = test.mode
		acutal, err := urlConvert.Transform(test.log)
		assert.Nil(t, err)
		assert.EqualValues(t, len(test.exp), len(acutal))
		assert.EqualValues(t, test.exp, acutal)
	}

}

func TestURLConvert_RawTransform(t *testing.T) {
	tests := []struct {
		log  []string
		mode string
		exp  []string
	}{
		{
			log:  []string{"http://qiniu.com/?a=fdafds"},
			mode: ModeEncode,
			exp:  []string{"http%3A%2F%2Fqiniu.com%2F%3Fa%3Dfdafds"},
		},
		{
			log:  []string{"qiniu.com"},
			mode: ModeEncode,
			exp:  []string{"qiniu.com"},
		},
		{
			log:  []string{"http%3A%2F%2Fqiniu.com%2F%3Fa%3Dfdafds"},
			mode: ModeDecode,
			exp:  []string{"http://qiniu.com/?a=fdafds"},
		},
		{
			log:  []string{"qiniu.com"},
			mode: ModeDecode,
			exp:  []string{"qiniu.com"},
		},
	}
	urlConvert := &URLConvert{}

	for _, test := range tests {
		urlConvert.Mode = test.mode
		acutal, err := urlConvert.RawTransform(test.log)
		assert.Nil(t, err)
		assert.EqualValues(t, len(test.exp), len(acutal))
		assert.EqualValues(t, test.exp, acutal)
	}
}
