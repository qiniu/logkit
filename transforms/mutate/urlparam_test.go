package mutate

import (
	"fmt"
	"testing"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/stretchr/testify/assert"
)

func TestParamTransformer(t *testing.T) {
	par := &UrlParam{
		Key: "myword",
	}
	data, err := par.Transform([]Data{
		{"myword": "?platform=2&vid=372&vu=caea966558&chan=android_sougou&sign=ad225ec02942c79bdb710e3ad0cf1b43&nonce_str=1510555032"},
		{"myword": "platform=2&vid=&vu=caea966558&chan=&sign=ad225ec02942c79bdb710e3ad0cf1b43&nonce_str=1510555032"},
		{"myword": "/index/mytest?platform=2&vid=&vu=caea966558&chan=&sign=ad225ec02942c79bdb710e3ad0cf1b43&nonce_str=1510555032"},
		{"myword": "/index/mytest1"},
		{"myword": "http://10.100.0.1/index/mytest"},
	})
	assert.NoError(t, err)
	exp := []Data{
		{
			"myword":           "?platform=2&vid=372&vu=caea966558&chan=android_sougou&sign=ad225ec02942c79bdb710e3ad0cf1b43&nonce_str=1510555032",
			"myword_platform":  "2",
			"myword_vid":       "372",
			"myword_vu":        "caea966558",
			"myword_chan":      "android_sougou",
			"myword_sign":      "ad225ec02942c79bdb710e3ad0cf1b43",
			"myword_nonce_str": "1510555032",
		},
		{
			"myword":           "platform=2&vid=&vu=caea966558&chan=&sign=ad225ec02942c79bdb710e3ad0cf1b43&nonce_str=1510555032",
			"myword_platform":  "2",
			"myword_vu":        "caea966558",
			"myword_sign":      "ad225ec02942c79bdb710e3ad0cf1b43",
			"myword_nonce_str": "1510555032",
		},
		{
			"myword":                "/index/mytest?platform=2&vid=&vu=caea966558&chan=&sign=ad225ec02942c79bdb710e3ad0cf1b43&nonce_str=1510555032",
			"myword_platform":       "2",
			"myword_vu":             "caea966558",
			"myword_sign":           "ad225ec02942c79bdb710e3ad0cf1b43",
			"myword_nonce_str":      "1510555032",
			"myword_url_param_path": "/index/mytest",
		},
		{
			"myword":                "/index/mytest1",
			"myword_url_param_path": "/index/mytest1",
		},
		{
			"myword":                "http://10.100.0.1/index/mytest",
			"myword_url_param_path": "/index/mytest",
			"myword_url_param_host": "10.100.0.1",
		},
	}
	assert.Equal(t, len(exp), len(data))
	for i, ex := range exp {
		da := data[i]
		assert.Equal(t, ex, da)
	}
	assert.Equal(t, par.Stage(), transforms.StageAfterParser)
	assert.Equal(t, StatsInfo{Success: 5}, par.stats)
}

func TestParamTransformerSelectKeys(t *testing.T) {
	par := &UrlParam{
		Key:        "myword",
		SelectKeys: "platform,vid",
	}
	assert.Nil(t, par.Init())
	data, err := par.Transform([]Data{
		{"myword": "?platform=2&vid=372&vu=caea966558&chan=android_sougou&sign=ad225ec02942c79bdb710e3ad0cf1b43&nonce_str=1510555032"},
		{"myword2": ""},
		{"myword": "platform=2&vid=&vu=caea966558&chan=&sign=ad225ec02942c79bdb710e3ad0cf1b43&nonce_str=1510555032"},
		{"myword": "/index/mytest?platform=2&vid=&vu=caea966558&chan=&sign=ad225ec02942c79bdb710e3ad0cf1b43&nonce_str=1510555032"},
		{"myword": "/index/mytest1"},
		{"myword": "http://10.100.0.1/index/mytest"},
	})
	assert.Error(t, err)
	expectError := "find total 1 erorrs in transform urlparam, last error info is transform key myword not exist in data"
	assert.Equal(t, expectError, err.Error())
	exp := []Data{
		{
			"myword":          "?platform=2&vid=372&vu=caea966558&chan=android_sougou&sign=ad225ec02942c79bdb710e3ad0cf1b43&nonce_str=1510555032",
			"myword_platform": "2",
			"myword_vid":      "372",
		},
		{
			"myword2": "",
		},
		{
			"myword":          "platform=2&vid=&vu=caea966558&chan=&sign=ad225ec02942c79bdb710e3ad0cf1b43&nonce_str=1510555032",
			"myword_platform": "2",
		},
		{
			"myword":                "/index/mytest?platform=2&vid=&vu=caea966558&chan=&sign=ad225ec02942c79bdb710e3ad0cf1b43&nonce_str=1510555032",
			"myword_platform":       "2",
			"myword_url_param_path": "/index/mytest",
		},
		{
			"myword":                "/index/mytest1",
			"myword_url_param_path": "/index/mytest1",
		},
		{
			"myword":                "http://10.100.0.1/index/mytest",
			"myword_url_param_path": "/index/mytest",
			"myword_url_param_host": "10.100.0.1",
		},
	}
	assert.Equal(t, len(exp), len(data))
	for i, ex := range exp {
		da := data[i]
		assert.Equal(t, ex, da)
	}
	assert.Equal(t, par.Stage(), transforms.StageAfterParser)
	assert.Equal(t, StatsInfo{Success: 5, Errors: 1, LastError: expectError}, par.stats)
}

func TestParamTransformerError(t *testing.T) {
	par := &UrlParam{
		Key: "myword",
	}
	data, err := par.Transform([]Data{
		{
			"myword": "platform=2=372&vu=caea966558&chan=android_sougou&sign=ad225ec02942c79bdb710e3ad0cf1b43&nonce_str=1510555032",
		},
		{
			"myword": "platform=2&vid&vu=caea966558&chan=&sign=ad225ec02942c79bdb710e3ad0cf1b43&nonce_str=1510555032",
		},
	})
	assert.NoError(t, err)
	exp := []Data{
		{"myword": "platform=2=372&vu=caea966558&chan=android_sougou&sign=ad225ec02942c79bdb710e3ad0cf1b43&nonce_str=1510555032",
			"myword_platform":  "2=372",
			"myword_vu":        "caea966558",
			"myword_chan":      "android_sougou",
			"myword_sign":      "ad225ec02942c79bdb710e3ad0cf1b43",
			"myword_nonce_str": "1510555032",
		},
		{
			"myword":           "platform=2&vid&vu=caea966558&chan=&sign=ad225ec02942c79bdb710e3ad0cf1b43&nonce_str=1510555032",
			"myword_platform":  "2",
			"myword_vu":        "caea966558",
			"myword_sign":      "ad225ec02942c79bdb710e3ad0cf1b43",
			"myword_nonce_str": "1510555032",
		},
	}
	assert.Equal(t, len(exp), len(data))
	for i, ex := range exp {
		da := data[i]
		for k, e := range ex {
			d, exist := da[k]
			assert.Equal(t, true, exist)
			assert.Equal(t, e, d)
		}
	}
}

func TestParamTransformerKeyRepeat(t *testing.T) {
	par := &UrlParam{
		Key: "myword",
	}
	data, err := par.Transform([]Data{
		{"myword": "a=a&a=b&a=c&a=d"},
		{
			"myword":   "a=a&a=b&b=c&b=d&b=e",
			"myword_a": "xx",
		},
		{
			"myword":    "a=x",
			"myword_a":  "a",
			"myword_a1": "b",
			"myword_a2": "c",
		},
		{
			"myword":    "a=x",
			"myword_a":  "a",
			"myword_a1": "b",
			"myword_a2": "c",
			"myword_a3": "c",
			"myword_a4": "c",
			"myword_a5": "c",
		},
	})
	assert.NoError(t, err)
	exp := []Data{
		{
			"myword":   "a=a&a=b&a=c&a=d",
			"myword_a": "a&b&c&d",
		},
		{
			"myword":    "a=a&a=b&b=c&b=d&b=e",
			"myword_a":  "xx",
			"myword_a1": "a&b",
			"myword_b":  "c&d&e",
		},
		{
			"myword":    "a=x",
			"myword_a":  "a",
			"myword_a1": "b",
			"myword_a2": "c",
			"myword_a3": "x",
		},
		{
			"myword":    "a=x",
			"myword_a":  "a",
			"myword_a1": "b",
			"myword_a2": "c",
			"myword_a3": "c",
			"myword_a4": "c",
			"myword_a5": "c",
		},
	}
	assert.Equal(t, len(exp), len(data))
	for i, ex := range exp {
		da := data[i]
		for k, e := range ex {
			d, exist := da[k]
			assert.Equal(t, true, exist)
			assert.Equal(t, e, d)
		}
	}
	assert.Equal(t, par.Stage(), transforms.StageAfterParser)
	par.stats.LastError = ""
	assert.Equal(t, StatsInfo{Success: 4}, par.stats)
}

func TestParamTransformerMultiKey(t *testing.T) {
	par := &UrlParam{
		Key: "multi.myword",
	}
	data, err := par.Transform([]Data{
		{"multi": map[string]interface{}{"myword": "platform=2&vid=372&vu=caea966558&chan=android_sougou&sign=ad225ec02942c79bdb710e3ad0cf1b43&nonce_str=1510555032"}},
		{"multi": map[string]interface{}{"myword": "platform=2&vid=&vu=caea966558&chan=&sign=ad225ec02942c79bdb710e3ad0cf1b43&nonce_str=1510555032"}},
	})
	assert.NoError(t, err)
	exp := []Data{
		{
			"multi": map[string]interface{}{
				"myword":           "platform=2&vid=372&vu=caea966558&chan=android_sougou&sign=ad225ec02942c79bdb710e3ad0cf1b43&nonce_str=1510555032",
				"myword_platform":  "2",
				"myword_vid":       "372",
				"myword_vu":        "caea966558",
				"myword_chan":      "android_sougou",
				"myword_sign":      "ad225ec02942c79bdb710e3ad0cf1b43",
				"myword_nonce_str": "1510555032",
			}},
		{
			"multi": map[string]interface{}{
				"myword":           "platform=2&vid=&vu=caea966558&chan=&sign=ad225ec02942c79bdb710e3ad0cf1b43&nonce_str=1510555032",
				"myword_platform":  "2",
				"myword_vu":        "caea966558",
				"myword_sign":      "ad225ec02942c79bdb710e3ad0cf1b43",
				"myword_nonce_str": "1510555032",
			}},
	}
	assert.Equal(t, len(exp), len(data))
	for i, ex := range exp {
		da := data[i]["multi"].(map[string]interface{})
		for k, e := range ex["multi"].(map[string]interface{}) {
			d, exist := da[k]
			assert.Equal(t, true, exist)
			assert.Equal(t, e, d, fmt.Sprintf("case %v %v", i, k))
		}
	}
	assert.Equal(t, par.Stage(), transforms.StageAfterParser)
	assert.Equal(t, StatsInfo{Success: 2}, par.stats)
}
