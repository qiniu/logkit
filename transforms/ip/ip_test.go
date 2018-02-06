package ip

import (
	"testing"

	"github.com/qiniu/logkit/transforms"
	"github.com/qiniu/logkit/utils"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/stretchr/testify/assert"
)

func TestIpTransformer(t *testing.T) {
	ipt := &IpTransformer{
		Key:      "ip",
		DataPath: "./17monipdb.dat",
	}
	data, err := ipt.Transform([]Data{{"ip": "111.2.3.4"}, {"ip": "x.x.x.x"}})
	assert.Error(t, err)
	exp := []Data{{
		"ip":      "111.2.3.4",
		"Region":  "浙江",
		"City":    "宁波",
		"Country": "中国",
		"Isp":     "N/A"},
		{
			"ip": "x.x.x.x",
		}}
	assert.Equal(t, exp, data)
	expe := utils.StatsInfo{
		Errors:    1,
		Success:   1,
		LastError: "invalid ip format",
	}
	assert.Equal(t, expe, ipt.stats)
	assert.Equal(t, ipt.Stage(), transforms.StageAfterParser)

	ipt2 := &IpTransformer{
		Key:      "multi.ip",
		DataPath: "./17monipdb.dat",
	}
	data2, err2 := ipt2.Transform([]Data{{"multi": map[string]interface{}{"ip": "111.2.3.4"}}, {"multi": map[string]interface{}{"ip": "x.x.x.x"}}})
	assert.Error(t, err2)
	exp2 := []Data{{
		"multi": map[string]interface{}{
			"ip":      "111.2.3.4",
			"Region":  "浙江",
			"City":    "宁波",
			"Country": "中国",
			"Isp":     "N/A"},
	},
		{"multi": map[string]interface{}{
			"ip": "x.x.x.x",
		},
		},
	}
	assert.Equal(t, exp2, data2)
	expe2 := utils.StatsInfo{
		Errors:    1,
		Success:   1,
		LastError: "invalid ip format",
	}
	assert.Equal(t, expe2, ipt.stats)
	assert.Equal(t, ipt2.Stage(), transforms.StageAfterParser)
}
