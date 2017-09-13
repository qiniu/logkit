package ip

import (
	"testing"

	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/utils"
	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/transforms"
)

func TestIpTransformer(t *testing.T) {
	ipt := &IpTransformer{
		Key:      "ip",
		DataPath: "./17monipdb.dat",
	}
	data, err := ipt.Transform([]sender.Data{{"ip": "111.2.3.4"}, {"ip": "x.x.x.x"}})
	assert.Error(t, err)
	exp := []sender.Data{{
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
}
