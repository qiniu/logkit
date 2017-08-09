package ip

import (
	"testing"

	"github.com/qiniu/logkit/sender"
	"github.com/stretchr/testify/assert"
)

func TestIpTransformer(t *testing.T) {
	ipt := &IpTransformer{
		Key:      "ip",
		DataPath: "./17monipdb.dat",
	}
	data, err := ipt.Transform([]sender.Data{{"ip": "111.2.3.4"}})
	assert.NoError(t, err)
	exp := []sender.Data{{
		"ip":      "111.2.3.4",
		"Region":  "浙江",
		"City":    "宁波",
		"Country": "中国",
		"Isp":     "N/A"}}
	assert.Equal(t, exp, data)
}
