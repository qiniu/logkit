package ip

import (
	"testing"

	. "github.com/qiniu/logkit/utils/models"

	"github.com/stretchr/testify/assert"
)

func TestInt2IpTransformer(t *testing.T) {
	jsonConf := &Number2Ip{
		Key: "my_int",
		New: "my_ip",
	}
	data := []Data{{"my_int": 223556667}}
	res, err := jsonConf.Transform(data)
	assert.NoError(t, err)
	exp := []Data{{"my_int": 223556667, "my_ip": "13.83.52.59"}}
	assert.Equal(t, exp, res)

	data2 := []Data{{"my_int": int64(223556667)}}
	res2, err2 := jsonConf.Transform(data2)
	assert.NoError(t, err2)
	exp2 := []Data{{"my_int": int64(223556667), "my_ip": "13.83.52.59"}}
	assert.Equal(t, exp2, res2)

	data3 := []Data{{"my_int": int32(223556667)}}
	res3, err3 := jsonConf.Transform(data3)
	assert.NoError(t, err3)
	exp3 := []Data{{"my_int": int32(223556667), "my_ip": "13.83.52.59"}}
	assert.Equal(t, exp3, res3)

	data4 := []Data{{"my_int": "223556667"}}
	res4, err4 := jsonConf.Transform(data4)
	assert.NoError(t, err4)
	exp4 := []Data{{"my_int": "223556667", "my_ip": "13.83.52.59"}}
	assert.Equal(t, exp4, res4)

	data5 := []Data{{"my_int": uint32(223556667)}}
	res5, err5 := jsonConf.Transform(data5)
	assert.NoError(t, err5)
	exp5 := []Data{{"my_int": uint32(223556667), "my_ip": "13.83.52.59"}}
	assert.Equal(t, exp5, res5)
}

func Test_convertIp(t *testing.T) {
	ipInt1 := int64(223556667)
	ip1 := convertIp(ipInt1)
	exp1 := "13.83.52.59"
	assert.Equal(t, exp1, ip1)

	ipInt2 := int64(1653276013)
	ip2 := convertIp(ipInt2)
	exp2 := "98.138.253.109"
	assert.Equal(t, exp2, ip2)

	ipInt3 := int64(165327601)
	ip3 := convertIp(ipInt3)
	exp3 := "9.218.178.241"
	assert.Equal(t, exp3, ip3)
}
