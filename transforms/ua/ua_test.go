package ua

import (
	"testing"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/stretchr/testify/assert"
)

func TestUaTransformer(t *testing.T) {
	ipt := &UATransformer{
		Key: "ua",
	}
	ipt.Init()
	data, err := ipt.Transform([]Data{{"ua": "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_3; en-us; Silk/1.1.0-80) AppleWebKit/533.16 (KHTML, like Gecko) Version/5.0 Safari/533.16 Silk-Accelerated=true"}})
	assert.NoError(t, err)
	exp := []Data{{
		"ua":               "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_3; en-us; Silk/1.1.0-80) AppleWebKit/533.16 (KHTML, like Gecko) Version/5.0 Safari/533.16 Silk-Accelerated=true",
		"UA_Family":        "Amazon Silk",
		"UA_Major":         "1",
		"UA_Minor":         "1",
		"UA_Patch":         "0-80",
		"UA_OS_Family":     "Android",
		"UA_Device_Family": "Kindle",
		"UA_Device_Brand":  "Amazon",
		"UA_Device_Model":  "Kindle"},
	}
	assert.Equal(t, exp, data)
	assert.Equal(t, ipt.Stage(), transforms.StageAfterParser)

}
