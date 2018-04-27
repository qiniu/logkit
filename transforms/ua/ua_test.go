package ua

import (
	"testing"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/stretchr/testify/assert"
	"github.com/ua-parser/uap-go/uaparser"
)

func TestUaTransformer(t *testing.T) {
	ipt := &UATransformer{
		Key:       "ua",
		UA_Device: "true",
		UA_OS:     "true",
		UA_Agent:  "true",
		MemCache:  "true",
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

var div *uaparser.Device
var os *uaparser.Os
var cl *uaparser.Client
var ag *uaparser.UserAgent

func BenchmarkUAALL(b *testing.B) {
	uaparser.NewFromSaved()
	us := uaparser.NewFromSaved()
	for i := 0; i < b.N; i++ {
		cl = us.Parse(`Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_3; en-us; Silk/1.1.0-80) AppleWebKit/533.16 (KHTML, like Gecko) Version/5.0 Safari/533.16 Silk-Accelerated=true`)
	}
	//fmt.Println(cl)
}

func BenchmarkUADevice(b *testing.B) {
	us := uaparser.NewFromSaved()
	for i := 0; i < b.N; i++ {
		div = us.ParseDevice(`Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_3; en-us; Silk/1.1.0-80) AppleWebKit/533.16 (KHTML, like Gecko) Version/5.0 Safari/533.16 Silk-Accelerated=true`)
	}
	//fmt.Println(div)
}

func BenchmarkUAOS(b *testing.B) {
	us := uaparser.NewFromSaved()
	for i := 0; i < b.N; i++ {
		os = us.ParseOs(`Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_3; en-us; Silk/1.1.0-80) AppleWebKit/533.16 (KHTML, like Gecko) Version/5.0 Safari/533.16 Silk-Accelerated=true`)
	}
	//fmt.Println(os)
}

func BenchmarkUAAgent(b *testing.B) {
	us := uaparser.NewFromSaved()
	for i := 0; i < b.N; i++ {
		ag = us.ParseUserAgent(`Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_3; en-us; Silk/1.1.0-80) AppleWebKit/533.16 (KHTML, like Gecko) Version/5.0 Safari/533.16 Silk-Accelerated=true`)
	}
	//fmt.Println(ag)
}
