package ua

import (
	"strconv"
	"strings"
	"testing"

	"github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"
	"github.com/ua-parser/uap-go/uaparser"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	bench     []Data
	testDatas = getUATestData(Data{"ua": "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_3; en-us; Silk/1.1.0-80) AppleWebKit/533.16 (KHTML, like Gecko) Version/5.0 Safari/533.1 Silk-Accelerated=true"}, 2*MB)
)

func TestUaTransformer(t *testing.T) {
	ipt := &UATransformer{
		Key:       "ua",
		UA_Device: "true",
		UA_OS:     "true",
		UA_Agent:  "true",
		MemCache:  "true",
	}
	MaxProcs = 2
	ipt.Init()
	data, err := ipt.Transform([]Data{
		{"ua": "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_1; en-us; Silk/1.1.0-80) AppleWebKit/533.16 (KHTML, like Gecko) Version/5.0 Safari/533.1 Silk-Accelerated=true"},
		{"ua": "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_2; en-us; Silk/1.1.0-80) AppleWebKit/533.16 (KHTML, like Gecko) Version/5.0 Safari/533.2 Silk-Accelerated=true"},
		{"ua": "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_3; en-us; Silk/1.1.0-80) AppleWebKit/533.16 (KHTML, like Gecko) Version/5.0 Safari/533.3 Silk-Accelerated=true"},
		{"ua": "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_4; en-us; Silk/1.1.0-80) AppleWebKit/533.16 (KHTML, like Gecko) Version/5.0 Safari/533.4 Silk-Accelerated=true"},
	})
	assert.NoError(t, err)
	exp := []Data{
		{
			"ua":               "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_1; en-us; Silk/1.1.0-80) AppleWebKit/533.16 (KHTML, like Gecko) Version/5.0 Safari/533.1 Silk-Accelerated=true",
			"UA_Family":        "Amazon Silk",
			"UA_Major":         "1",
			"UA_Minor":         "1",
			"UA_Patch":         "0-80",
			"UA_OS_Family":     "Android",
			"UA_Device_Family": "Kindle",
			"UA_Device_Brand":  "Amazon",
			"UA_Device_Model":  "Kindle",
		},
		{
			"ua":               "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_2; en-us; Silk/1.1.0-80) AppleWebKit/533.16 (KHTML, like Gecko) Version/5.0 Safari/533.2 Silk-Accelerated=true",
			"UA_Family":        "Amazon Silk",
			"UA_Major":         "1",
			"UA_Minor":         "1",
			"UA_Patch":         "0-80",
			"UA_OS_Family":     "Android",
			"UA_Device_Family": "Kindle",
			"UA_Device_Brand":  "Amazon",
			"UA_Device_Model":  "Kindle",
		},
		{
			"ua":               "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_3; en-us; Silk/1.1.0-80) AppleWebKit/533.16 (KHTML, like Gecko) Version/5.0 Safari/533.3 Silk-Accelerated=true",
			"UA_Family":        "Amazon Silk",
			"UA_Major":         "1",
			"UA_Minor":         "1",
			"UA_Patch":         "0-80",
			"UA_OS_Family":     "Android",
			"UA_Device_Family": "Kindle",
			"UA_Device_Brand":  "Amazon",
			"UA_Device_Model":  "Kindle",
		},
		{
			"ua":               "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_4; en-us; Silk/1.1.0-80) AppleWebKit/533.16 (KHTML, like Gecko) Version/5.0 Safari/533.4 Silk-Accelerated=true",
			"UA_Family":        "Amazon Silk",
			"UA_Major":         "1",
			"UA_Minor":         "1",
			"UA_Patch":         "0-80",
			"UA_OS_Family":     "Android",
			"UA_Device_Family": "Kindle",
			"UA_Device_Brand":  "Amazon",
			"UA_Device_Model":  "Kindle",
		},
	}
	assert.Equal(t, len(exp), len(data))
	assert.EqualValues(t, exp, data)
	assert.Equal(t, ipt.Stage(), transforms.StageAfterParser)
}

func Test_getParsedData(t *testing.T) {
	ipt := &UATransformer{
		Key:       "ua",
		UA_Device: "true",
		UA_OS:     "true",
		UA_Agent:  "true",
		MemCache:  "true",
	}
	ipt.Init()
	userAgent, os, device := ipt.getParsedData("Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_3; en-us; Silk/1.1.0-80) AppleWebKit/533.16 (KHTML, like Gecko) Version/5.0 Safari/533.16 Silk-Accelerated=true")
	assert.NotNil(t, userAgent)
	assert.NotNil(t, os)
	assert.NotNil(t, device)

	assert.EqualValues(t, uaparser.UserAgent{
		Family: "Amazon Silk",
		Major:  "1",
		Minor:  "1",
		Patch:  "0-80",
	}, *userAgent)
	assert.EqualValues(t, uaparser.Device{
		Family: "Kindle",
		Brand:  "Amazon",
		Model:  "Kindle",
	}, *device)
	assert.EqualValues(t, uaparser.Os{
		Family: "Android",
	}, *os)
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

// old: 1	11556900597 ns/op routine = 1  (2MB no cache)
// now: 1	12901170071 ns/op routine = 1  (2MB no cache)
// now: 1	8138148657 ns/op routine = 2  (2MB no cache)
// old: 1	12638076407 ns/op routine = 1  (2MB cache)
// now: 1	11764297398 ns/op routine = 1  (2MB cache)
// now: 1	8476822464 ns/op routine = 2  (2MB cache)
func Benchmark_Transform(b *testing.B) {
	ipt := &UATransformer{
		Key:       "ua",
		UA_Device: "true",
		UA_OS:     "true",
		UA_Agent:  "true",
		MemCache:  "true",
	}
	MaxProcs = 2
	ipt.Init()
	var datas []Data
	for n := 0; n < b.N; n++ {
		datas, _ = ipt.Transform(testDatas)
	}
	bench = datas
}

// 获取测试数据
func getUATestData(data Data, size int) []Data {
	testSlice := make([]Data, 0)
	totalSize := 0
	index := 1
	for {
		if totalSize > size {
			return testSlice
		}
		var newData = make(Data)
		for key, value := range data {
			valueStr, _ := value.(string)
			value = strings.Replace(valueStr, "Safari/533.", "Safari/533."+strconv.Itoa(index), -1)
			newData[key] = value
		}
		testSlice = append(testSlice, newData)
		dataBytes, _ := jsoniter.Marshal(newData)
		totalSize += len(dataBytes)
		index++
	}
	return testSlice
}
