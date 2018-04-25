package sender

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/stretchr/testify/assert"
)

func TestHttpSender(t *testing.T) {
	c := conf.MapConf{
		reader.KeyHttpServiceAddress: ":8000",
		reader.KeyHttpServicePath:    "/logkit/data",
	}
	readConf := conf.MapConf{
		reader.KeyMetaPath: "./meta",
		reader.KeyFileDone: "./meta",
		reader.KeyMode:     reader.ModeHttp,
		KeyRunnerName:      "TestNewHttpReader",
	}
	meta, err := reader.NewMetaWithConf(readConf)
	assert.NoError(t, err)
	defer os.RemoveAll("./meta")
	hhttpReader, err := reader.NewHttpReader(meta, c)
	httpReader := hhttpReader.(*reader.HttpReader)
	assert.NoError(t, err)
	err = httpReader.Start()
	assert.NoError(t, err)
	defer httpReader.Close()

	testData := []struct {
		input   []Data
		jsonExp [][]string
		csvExp  []map[string]string
	}{
		{
			input: []Data{
				{
					"a": 1,
					"b": true,
					"c": "1",
					"e": 1.43,
					"d": map[string]interface{}{
						"a1": 1,
						"b1": true,
						"c1": "1",
						"d1": map[string]interface{}{},
					},
				},
				{
					"a": 1,
					"b": true,
					"c": "1",
					"d": map[string]interface{}{
						"a1": 1,
						"b1": true,
						"c1": "1",
						"d1": map[string]interface{}{},
					},
				},
			},
			jsonExp: [][]string{
				{
					`"a":1`,
					`"b":true`,
					`"c":"1"`,
					`"e":1.43`,
					`"d":{"`,
					`"a1":1`,
					`"b1":true`,
					`"c1":"1"`,
					`"d1":{}`,
				},
				{
					`"a":1`,
					`"b":true`,
					`"c":"1"`,
					`"d":{"`,
					`"a1":1`,
					`"b1":true`,
					`"c1":"1"`,
					`"d1":{}`,
				},
			},
			csvExp: []map[string]string{
				{
					"a": "1",
					"b": "true",
					"c": "1",
					"d": `{"a1":1,"b1":true,"c1":"1","d1":{}}`,
					"e": "1.43",
				},
				{
					"a": "1",
					"b": "true",
					"c": "1",
					"d": `{"a1":1,"b1":true,"c1":"1","d1":{}}`,
					"e": "",
				},
			},
		},
	}

	// gzip = true, protocol = json
	senderConf := conf.MapConf{
		KeyHttpSenderGzip:     "true",
		KeyHttpSenderCsvSplit: "\t",
		KeyHttpSenderProtocol: "json",
		KeyHttpSenderCsvHead:  "false",
		KeyRunnerName:         "testRunner",
		KeyHttpSenderUrl:      "http://127.0.0.1:8000/logkit/data",
	}
	httpSender, err := NewHttpSender(senderConf)
	assert.NoError(t, err)

	for _, val := range testData {
		httpSender.Send(val.input)
		for _, exp := range val.jsonExp {
			got, err := httpReader.ReadLine()
			assert.NoError(t, err)
			for _, e := range exp {
				if !strings.Contains(got, e) {
					t.Fatalf("exp: %v contains %v, but not", got, e)
				}
			}
		}
	}

	// gzip = false, protocol = json
	senderConf = conf.MapConf{
		KeyHttpSenderGzip:     "false",
		KeyHttpSenderCsvSplit: "\t",
		KeyHttpSenderProtocol: "json",
		KeyHttpSenderCsvHead:  "false",
		KeyRunnerName:         "testRunner",
		KeyHttpSenderUrl:      "127.0.0.1:8000/logkit/data",
	}
	httpSender, err = NewHttpSender(senderConf)
	assert.NoError(t, err)

	for _, val := range testData {
		httpSender.Send(val.input)
		for _, exp := range val.jsonExp {
			got, err := httpReader.ReadLine()
			assert.NoError(t, err)
			for _, e := range exp {
				if !strings.Contains(got, e) {
					t.Fatalf("exp: %v contains %v, but not", got, e)
				}
			}
		}
	}

	// gzip = true, protocol = csv, csvHead = true
	senderConf = conf.MapConf{
		KeyHttpSenderGzip:     "true",
		KeyHttpSenderCsvSplit: "\t",
		KeyHttpSenderProtocol: "csv",
		KeyHttpSenderCsvHead:  "true",
		KeyRunnerName:         "testRunner",
		KeyHttpSenderUrl:      "http://127.0.0.1:8000/logkit/data",
	}
	httpSender, err = NewHttpSender(senderConf)
	assert.NoError(t, err)

	for _, val := range testData {
		httpSender.Send(val.input)
		head, err := httpReader.ReadLine()
		assert.NoError(t, err)
		headArray := strings.Split(head, "\t")
		for _, expMap := range val.csvExp {
			gotStr, err := httpReader.ReadLine()
			assert.NoError(t, err)
			gotArray := strings.Split(gotStr, "\t")
			assert.Equal(t, len(expMap), len(gotArray))
			assert.Equal(t, len(expMap), len(headArray))
			for i, head := range headArray {
				exp, ok := expMap[head]
				assert.Equal(t, true, ok)
				if head == "d" && len(exp) != len(gotArray[i]) {
					t.Fatalf("error: exp %v, but got %v", exp, gotArray[i])
				} else {
					assert.Equal(t, exp, gotArray[i])
				}
			}
		}
	}

	// gzip = false, protocol = csv, csvHead = true
	senderConf = conf.MapConf{
		KeyHttpSenderGzip:     "false",
		KeyHttpSenderCsvSplit: "\t",
		KeyHttpSenderProtocol: "csv",
		KeyHttpSenderCsvHead:  "true",
		KeyRunnerName:         "testRunner",
		KeyHttpSenderUrl:      "http://127.0.0.1:8000/logkit/data",
	}
	httpSender, err = NewHttpSender(senderConf)
	assert.NoError(t, err)

	for _, val := range testData {
		httpSender.Send(val.input)
		head, err := httpReader.ReadLine()
		assert.NoError(t, err)
		headArray := strings.Split(head, "\t")
		for _, expMap := range val.csvExp {
			gotStr, err := httpReader.ReadLine()
			assert.NoError(t, err)
			gotArray := strings.Split(gotStr, "\t")
			assert.Equal(t, len(expMap), len(gotArray))
			assert.Equal(t, len(expMap), len(headArray))
			for i, head := range headArray {
				exp, ok := expMap[head]
				assert.Equal(t, true, ok)
				if head == "d" && len(exp) != len(gotArray[i]) {
					t.Fatalf("error: exp %v, but got %v", exp, gotArray[i])
				} else {
					assert.Equal(t, exp, gotArray[i])
				}
			}
		}
	}

	// gzip = true, protocol = csv, csvHead = false
	senderConf = conf.MapConf{
		KeyHttpSenderGzip:     "true",
		KeyHttpSenderCsvSplit: "\t",
		KeyHttpSenderProtocol: "csv",
		KeyHttpSenderCsvHead:  "false",
		KeyRunnerName:         "testRunner",
		KeyHttpSenderUrl:      "127.0.0.1:8000/logkit/data",
	}
	httpSender, err = NewHttpSender(senderConf)
	assert.NoError(t, err)

	for _, val := range testData {
		httpSender.Send(val.input)
		for i := 0; i < len(val.csvExp); i++ {
			_, err := httpReader.ReadLine()
			assert.NoError(t, err)
		}
		got, err := httpReader.ReadLine()
		assert.NoError(t, err)
		assert.Equal(t, "", got)
	}
}

func TestGzipData(t *testing.T) {
	testData := []string{
		`kjhgfdsdfghjkjhgfdfghjk`,
		`234567890-[poiuytfrdghjkl;`,
		`{"a":"a","b":"b"}`,
		`["a", "b", "c", "d"]`,
		`\werj23\3r\3435\wrw\r345`,
	}
	for _, val := range testData {
		gotByte, err := gzipData([]byte(val))
		assert.NoError(t, err)
		bytes.NewReader(gotByte)
		tmpData, err := gzip.NewReader(bytes.NewReader(gotByte))
		assert.NoError(t, err)
		tmpByte, err := ioutil.ReadAll(tmpData)
		assert.NoError(t, err)
		assert.Equal(t, val, string(tmpByte))
	}
}
