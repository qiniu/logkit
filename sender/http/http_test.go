package http

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	"github.com/qiniu/logkit/reader/http"
	"github.com/qiniu/logkit/sender"
	. "github.com/qiniu/logkit/utils/models"
)

func TestHTTPSender(t *testing.T) {
	readConf := conf.MapConf{
		reader.KeyMetaPath: "./meta",
		reader.KeyFileDone: "./meta",
		reader.KeyMode:     reader.ModeHTTP,
		KeyRunnerName:      "TestNewHttpReader",
	}
	meta, err := reader.NewMetaWithConf(readConf)
	assert.NoError(t, err)
	defer os.RemoveAll("./meta")

	c := conf.MapConf{
		reader.KeyHTTPServiceAddress: "127.0.0.1:8000",
		reader.KeyHTTPServicePath:    "/logkit/data",
	}
	reader, err := http.NewReader(meta, c)
	httpReader := reader.(*http.Reader)
	assert.NoError(t, err)
	assert.NoError(t, httpReader.Start())
	defer httpReader.Close()

	// CI 环境启动监听较慢，需要等待几秒
	time.Sleep(3 * time.Second)

	testData := []struct {
		input       []Data
		jsonExp     [][]string
		csvExp      []map[string]string
		bodyJSONExp string
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
			bodyJSONExp: `[{"a":1,"b":true,"c":"1","e":1.43,"d":{"a1":1,"b1":true,"c1":"1","d1":{}}},{"b":true,"c":"1","d":{"b1":true,"c1":"1","d1":{},"a1":1},"a":1}]`,
		},
	}

	// gzip = true, protocol = json
	senderConf := conf.MapConf{
		sender.KeyHttpSenderGzip:     "true",
		sender.KeyHttpSenderCsvSplit: "\t",
		sender.KeyHttpSenderProtocol: "json",
		sender.KeyHttpSenderCsvHead:  "false",
		KeyRunnerName:                "testRunner",
		sender.KeyHttpSenderUrl:      "http://127.0.0.1:8000/logkit/data",
	}
	httpSender, err := NewSender(senderConf)
	assert.NoError(t, err)

	var wg sync.WaitGroup
	for _, val := range testData {
		wg.Add(1)
		go func(httpReader *http.Reader, val struct {
			input       []Data
			jsonExp     [][]string
			csvExp      []map[string]string
			bodyJSONExp string
		}, t *testing.T) {
			for _, exp := range val.jsonExp {
				got, err := httpReader.ReadLine()
				assert.NoError(t, err)
				for _, e := range exp {
					if !strings.Contains(got, e) {
						t.Fatalf("exp: %v contains %v, but not", got, e)
					}
				}
			}
			wg.Done()
		}(httpReader, val, t)
		httpSender.Send(val.input)
	}
	wg.Wait()

	// gzip = false, protocol = json
	senderConf = conf.MapConf{
		sender.KeyHttpSenderGzip:     "false",
		sender.KeyHttpSenderCsvSplit: "\t",
		sender.KeyHttpSenderProtocol: "json",
		sender.KeyHttpSenderCsvHead:  "false",
		KeyRunnerName:                "testRunner",
		sender.KeyHttpSenderUrl:      "127.0.0.1:8000/logkit/data",
	}
	httpSender, err = NewSender(senderConf)
	assert.NoError(t, err)

	for _, val := range testData {
		wg.Add(1)
		go func(httpReader *http.Reader, val struct {
			input       []Data
			jsonExp     [][]string
			csvExp      []map[string]string
			bodyJSONExp string
		}, t *testing.T) {
			for _, exp := range val.jsonExp {
				got, err := httpReader.ReadLine()
				assert.NoError(t, err)
				for _, e := range exp {
					if !strings.Contains(got, e) {
						t.Fatalf("exp: %v contains %v, but not", got, e)
					}
				}
			}
			wg.Done()
		}(httpReader, val, t)
		httpSender.Send(val.input)
	}
	wg.Wait()

	// gzip = true, protocol = csv, csvHead = true
	senderConf = conf.MapConf{
		sender.KeyHttpSenderGzip:     "true",
		sender.KeyHttpSenderCsvSplit: "\t",
		sender.KeyHttpSenderProtocol: "csv",
		sender.KeyHttpSenderCsvHead:  "true",
		KeyRunnerName:                "testRunner",
		sender.KeyHttpSenderUrl:      "http://127.0.0.1:8000/logkit/data",
	}
	httpSender, err = NewSender(senderConf)
	assert.NoError(t, err)

	for _, val := range testData {
		wg.Add(1)
		go func(httpReader *http.Reader, val struct {
			input       []Data
			jsonExp     [][]string
			csvExp      []map[string]string
			bodyJSONExp string
		}, t *testing.T) {
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
			wg.Done()
		}(httpReader, val, t)
		httpSender.Send(val.input)
	}
	wg.Wait()

	// gzip = false, protocol = csv, csvHead = true
	senderConf = conf.MapConf{
		sender.KeyHttpSenderGzip:     "false",
		sender.KeyHttpSenderCsvSplit: "\t",
		sender.KeyHttpSenderProtocol: "csv",
		sender.KeyHttpSenderCsvHead:  "true",
		KeyRunnerName:                "testRunner",
		sender.KeyHttpSenderUrl:      "http://127.0.0.1:8000/logkit/data",
	}
	httpSender, err = NewSender(senderConf)
	assert.NoError(t, err)

	for _, val := range testData {
		wg.Add(1)
		go func(httpReader *http.Reader, val struct {
			input       []Data
			jsonExp     [][]string
			csvExp      []map[string]string
			bodyJSONExp string
		}, t *testing.T) {
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
			wg.Done()
		}(httpReader, val, t)
		httpSender.Send(val.input)
	}
	wg.Wait()

	// gzip = true, protocol = csv, csvHead = false
	senderConf = conf.MapConf{
		sender.KeyHttpSenderGzip:     "true",
		sender.KeyHttpSenderCsvSplit: "\t",
		sender.KeyHttpSenderProtocol: "csv",
		sender.KeyHttpSenderCsvHead:  "false",
		KeyRunnerName:                "testRunner",
		sender.KeyHttpSenderUrl:      "127.0.0.1:8000/logkit/data",
	}
	httpSender, err = NewSender(senderConf)
	assert.NoError(t, err)

	for _, val := range testData {
		wg.Add(1)
		go func(httpReader *http.Reader, val struct {
			input       []Data
			jsonExp     [][]string
			csvExp      []map[string]string
			bodyJSONExp string
		}, t *testing.T) {
			for i := 0; i < len(val.input); i++ {
				_, err := httpReader.ReadLine()
				assert.NoError(t, err)
			}
			got, err := httpReader.ReadLine()
			assert.NoError(t, err)
			assert.Equal(t, "", got)
			wg.Done()
		}(httpReader, val, t)
		httpSender.Send(val.input)
	}
	wg.Wait()

	// gzip = true, protocol = body_json, csvHead = false
	senderConf = conf.MapConf{
		sender.KeyHttpSenderGzip:     "true",
		sender.KeyHttpSenderCsvSplit: "\t",
		sender.KeyHttpSenderProtocol: "body_json",
		sender.KeyHttpSenderCsvHead:  "false",
		KeyRunnerName:                "testRunner",
		sender.KeyHttpSenderUrl:      "127.0.0.1:8000/logkit/data",
	}
	httpSender, err = NewSender(senderConf)
	assert.NoError(t, err)

	for _, val := range testData {
		wg.Add(1)
		go func(httpReader *http.Reader, val struct {
			input       []Data
			jsonExp     [][]string
			csvExp      []map[string]string
			bodyJSONExp string
		}, t *testing.T) {
			got, err := httpReader.ReadLine()
			assert.NoError(t, err)
			var exps, datas []Data

			err = jsoniter.Unmarshal([]byte(got), &datas)
			if err != nil {
				t.Fatal(err)
			}
			err = jsoniter.Unmarshal([]byte(val.bodyJSONExp), &exps)
			if err != nil {
				t.Fatal(err)
			}
			assert.Equal(t, exps, datas)
			wg.Done()
		}(httpReader, val, t)
		err = httpSender.Send(val.input)
		if err != nil {
			t.Fatal(err)
		}
	}
	wg.Wait()
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
