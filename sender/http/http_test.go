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
	readerConf "github.com/qiniu/logkit/reader/config"
	"github.com/qiniu/logkit/reader/http"
	. "github.com/qiniu/logkit/sender/config"
	. "github.com/qiniu/logkit/utils/models"
)

var templateExp = `{"keyA":{{a}},"keyC":"{{c}}"}`

var testData = []struct {
	input       []Data
	jsonExp     [][]string
	csvExp      []map[string]string
	bodyJSONExp string

	templateJSONExp     []string
	templateBodyJSONExp string
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
		bodyJSONExp:         `[{"a":1,"b":true,"c":"1","e":1.43,"d":{"a1":1,"b1":true,"c1":"1","d1":{}}},{"b":true,"c":"1","d":{"b1":true,"c1":"1","d1":{},"a1":1},"a":1}]`,
		templateJSONExp:     []string{`{"keyA":1,"keyC":"1"}`, `{"keyA":1,"keyC":"1"}`},
		templateBodyJSONExp: `[{"keyA":1,"keyC":"1"},{"keyA":1,"keyC":"1"}]`,
	},
}

func newHTTPReader(runnerName, port string) (*http.Reader, error) {
	meta, err := reader.NewMetaWithConf(conf.MapConf{
		readerConf.KeyMetaPath: "./meta/" + runnerName,
		readerConf.KeyFileDone: "./meta/" + runnerName,
		readerConf.KeyMode:     readerConf.ModeHTTP,
		KeyRunnerName:          runnerName,
	})
	if err != nil {
		return nil, err
	}

	reader, err := http.NewReader(meta, conf.MapConf{
		readerConf.KeyHTTPServiceAddress: "127.0.0.1:" + port,
		readerConf.KeyHTTPServicePath:    "/logkit/data",
	})
	httpReader := reader.(*http.Reader)
	if err != nil {
		return nil, err
	}
	return httpReader, httpReader.Start()
}

func TestHTTPSenderGzipWithJSON(t *testing.T) {
	t.Parallel()
	runnerName := "TestHTTPSenderGzipWithJSON"
	httpReader, err := newHTTPReader(runnerName, "8000")
	assert.NoError(t, err)
	defer func() {
		os.RemoveAll("./meta/" + runnerName)
		httpReader.Close()
	}()

	// CI 环境启动监听较慢，需要等待几秒
	time.Sleep(2 * time.Second)

	// gzip = true, protocol = json
	httpSender, err := NewSender(conf.MapConf{
		KeyHttpSenderGzip:     "true",
		KeyHttpSenderCsvSplit: "\t",
		KeyHttpSenderProtocol: "json",
		KeyHttpSenderCsvHead:  "false",
		KeyHttpSenderTemplate: "",
		KeyRunnerName:         runnerName,
		KeyHttpSenderUrl:      "http://127.0.0.1:8000/logkit/data",
	})
	assert.NoError(t, err)

	var wg sync.WaitGroup
	for _, val := range testData {
		wg.Add(1)
		go func(httpReader *http.Reader, val struct {
			input               []Data
			jsonExp             [][]string
			csvExp              []map[string]string
			bodyJSONExp         string
			templateJSONExp     []string
			templateBodyJSONExp string
		}, t *testing.T) {
			for _, exp := range val.jsonExp {
				got, err := httpReader.ReadLine()
				assert.NoError(t, err)
				for _, e := range exp {
					assert.Contains(t, got, e)
				}
			}
			wg.Done()
		}(httpReader, val, t)
		assert.NoError(t, httpSender.Send(val.input))
	}
	wg.Wait()
}

func TestHTTPSenderNoGzipWithJSON(t *testing.T) {
	t.Parallel()
	runnerName := "TestHTTPSenderNoGzipWithJSON"
	httpReader, err := newHTTPReader(runnerName, "8001")
	assert.NoError(t, err)
	defer func() {
		os.RemoveAll("./meta/" + runnerName)
		httpReader.Close()
	}()

	// CI 环境启动监听较慢，需要等待几秒
	time.Sleep(2 * time.Second)

	// gzip = false, protocol = json
	httpSender, err := NewSender(conf.MapConf{
		KeyHttpSenderGzip:     "false",
		KeyHttpSenderCsvSplit: "\t",
		KeyHttpSenderProtocol: "json",
		KeyHttpSenderCsvHead:  "false",
		KeyHttpSenderTemplate: "",
		KeyRunnerName:         runnerName,
		KeyHttpSenderUrl:      "127.0.0.1:8001/logkit/data",
	})
	assert.NoError(t, err)

	var wg sync.WaitGroup
	for _, val := range testData {
		wg.Add(1)
		go func(httpReader *http.Reader, val struct {
			input               []Data
			jsonExp             [][]string
			csvExp              []map[string]string
			bodyJSONExp         string
			templateJSONExp     []string
			templateBodyJSONExp string
		}, t *testing.T) {
			for _, exp := range val.jsonExp {
				got, err := httpReader.ReadLine()
				assert.NoError(t, err)
				for _, e := range exp {
					assert.Contains(t, got, e)
				}
			}
			wg.Done()
		}(httpReader, val, t)
		assert.NoError(t, httpSender.Send(val.input))
	}
	wg.Wait()
}

func TestHTTPSenderGzipAndCSVHeadWithCSV(t *testing.T) {
	t.Parallel()
	runnerName := "TestHTTPSenderGzipAndCSVHeadWithCSV"
	httpReader, err := newHTTPReader(runnerName, "8002")
	assert.NoError(t, err)
	defer func() {
		os.RemoveAll("./meta/" + runnerName)
		httpReader.Close()
	}()

	// CI 环境启动监听较慢，需要等待几秒
	time.Sleep(2 * time.Second)

	// gzip = true, protocol = csv, csvHead = true
	httpSender, err := NewSender(conf.MapConf{
		KeyHttpSenderGzip:     "true",
		KeyHttpSenderCsvSplit: "\t",
		KeyHttpSenderProtocol: "csv",
		KeyHttpSenderCsvHead:  "true",
		KeyHttpSenderTemplate: "",
		KeyRunnerName:         runnerName,
		KeyHttpSenderUrl:      "http://127.0.0.1:8002/logkit/data",
	})
	assert.NoError(t, err)

	var wg sync.WaitGroup
	for _, val := range testData {
		wg.Add(1)
		go func(httpReader *http.Reader, val struct {
			input               []Data
			jsonExp             [][]string
			csvExp              []map[string]string
			bodyJSONExp         string
			templateJSONExp     []string
			templateBodyJSONExp string
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
		assert.NoError(t, httpSender.Send(val.input))
	}
	wg.Wait()
}

func TestHTTPSenderNoGzipAndCSVHeadWithCSV(t *testing.T) {
	t.Parallel()
	runnerName := "TestHTTPSenderNoGzipAndCSVHeadWithCSV"
	httpReader, err := newHTTPReader(runnerName, "8003")
	assert.NoError(t, err)
	defer func() {
		os.RemoveAll("./meta/" + runnerName)
		httpReader.Close()
	}()

	// CI 环境启动监听较慢，需要等待几秒
	time.Sleep(2 * time.Second)

	// gzip = false, protocol = csv, csvHead = true
	httpSender, err := NewSender(conf.MapConf{
		KeyHttpSenderGzip:     "false",
		KeyHttpSenderCsvSplit: "\t",
		KeyHttpSenderProtocol: "csv",
		KeyHttpSenderCsvHead:  "true",
		KeyHttpSenderTemplate: "",
		KeyRunnerName:         runnerName,
		KeyHttpSenderUrl:      "http://127.0.0.1:8003/logkit/data",
	})
	assert.NoError(t, err)

	var wg sync.WaitGroup
	for _, val := range testData {
		wg.Add(1)
		go func(httpReader *http.Reader, val struct {
			input               []Data
			jsonExp             [][]string
			csvExp              []map[string]string
			bodyJSONExp         string
			templateJSONExp     []string
			templateBodyJSONExp string
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
		assert.NoError(t, httpSender.Send(val.input))
	}
	wg.Wait()
}

func TestHTTPSenderGzipAndNoCSVHeadWithCSV(t *testing.T) {
	t.Parallel()
	runnerName := "TestHTTPSenderGzipAndNoCSVHeadWithCSV"
	httpReader, err := newHTTPReader(runnerName, "8004")
	assert.NoError(t, err)
	defer func() {
		os.RemoveAll("./meta/" + runnerName)
		httpReader.Close()
	}()

	// CI 环境启动监听较慢，需要等待几秒
	time.Sleep(2 * time.Second)

	// gzip = true, protocol = csv, csvHead = false
	httpSender, err := NewSender(conf.MapConf{
		KeyHttpSenderGzip:     "true",
		KeyHttpSenderCsvSplit: "\t",
		KeyHttpSenderProtocol: "csv",
		KeyHttpSenderCsvHead:  "false",
		KeyHttpSenderTemplate: "",
		KeyRunnerName:         runnerName,
		KeyHttpSenderUrl:      "127.0.0.1:8004/logkit/data",
	})
	assert.NoError(t, err)

	var wg sync.WaitGroup
	for _, val := range testData {
		wg.Add(1)
		go func(httpReader *http.Reader, val struct {
			input               []Data
			jsonExp             [][]string
			csvExp              []map[string]string
			bodyJSONExp         string
			templateJSONExp     []string
			templateBodyJSONExp string
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
		assert.NoError(t, httpSender.Send(val.input))
	}
	wg.Wait()
}

func TestHTTPSenderGzipAndNoCSVHeadWithBodyJSON(t *testing.T) {
	t.Parallel()
	runnerName := "TestHTTPSenderGzipAndNoCSVHeadWithBodyJSON"
	httpReader, err := newHTTPReader(runnerName, "8005")
	assert.NoError(t, err)
	defer func() {
		os.RemoveAll("./meta/" + runnerName)
		httpReader.Close()
	}()

	// CI 环境启动监听较慢，需要等待几秒
	time.Sleep(2 * time.Second)

	// gzip = true, protocol = body_json, csvHead = false
	httpSender, err := NewSender(conf.MapConf{
		KeyHttpSenderGzip:     "true",
		KeyHttpSenderCsvSplit: "\t",
		KeyHttpSenderProtocol: "body_json",
		KeyHttpSenderCsvHead:  "false",
		KeyHttpSenderTemplate: "",
		KeyRunnerName:         runnerName,
		KeyHttpSenderUrl:      "127.0.0.1:8005/logkit/data",
	})
	assert.NoError(t, err)

	var wg sync.WaitGroup
	for _, val := range testData {
		wg.Add(1)
		go func(httpReader *http.Reader, val struct {
			input               []Data
			jsonExp             [][]string
			csvExp              []map[string]string
			bodyJSONExp         string
			templateJSONExp     []string
			templateBodyJSONExp string
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

func TestHTTPSenderNoGzipWithRaw(t *testing.T) {
	testData := []Data{
		{
			"raw": "aaaa",
		},
		{
			"raw": "If newbie have bad time,it's a bug on logkit!!!",
		},
	}
	t.Parallel()
	runnerName := "TestHTTPSenderNoGzipWithRaw"
	httpReader, err := newHTTPReader(runnerName, "8006")
	assert.NoError(t, err)
	defer func() {
		os.RemoveAll("./meta/" + runnerName)
		httpReader.Close()
	}()

	// CI 环境启动监听较慢，需要等待几秒
	time.Sleep(2 * time.Second)

	// gzip = false, protocol = json
	httpSender, err := NewSender(conf.MapConf{
		KeyHttpSenderGzip:     "false",
		KeyHttpSenderProtocol: "raw",
		KeyHttpSenderCsvHead:  "false",
		KeyHttpSenderTemplate: "",
		KeyRunnerName:         runnerName,
		KeyHttpSenderUrl:      "127.0.0.1:8006/logkit/data",
	})
	assert.NoError(t, err)

	var (
		res string
		wg  sync.WaitGroup
	)
	wg.Add(1)
	go func(httpReader *http.Reader, val []Data, t *testing.T) {
		for {
			got, err := httpReader.ReadLine()
			assert.NoError(t, err)
			if got == "" {
				break
			}
			res += got
		}
		wg.Done()
	}(httpReader, testData, t)
	assert.NoError(t, httpSender.Send(testData))
	wg.Wait()
	assert.Equal(t, "aaaaIf newbie have bad time,it's a bug on logkit!!!", res)
}

func TestHTTPSenderJSONWithTemplate(t *testing.T) {
	t.Parallel()
	runnerName := "TestHTTPSenderJSONWithTamplate"

	httpReader, err := newHTTPReader(runnerName, "8007")
	assert.NoError(t, err)
	defer func() {
		os.RemoveAll("./meta/" + runnerName)
		httpReader.Close()
	}()

	//	// CI 环境启动监听较慢，需要等待几秒
	time.Sleep(2 * time.Second)

	httpSender, err := NewSender(conf.MapConf{
		KeyHttpSenderGzip:     "false",
		KeyHttpSenderCsvSplit: "\t",
		KeyHttpSenderProtocol: "json",
		KeyHttpSenderCsvHead:  "false",
		KeyHttpSenderTemplate: templateExp,
		KeyRunnerName:         runnerName,
		KeyHttpSenderUrl:      "http://127.0.0.1:8007/logkit/data",
	})
	assert.NoError(t, err)
	var wg sync.WaitGroup
	for _, val := range testData {
		wg.Add(1)
		go func(httpReader *http.Reader, val struct {
			input               []Data
			jsonExp             [][]string
			csvExp              []map[string]string
			bodyJSONExp         string
			templateJSONExp     []string
			templateBodyJSONExp string
		}, t *testing.T) {
			for _, exp := range val.templateJSONExp {
				got, err := httpReader.ReadLine()
				assert.NoError(t, err)
				assert.Equal(t, got, exp)
			}
			wg.Done()
		}(httpReader, val, t)
		assert.NoError(t, httpSender.Send(val.input))
	}
	wg.Wait()
}

func TestHTTPSenderJSONBodyWithTemplate(t *testing.T) {
	t.Parallel()
	runnerName := "TestHTTPSenderJSONWithTamplate"

	httpReader, err := newHTTPReader(runnerName, "8008")
	assert.NoError(t, err)
	defer func() {
		os.RemoveAll("./meta/" + runnerName)
		httpReader.Close()
	}()

	//	// CI 环境启动监听较慢，需要等待几秒
	time.Sleep(2 * time.Second)

	httpSender, err := NewSender(conf.MapConf{
		KeyHttpSenderGzip:     "false",
		KeyHttpSenderCsvSplit: "\t",
		KeyHttpSenderProtocol: "body_json",
		KeyHttpSenderCsvHead:  "false",
		KeyHttpSenderTemplate: templateExp,
		KeyRunnerName:         runnerName,
		KeyHttpSenderUrl:      "http://127.0.0.1:8008/logkit/data",
	})
	assert.NoError(t, err)
	var wg sync.WaitGroup
	for _, val := range testData {
		wg.Add(1)
		go func(httpReader *http.Reader, val struct {
			input               []Data
			jsonExp             [][]string
			csvExp              []map[string]string
			bodyJSONExp         string
			templateJSONExp     []string
			templateBodyJSONExp string
		}, t *testing.T) {
			got, err := httpReader.ReadLine()
			assert.NoError(t, err)
			assert.Equal(t, got, val.templateBodyJSONExp)
			wg.Done()
		}(httpReader, val, t)
		assert.NoError(t, httpSender.Send(val.input))
	}
	wg.Wait()
}

func TestGzipData(t *testing.T) {
	t.Parallel()
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
		gr, err := gzip.NewReader(bytes.NewReader(gotByte))
		assert.NoError(t, err)
		data, err := ioutil.ReadAll(gr)
		assert.NoError(t, err)
		assert.Equal(t, val, string(data))
	}
}

func Test_renderTemplate(t *testing.T) {
	t.Parallel()
	httpSender := Sender{
		protocol:   "json",
		escapeHtml: true,
	}
	actual, err := httpSender.renderTemplate(Data{"raw": "<a&b&c>"})
	assert.Nil(t, err)
	assert.EqualValues(t, "{\"raw\":\"\\u003ca\\u0026b\\u0026c\\u003e\"}", actual)

	httpSender.escapeHtml = false
	actual, err = httpSender.renderTemplate(Data{"raw": "<a&b&c>"})
	assert.Nil(t, err)
	assert.EqualValues(t, "{\"raw\":\"<a&b&c>\"}", actual)
}
