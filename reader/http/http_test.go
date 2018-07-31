package http

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/reader/test"
	. "github.com/qiniu/logkit/utils/models"
)

func TestNewHttpReader(t *testing.T) {
	readConf := conf.MapConf{
		reader.KeyMetaPath: MetaDir,
		reader.KeyFileDone: MetaDir,
		reader.KeyMode:     reader.ModeHTTP,
		KeyRunnerName:      "TestNewHttpReader",
	}
	meta, err := reader.NewMetaWithConf(readConf)
	assert.NoError(t, err)
	defer os.RemoveAll("./meta")

	c := conf.MapConf{
		reader.KeyHTTPServiceAddress: ":7110",
		reader.KeyHTTPServicePath:    "/logkit/data",
	}
	hhttpReader, err := NewReader(meta, c)
	assert.NoError(t, err)
	httpReader := hhttpReader.(*Reader)
	assert.NoError(t, httpReader.Start())
	defer httpReader.Close()

	// CI 环境启动监听较慢，需要等待几秒
	time.Sleep(3 * time.Second)

	testData := []string{
		"1234567890987654321",
		"qwertyuiopoiuytrewq",
		"zxcvbnm,./.,mnbvcxz",
		"asdfghjkl;';lkjhgfdsa",
	}

	// 测试正常发送
	req, err := http.NewRequest(http.MethodPost, "http://127.0.0.1:7110/logkit/data", nil)
	assert.NoError(t, err)
	for _, val := range testData {
		req.Body = ioutil.NopCloser(bytes.NewReader([]byte(val)))
		resp, err := http.DefaultClient.Do(req)
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		got, err := httpReader.ReadLine()
		assert.NoError(t, err)
		assert.Equal(t, val, got)
	}

	// 测试 gzip 发送
	req, err = http.NewRequest(http.MethodPost, "http://127.0.0.1:7110/logkit/data", nil)
	req.Header.Set(ContentTypeHeader, ApplicationGzip)
	req.Header.Set(ContentEncodingHeader, "gzip")
	assert.NoError(t, err)
	for _, val := range testData {
		var buf bytes.Buffer
		g := gzip.NewWriter(&buf)
		_, err := g.Write([]byte(val))
		assert.NoError(t, err)
		g.Close()
		byteVal := buf.Bytes()
		req.Body = ioutil.NopCloser(bytes.NewReader(byteVal))
		resp, err := http.DefaultClient.Do(req)
		assert.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.StatusCode)
		got, err := httpReader.ReadLine()
		assert.NoError(t, err)
		assert.Equal(t, val, got)
	}
}
