package reader

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"net/http"
	"os"
	"testing"

	"github.com/qiniu/logkit/conf"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/stretchr/testify/assert"
)

func TestNewHttpReader(t *testing.T) {
	c := conf.MapConf{
		KeyHttpServiceAddress: ":7110",
		KeyHttpServicePath:    "/logkit/data",
	}
	readConf := conf.MapConf{
		KeyMetaPath:   metaDir,
		KeyFileDone:   metaDir,
		KeyMode:       ModeHttp,
		KeyRunnerName: "TestNewHttpReader",
	}
	meta, err := NewMetaWithConf(readConf)
	assert.NoError(t, err)
	defer os.RemoveAll("./meta")
	hhttpReader, err := NewHttpReader(meta, c)
	httpReader:=hhttpReader.(*HttpReader)
	assert.NoError(t, err)
	err = httpReader.Start()
	assert.NoError(t, err)
	defer httpReader.Close()

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
