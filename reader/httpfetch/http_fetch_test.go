package httpfetch

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo"
	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/reader/config"
	. "github.com/qiniu/logkit/reader/test"
	. "github.com/qiniu/logkit/utils/models"
)

var retData = &map[string]string{
	"logkit1": "data1",
	"logkit2": "data2",
	"logkit3": "data3",
	"logkit4": "data4",
}

func getHttpFetchReader(port string) (*Reader, error) {
	readConf := conf.MapConf{
		KeyMetaPath:   MetaDir,
		KeyFileDone:   MetaDir,
		KeyMode:       ModeHTTP,
		KeyRunnerName: "TestNewHttpFetchReader",
	}
	meta, err := reader.NewMetaWithConf(readConf)
	if err != nil {
		return nil, err
	}
	c := conf.MapConf{
		KeyHTTPServiceAddress: "127.0.0.1:" + port,
		KeyHTTPServicePath:    "/logkit/data",
		KeyHttpCron:           "loop1s",
	}
	reader, err := NewReader(meta, c)
	httpReader := reader.(*Reader)
	if err != nil {
		return nil, err
	}
	err = httpReader.Start()
	if err != nil {
		return nil, err
	}
	return httpReader, nil
}

func TestNewHttpFetchReader(t *testing.T) {
	var err error
	go func() {
		_, err = startServer(8888)
		assert.NoError(t, err)
	}()

	httpReader, err := getHttpFetchReader("8888")
	assert.NoError(t, err)
	defer func() {
		os.RemoveAll("./meta")
		httpReader.Close()
	}()
	except := `{"logkit1":"data1","logkit2":"data2","logkit3":"data3","logkit4":"data4"}`
	var line string
	times := 0
	for line == "" && times < 5 {
		line, err = httpReader.ReadLine()
		if err != nil && strings.Contains(err.Error(), "connection refused") {
			return
		}
		assert.NoError(t, err)
		time.Sleep(2 * time.Second)
	}
	assert.NoError(t, err)
	assert.Equal(t, except, line)
}

func startServer(port int) (*http.Server, error) {
	e := echo.New()
	e.GET("/logkit/data", func(c echo.Context) error {
		return c.JSON(http.StatusOK, *retData)
	})

	server := &http.Server{
		Handler: e,
		Addr:    fmt.Sprintf("0.0.0.0:%d", port),
	}
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		return nil, err
	}
	return server, nil
}
