package mgr

import (
	"net/http"

	"github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/cleaner"
)

func cleanerAPITest(c *testParam) {
	t := c.t
	rs := c.rs

	var got respModeOptions
	url := "http://127.0.0.1" + rs.address + "/logkit/cleaner/options"
	respCode, respBody, err := makeRequest(url, http.MethodGet, []byte{})
	assert.NoError(t, err, string(respBody))
	assert.Equal(t, http.StatusOK, respCode)
	if err = jsoniter.Unmarshal(respBody, &got); err != nil {
		t.Fatalf("respBody %v unmarshal failed, error is %v", respBody, err)
	}
	assert.Equal(t, cleaner.ModeKeyOptions, got.Data)
}
