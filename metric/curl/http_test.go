package curl

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_setData(t *testing.T) {
	t.Parallel()

}

func Test_setDataValue(t *testing.T) {
	t.Parallel()

}

func Test_compareExpectResult(t *testing.T) {
	t.Parallel()
	tests := []struct {
		expectCode   int
		realCode     int
		expectData   string
		realData     string
		expectResult bool
		expectErr    error
	}{
		{
			expectCode:   200,
			realCode:     200,
			expectData:   "ok",
			realData:     "test is ok",
			expectResult: true,
			expectErr:    nil,
		},
		{
			expectCode:   200,
			realCode:     500,
			expectData:   "ok",
			realData:     "test is ok",
			expectResult: false,
			expectErr:    fmt.Errorf("return status code is: 500, expect: 200"),
		},
	}

	for _, test := range tests {
		ok, err := compareExpectResult(test.expectCode, test.realCode, test.expectData, test.realData)
		assert.EqualValues(t, test.expectErr, err)
		assert.EqualValues(t, test.expectResult, ok)
	}
}

func Test_joinIdx(t *testing.T) {
	t.Parallel()
	httpData, httpTimeCost, httpTarget, httpStatusCode, httpRespHead, httpErrState, httpErrMsg := joinIdx("1")
	assert.EqualValues(t, HttpData+"_1", httpData)
	assert.EqualValues(t, HttpTimeCost+"_1", httpTimeCost)
	assert.EqualValues(t, HttpTarget+"_1", httpTarget)
	assert.EqualValues(t, HttpStatusCode+"_1", httpStatusCode)
	assert.EqualValues(t, HttpRespHead+"_1", httpRespHead)
	assert.EqualValues(t, HttpErrState+"_1", httpErrState)
	assert.EqualValues(t, HttpErrMsg+"_1", httpErrMsg)
}
