package qplayerqos

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/conf"
)

func TestName(t *testing.T) {
	log.SetOutputLevel(0)
	testCases := []struct {
		conf         conf.MapConf
		expectedName string
	}{
		{
			conf:         conf.MapConf{},
			expectedName: "",
		},
		{
			conf: conf.MapConf{
				"name": "name1",
			},
			expectedName: "name1",
		},
	}
	for i, testCase := range testCases {
		p, err := NewParser(testCase.conf)
		assert.Nil(t, err, fmt.Sprintf("test case %d: failed to create parser", i))
		assert.Equal(t, testCase.expectedName, p.Name(), fmt.Sprintf("test case %d: name not equal", i))
	}
}

func TestParse(t *testing.T) {
	// TODO: write tests for other output formats
	log.SetOutputLevel(0)
	testCases := []struct {
		conf             conf.MapConf
		receiveTimestamp int64
		body             string
		shouldOutput     bool
		expectedFields   map[string]interface{}
	}{
		{
			conf: conf.MapConf{
				"event_types": "e1,e2",
				"e1.fields":   "0:k0,1:k1,2:k2=int",
			},
			receiveTimestamp: 1605518585000,
			body:             "v0\te1\t9999",
			shouldOutput:     true,
			expectedFields: map[string]interface{}{
				"k0":          "v0",
				"k1":          "e1",
				"k2":          int64(9999),
				"server_time": time.Unix(1605518585, 0).Format(time.RFC3339),
			},
		},
		{
			conf: conf.MapConf{
				"event_types":      "e1",
				"e1.min_field_num": "4",
			},
			receiveTimestamp: 1605518585000,
			body:             "v0\te1\t9999",
			shouldOutput:     false,
		},
		{
			conf: conf.MapConf{
				"event_types": "e1",
			},
			receiveTimestamp: 1605518585000,
			body:             "v0\te0\t9999",
			shouldOutput:     false,
		},
		{
			conf: conf.MapConf{
				"event_types": "play.v5",
			},
			receiveTimestamp: 1605518585000,
			body:             "1.2.3.4\tplay.v5\t1605518583000\t35A1C934-92F7-425C-8F10-10F5A256E76A\t1.1.0.78\trtmp\tpili-live-rtmp.weihua.com\tmoshengtest/lc37701002-LWYYKZ\t-\t218.98.28.119\t1605506341998\t1605506401219\t0\t18.70\t0\t46.84\t0\t18.75\t34.00\t501\t754\t97595\t110187\tapp_test\t1\tsession_0",
			shouldOutput:     true,
			expectedFields: map[string]interface{}{
				"client_ip":   "1.2.3.4",
				"tag":         "play.v5",
				"client_time": time.Unix(1605518583, 0).Format(time.RFC3339),
				"server_time": time.Unix(1605518585, 0).Format(time.RFC3339),

				"app_id":        "app_test",
				"play_src_type": int64(1),
				"session_id":    "session_0",
			},
		},
	}

	for i, testCase := range testCases {
		p, err := NewParser(testCase.conf)
		assert.Nil(t, err, fmt.Sprintf("test case %d: failed to create parser", i))
		testMsg := map[string]interface{}{
			"headers": map[string]string{
				"service":   "qos_test",
				"topic":     "topic_qos",
				"timestamp": fmt.Sprint(testCase.receiveTimestamp),
			},
			"body": []byte(testCase.body),
		}

		msgBuf := []byte{}
		msgBuf, err = avroCodec.BinaryFromNative(msgBuf, testMsg)
		assert.Nil(t, err, fmt.Sprintf("test case %d: failed to encode into avro binary", i))

		datas, err := p.Parse([]string{string(msgBuf)})
		if testCase.shouldOutput {
			assert.Len(t, datas, 1, fmt.Sprintf("test case %d: should parse into 1 data", i))
			data := datas[0]
			for k, v := range testCase.expectedFields {
				assert.Equal(t, v, data[k], fmt.Sprintf("test case %d: key %s should be same as expected", i, k))
			}
		} else {
			assert.Len(t, datas, 0, fmt.Sprintf("test case %d: should parse into no datas", i))
		}
	}
}
