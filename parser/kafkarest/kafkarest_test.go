package kafkarest

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	. "github.com/qiniu/logkit/parser/config"
	. "github.com/qiniu/logkit/utils/models"
)

func TestKafaRestLogParser(t *testing.T) {
	c := conf.MapConf{}
	c[KeyParserName] = "krp-1"
	c[KeyParserType] = "kafkarest"
	c[KeyDisableRecordErrData] = "true"
	ps := parser.NewRegistry()
	p, err := ps.NewLogParser(c)
	if err != nil {
		t.Error(err)
	}
	lines := []string{
		`[2016-12-05 03:35:20,682] INFO 172.16.16.191 - - [05/Dec/2016:03:35:20 +0000] "POST /topics/VIP_VvBVy0tuMPPspm1A_0000000000 HTTP/1.1" 200 101640  46 (io.confluent.rest-utils.requests)` + "\n",
		`[2016-08-19 22:35:09,232] WARN Accept failed for channel null (org.eclipse.jetty.io.SelectorManager)`,
		`[2016-07-29 18:22:20,478] ERROR Producer error for request io.confluent.kafkarest.ProduceTask@2840cf6d (io.confluent.kafkarest.ProduceTask)`,
		`[2016-12-07 07:35:11,009] INFO 192.168.85.32 - - [07/Dec/2016:07:35:10 +0800] "GET /topics/VIP_XfH2Fd3NRCuZpqyP_0000000000/partitions/16/messages?offset=3857621267&count=20000 HTTP/1.1" 200 448238  211 (io.confluent.rest-utils.requests)` + "\n",
		`a b c d e f g h i j k l m n o p`,
		`abcd efg Warn hijk`,
		"",
	}
	dts, err := p.Parse(lines)
	assert.Error(t, err)
	if len(dts) != 4 {
		t.Fatalf("parse lines error, expect 4 lines but got %v lines", len(dts))
	}
	expectedResultPost := make(map[string]interface{})
	expectedResultPost[KEY_SRC_IP] = "172.16.16.191"
	expectedResultPost[KEY_TOPIC] = "VIP_VvBVy0tuMPPspm1A_0000000000"
	expectedResultPost[KEY_METHOD] = "POST"
	expectedResultPost[KEY_CODE] = 200
	expectedResultPost[KEY_DURATION] = 46
	expectedResultPost[KEY_RESP_LEN] = 101640
	postLine := dts[0]
	for k, v := range expectedResultPost {
		if v != postLine[k] {
			t.Errorf("unexpected result get of key:%v, %v not equal %v", k, postLine[k], v)
		}
	}

	expectedResultGet := make(map[string]interface{})
	expectedResultGet[KEY_SRC_IP] = "192.168.85.32"
	expectedResultGet[KEY_TOPIC] = "VIP_XfH2Fd3NRCuZpqyP_0000000000"
	expectedResultGet[KEY_METHOD] = "GET"
	expectedResultGet[KEY_CODE] = 200
	expectedResultGet[KEY_DURATION] = 211
	expectedResultGet[KEY_RESP_LEN] = 448238
	getLine := dts[3]
	for k, v := range expectedResultGet {
		if v != getLine[k] {
			t.Errorf("unexpected result get of key:%v, %v not equal %v", k, getLine[k], v)
		}
	}
	assert.EqualValues(t, "krp-1", p.Name())
}

func TestKafaRestKeepRawData(t *testing.T) {
	c := conf.MapConf{}
	c[KeyParserName] = "krp-1"
	c[KeyParserType] = "kafkarest"
	c[KeyDisableRecordErrData] = "false"
	c[KeyKeepRawData] = "true"
	ps := parser.NewRegistry()
	p, err := ps.NewLogParser(c)
	if err != nil {
		t.Error(err)
	}
	lines := []string{
		`[2016-12-05 03:35:20,682] INFO 172.16.16.191 - - [05/Dec/2016:03:35:20 +0000] "POST /topics/VIP_VvBVy0tuMPPspm1A_0000000000 HTTP/1.1" 200 101640  46 (io.confluent.rest-utils.requests)` + "\n",
		`[2016-12-07 07:35:11,009] INFO 192.168.85.32 - - [07/Dec/2016:07:35:10 +0800] "GET /topics/VIP_XfH2Fd3NRCuZpqyP_0000000000/partitions/16/messages?offset=3857621267&count=20000 HTTP/1.1" 200 448238  211 (io.confluent.rest-utils.requests)` + "\n",
		`a b`,
	}
	dts, err := p.Parse(lines)
	assert.Error(t, err)
	if len(dts) != 3 {
		t.Fatalf("parse lines error, expect 3 lines but got %v lines", len(dts))
	}
	expectedResultPost := make(map[string]interface{})
	expectedResultPost[KEY_SRC_IP] = "172.16.16.191"
	expectedResultPost[KEY_TOPIC] = "VIP_VvBVy0tuMPPspm1A_0000000000"
	expectedResultPost[KEY_METHOD] = "POST"
	expectedResultPost[KEY_CODE] = 200
	expectedResultPost[KEY_DURATION] = 46
	expectedResultPost[KEY_RESP_LEN] = 101640
	expectedResultPost[KeyRawData] = `[2016-12-05 03:35:20,682] INFO 172.16.16.191 - - [05/Dec/2016:03:35:20 +0000] "POST /topics/VIP_VvBVy0tuMPPspm1A_0000000000 HTTP/1.1" 200 101640  46 (io.confluent.rest-utils.requests)`
	postLine := dts[0]
	for k, v := range expectedResultPost {
		if v != postLine[k] {
			t.Errorf("unexpected result get of key:%v, %v not equal %v", k, postLine[k], v)
		}
	}

	expectedResultGet := make(map[string]interface{})
	expectedResultGet[KEY_SRC_IP] = "192.168.85.32"
	expectedResultGet[KEY_TOPIC] = "VIP_XfH2Fd3NRCuZpqyP_0000000000"
	expectedResultGet[KEY_METHOD] = "GET"
	expectedResultGet[KEY_CODE] = 200
	expectedResultGet[KEY_DURATION] = 211
	expectedResultGet[KEY_RESP_LEN] = 448238
	expectedResultGet[KeyRawData] = `[2016-12-07 07:35:11,009] INFO 192.168.85.32 - - [07/Dec/2016:07:35:10 +0800] "GET /topics/VIP_XfH2Fd3NRCuZpqyP_0000000000/partitions/16/messages?offset=3857621267&count=20000 HTTP/1.1" 200 448238  211 (io.confluent.rest-utils.requests)`
	getLine := dts[1]
	for k, v := range expectedResultGet {
		if v != getLine[k] {
			t.Errorf("unexpected result get of key:%v, %v not equal %v", k, getLine[k], v)
		}
	}

	expectedResultErr := make(map[string]interface{})
	expectedResultErr[KeyPandoraStash] = "a b"
	expectedResultErr[KeyRawData] = "a b"
	errLine := dts[2]
	for k, v := range expectedResultErr {
		if v != errLine[k] {
			t.Errorf("unexpected result get of key:%v, %v not equal %v", k, getLine[k], v)
		}
	}
	assert.EqualValues(t, "krp-1", p.Name())
}

func TestKafaRestLogParserForErrData(t *testing.T) {
	c := conf.MapConf{}
	c[KeyParserName] = "krp-1"
	c[KeyParserType] = "kafkarest"
	c[KeyDisableRecordErrData] = "false"
	ps := parser.NewRegistry()
	p, err := ps.NewLogParser(c)
	if err != nil {
		t.Error(err)
	}
	lines := []string{
		`[2016-12-05 03:35:20,682] INFO 172.16.16.191 - - [05/Dec/2016:03:35:20 +0000] "POST /topics/VIP_VvBVy0tuMPPspm1A_0000000000 HTTP/1.1" 200 101640  46 (io.confluent.rest-utils.requests)` + "\n",
		"",
	}
	dts, err := p.Parse(lines)
	assert.Error(t, err)
	if len(dts) != 2 {
		t.Fatalf("parse lines error, expect 2 lines but got %v lines", len(dts))
	}
	expectedResultPost := make(map[string]interface{})
	expectedResultPost[KEY_SRC_IP] = "172.16.16.191"
	expectedResultPost[KEY_TOPIC] = "VIP_VvBVy0tuMPPspm1A_0000000000"
	expectedResultPost[KEY_METHOD] = "POST"
	expectedResultPost[KEY_CODE] = 200
	expectedResultPost[KEY_DURATION] = 46
	expectedResultPost[KEY_RESP_LEN] = 101640
	postLine := dts[0]
	for k, v := range expectedResultPost {
		if v != postLine[k] {
			t.Errorf("unexpected result get of key:%v, %v not equal %v", k, postLine[k], v)
		}
	}

	assert.EqualValues(t, "krp-1", p.Name())
}

func TestParseField(t *testing.T) {
	restParser := &Parser{}
	log := `[2016-12-05 03:35:20,682] INFO 172.16.16.191 - - [05/Dec/2016:03:35:20 +0000] "POST /topics/VIP_VvBVy0tuMPPspm1A_0000000000 HTTP/1.1" 200 101640  46 (io.confluent.rest-utils.requests)` + "\n"
	fields := strings.Split(log, " ")

	ip := restParser.ParseIp(fields)
	if ip == EMPTY_STRING {
		t.Error("failed to parse field ip")
	}
	method := restParser.ParseMethod(fields)
	if method == EMPTY_STRING {
		t.Error("failed to parse field method")
	}

	topic := restParser.ParseTopic(fields)
	if topic == EMPTY_STRING {
		t.Error("failed to parse field topic")
	}

	code := restParser.ParseCode(fields)
	if code == 0 {
		t.Error("failed to parse field code")
	}

	respLen := restParser.ParseRespCL(fields)
	if respLen == 0 {
		t.Error("failed to parse field resplen")
	}

	duration := restParser.ParseDuration(fields)
	if duration == 0 {
		t.Error("failed to parse field duration")
	}
}
