package kafkarest

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
)

func TestKafaRestLogParser(t *testing.T) {
	c := conf.MapConf{}
	c[parser.KeyParserName] = "krp-1"
	c[parser.KeyParserType] = "kafkarest"
	c[parser.KeyDisableRecordErrData] = "true"
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
	if err != nil {
		t.Error(err)
	}
	if len(dts) != 4 {
		t.Fatalf("parse lines error, expect 4 lines but got %v lines", len(dts))
	}
	expected_result_post := make(map[string]interface{})
	expected_result_post[KEY_SRC_IP] = "172.16.16.191"
	expected_result_post[KEY_TOPIC] = "VIP_VvBVy0tuMPPspm1A_0000000000"
	expected_result_post[KEY_METHOD] = "POST"
	expected_result_post[KEY_CODE] = 200
	expected_result_post[KEY_DURATION] = 46
	expected_result_post[KEY_RESP_LEN] = 101640
	post_line := dts[0]
	for k, v := range expected_result_post {
		if v != post_line[k] {
			t.Errorf("unexpected result get of key:%v, %v not equal %v", k, post_line[k], v)
		}
	}

	expected_result_get := make(map[string]interface{})
	expected_result_get[KEY_SRC_IP] = "192.168.85.32"
	expected_result_get[KEY_TOPIC] = "VIP_XfH2Fd3NRCuZpqyP_0000000000"
	expected_result_get[KEY_METHOD] = "GET"
	expected_result_get[KEY_CODE] = 200
	expected_result_get[KEY_DURATION] = 211
	expected_result_get[KEY_RESP_LEN] = 448238
	get_line := dts[3]
	for k, v := range expected_result_get {
		if v != get_line[k] {
			t.Errorf("unexpected result get of key:%v, %v not equal %v", k, get_line[k], v)
		}
	}
	assert.EqualValues(t, "krp-1", p.Name())
}

func TestKafaRestLogParserForErrData(t *testing.T) {
	c := conf.MapConf{}
	c[parser.KeyParserName] = "krp-1"
	c[parser.KeyParserType] = "kafkarest"
	c[parser.KeyDisableRecordErrData] = "false"
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
	if err != nil {
		t.Error(err)
	}
	if len(dts) != 2 {
		t.Fatalf("parse lines error, expect 2 lines but got %v lines", len(dts))
	}
	expected_result_post := make(map[string]interface{})
	expected_result_post[KEY_SRC_IP] = "172.16.16.191"
	expected_result_post[KEY_TOPIC] = "VIP_VvBVy0tuMPPspm1A_0000000000"
	expected_result_post[KEY_METHOD] = "POST"
	expected_result_post[KEY_CODE] = 200
	expected_result_post[KEY_DURATION] = 46
	expected_result_post[KEY_RESP_LEN] = 101640
	post_line := dts[0]
	for k, v := range expected_result_post {
		if v != post_line[k] {
			t.Errorf("unexpected result get of key:%v, %v not equal %v", k, post_line[k], v)
		}
	}

	assert.EqualValues(t, "krp-1", p.Name())
}

func TestParseField(t *testing.T) {
	rest_parser := &Parser{}
	log := `[2016-12-05 03:35:20,682] INFO 172.16.16.191 - - [05/Dec/2016:03:35:20 +0000] "POST /topics/VIP_VvBVy0tuMPPspm1A_0000000000 HTTP/1.1" 200 101640  46 (io.confluent.rest-utils.requests)` + "\n"
	fields := strings.Split(log, " ")
	//time := rest_parser.ParseLogTime(fields)

	ip := rest_parser.ParseIp(fields)
	if ip == EMPTY_STRING {
		t.Error("failed to parse field ip")
	}
	method := rest_parser.ParseMethod(fields)
	if method == EMPTY_STRING {
		t.Error("failed to parse field method")
	}

	topic := rest_parser.ParseTopic(fields)
	if topic == EMPTY_STRING {
		t.Error("failed to parse field topic")
	}

	code := rest_parser.ParseCode(fields)
	if code == 0 {
		t.Error("failed to parse field code")
	}

	resp_len := rest_parser.ParseRespCL(fields)
	if resp_len == 0 {
		t.Error("failed to parse field resplen")
	}

	duration := rest_parser.ParseDuration(fields)
	if duration == 0 {
		t.Error("failed to parse field duration")
	}
}
