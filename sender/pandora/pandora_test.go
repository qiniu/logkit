package pandora

import (
	"bytes"
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"

	"github.com/qiniu/log"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"
	"github.com/qiniu/pandora-go-sdk/pipeline"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/sender"
	mockPandora "github.com/qiniu/logkit/sender/mock_pandora"
	"github.com/qiniu/logkit/times"
	. "github.com/qiniu/logkit/utils/models"
)

func TestPandoraSender(t *testing.T) {
	pandora, pt := mockPandora.NewMockPandoraWithPrefix("/v2")
	opt := &PandoraOption{
		name:               "p",
		repoName:           "TestPandoraSender",
		region:             "nb",
		endpoint:           "http://127.0.0.1:" + pt,
		ak:                 "ak",
		sk:                 "sk",
		schema:             "ab, abc a1,d",
		autoCreate:         "ab *s,a1 f*,ac *long,d DATE*",
		updateInterval:     time.Second,
		reqRateLimit:       0,
		flowRateLimit:      0,
		ignoreInvalidField: true,
		autoConvertDate:    true,
		gzip:               true,
		tokenLock:          new(sync.RWMutex),
	}
	s, err := newPandoraSender(opt)
	if err != nil {
		t.Fatal(err)
	}
	d := Data{}
	d["ab"] = "hh"
	d["abc"] = 1.2
	d["ac"] = 2
	d["d"] = 14773736325048765
	err = s.Send([]Data{d})
	if st, ok := err.(*StatsError); ok {
		err = st.ErrorDetail
	}
	if err != nil {
		t.Error(err)
	}
	_, offset := time.Now().Zone()
	value := offset / 3600
	var timestr string
	if value > 0 {
		timestr = fmt.Sprintf("+%02d:00", value)
	} else if value < 0 {
		timestr = fmt.Sprintf("-%02d:00", value)
	}
	if timestr == "" {
		timestr = "Z"
	}
	_, zoneValue := times.GetTimeZone()
	timeVal := int64(1477373632504876500)
	exp := "a1=1.2 ab=hh ac=2 d=" + time.Unix(0, timeVal*int64(time.Nanosecond)).Format(time.RFC3339Nano)
	if pandora.Body != exp {
		t.Errorf("send data error exp %v but %v", exp, pandora.Body)
	}
	d = Data{"ab": "h1"}
	err = s.Send([]Data{d})
	if st, ok := err.(*StatsError); ok {
		err = st.ErrorDetail
	}
	if err != nil {
		t.Error(err)
	}

	if !strings.Contains(pandora.Body, "ac=0") || !strings.Contains(pandora.Body, "ab=h1") || !strings.Contains(pandora.Body, "a1=0") {
		t.Errorf("send data error exp ac=0	ab=h1	a1=0 got %v", pandora.Body)
	}
	d = Data{}
	d["ab"] = "h"
	d["d"] = "2016/11/01 12:00:00.123456" + zoneValue
	err = s.Send([]Data{d})
	if st, ok := err.(*StatsError); ok {
		err = st.ErrorDetail
	}
	if err != nil {
		t.Error(err)
	}

	exp = "a1=0 ab=h ac=0 d=2016-11-01T12:00:00.123456" + timestr
	if pandora.Body != exp {
		t.Errorf("send data error exp %v but %v", exp, pandora.Body)
	}

	dataJson := `{"ab":"REQ","ac":200,"d":14774559431867215}`

	d = Data{}
	jsonDecoder := jsoniter.NewDecoder(bytes.NewReader([]byte(dataJson)))
	jsonDecoder.UseNumber()
	err = jsonDecoder.Decode(&d)
	if err != nil {
		t.Error(err)
	}
	err = s.Send([]Data{d})
	if st, ok := err.(*StatsError); ok {
		err = st.ErrorDetail
	}
	if err != nil {
		t.Error(err)
	}
	exptime := time.Unix(0, int64(1477455943186721500)*int64(time.Nanosecond)).Format(time.RFC3339Nano)
	exp = "a1=0 ab=REQ ac=200 d=" + exptime
	if pandora.Body != exp {
		t.Errorf("send data error exp %v but %v", exp, pandora.Body)
	}
	pandora.ChangeSchema([]pipeline.RepoSchemaEntry{
		{
			Key:       "ab",
			ValueType: "string",
			Required:  true,
		},
		{
			Key:       "a1",
			ValueType: "float",
			Required:  true,
		},
		{
			Key:       "ac",
			ValueType: "long",
			Required:  true,
		},
		{
			Key:       "d",
			ValueType: "date",
			Required:  true,
		},
		{
			Key:       "ax",
			ValueType: "string",
			Required:  false,
		},
	})
	s.UserSchema = parseUserSchema("TestPandoraSenderRepo", "ab, abc a1,d,...")
	time.Sleep(2 * time.Second)
	d["ab"] = "a"
	d["abc"] = 1.1
	d["ac"] = 0
	d["d"] = 1477373632504888
	d["ax"] = "b"
	s.opt.updateInterval = 0
	err = s.Send([]Data{d})
	if st, ok := err.(*StatsError); ok {
		err = st.ErrorDetail
	}
	if err != nil {
		t.Error(err)
	}

	timeVal = int64(1477373632504888)
	timeexp := time.Unix(0, timeVal*int64(time.Microsecond)).Format(time.RFC3339Nano)
	exp = "a1=1.1 ab=a ac=0 ax=b d=" + timeexp
	assert.Equal(t, exp, pandora.Body)

}

func TestNestPandoraSender(t *testing.T) {
	pandora, pt := mockPandora.NewMockPandoraWithPrefix("/v2")
	opt := &PandoraOption{
		name:           "p_TestNestPandoraSender",
		repoName:       "TestNestPandoraSender",
		region:         "nb",
		endpoint:       "http://127.0.0.1:" + pt,
		ak:             "ak",
		sk:             "sk",
		schema:         "",
		autoCreate:     "x1 *s,x2 f,x3 l,x4 a(f),x5 {x6 l, x7{x8 a(s),x9 b}}",
		updateInterval: time.Second,
		reqRateLimit:   0,
		flowRateLimit:  0,
		gzip:           false,
		tokenLock:      new(sync.RWMutex),
	}
	s, err := newPandoraSender(opt)
	if err != nil {
		t.Fatal(err)
	}
	d := Data{}
	d["x1"] = "hh"
	d["x2"] = 1.2
	d["x3"] = 2
	d["x4"] = []float64{1.1, 2.2, 3.3}
	d["x5"] = map[string]interface{}{
		"x6": 123,
		"x7": map[string]interface{}{
			"x8": []string{"abc"},
			"x9": true,
		},
	}
	err = s.Send([]Data{d})
	if st, ok := err.(*StatsError); ok {
		err = st.ErrorDetail
	}
	if err != nil {
		t.Error(err)
	}

	exp := "x1=hh x2=1.2 x3=2 x4=[1.1,2.2,3.3] x5={\"x6\":123,\"x7\":{\"x8\":[\"abc\"],\"x9\":true}}"
	if pandora.Body != exp {
		t.Errorf("send data error exp %v but %v", exp, pandora.Body)
	}
}

func TestUUIDPandoraSender(t *testing.T) {
	pandora, pt := mockPandora.NewMockPandoraWithPrefix("/v2")
	opt := &PandoraOption{
		name:           "TestUUIDPandoraSender",
		repoName:       "TestUUIDPandoraSender",
		region:         "nb",
		endpoint:       "http://127.0.0.1:" + pt,
		ak:             "ak",
		sk:             "sk",
		schema:         "",
		autoCreate:     "x1 s",
		updateInterval: time.Second,
		reqRateLimit:   0,
		flowRateLimit:  0,
		gzip:           false,
		uuid:           true,
		schemaFree:     true,
		tokenLock:      new(sync.RWMutex),
	}
	s, err := newPandoraSender(opt)
	if err != nil {
		t.Fatal(err)
	}
	d := Data{}
	d["x1"] = "hh"
	err = s.Send([]Data{d})
	if st, ok := err.(*StatsError); ok {
		err = st.ErrorDetail
	}
	if err != nil {
		t.Error(err)
	}
	if !strings.Contains(pandora.Body, "x1=hh") {
		t.Error("not x1 find error")
	}
	if !strings.Contains(pandora.Body, sender.PandoraUUID) {
		t.Error("no uuid found")
	}
}

func TestStatsSender(t *testing.T) {
	pandora, pt := mockPandora.NewMockPandoraWithPrefix("/v2")
	opt := &PandoraOption{
		name:           "TestStatsSender",
		repoName:       "TestStatsSender",
		region:         "nb",
		endpoint:       "http://127.0.0.1:" + pt,
		ak:             "ak",
		sk:             "sk",
		schema:         "",
		autoCreate:     "x1 s",
		updateInterval: time.Second,
		schemaFree:     true,
		tokenLock:      new(sync.RWMutex),
	}
	s, err := newPandoraSender(opt)
	if err != nil {
		t.Fatal(err)
	}
	d := Data{}
	d["x1"] = "hh"
	err = s.Send([]Data{d})
	assert.NoError(t, err)
	if !strings.Contains(pandora.Body, "x1=hh") {
		t.Error("not x1 find error")
	}
}

func TestConvertDate(t *testing.T) {
	tnow := time.Now()
	var tt interface{}
	tt = tnow.UnixNano()
	newtime, err := convertDate(tt, forceMicrosecondOption{0, true})
	if err != nil {
		t.Error(err)
	}
	exp := tnow.Format(time.RFC3339Nano)
	if newtime != exp {
		t.Errorf("convertDate error exp %v,got %v", exp, newtime)
	}
	ntnow := tnow.UnixNano() / int64(time.Microsecond) * int64(time.Microsecond)
	tt = time.Unix(0, ntnow).Format(time.RFC3339Nano)
	newtime, err = convertDate(tt, forceMicrosecondOption{0, true})
	if err != nil {
		t.Error(err)
	}
	exp = time.Unix(0, ntnow).Format(time.RFC3339Nano)
	if newtime != exp {
		t.Errorf("convertDate error exp %v,got %v", exp, newtime)
	}
	ntSec := tnow.UnixNano() / int64(time.Second)
	ntnow = ntSec * int64(time.Second)
	exp = time.Unix(0, ntnow).Format(time.RFC3339Nano)
	newtime, err = convertDate(ntSec, forceMicrosecondOption{0, true})
	if err != nil {
		t.Error(err)
	}
	if newtime != exp {
		t.Errorf("convertDate error exp %v,got %v", exp, newtime)
	}
}

func TestSuppurtedTimeFormat(t *testing.T) {
	tests := []interface{}{
		"2017/03/28 15:41:53",
		"2017-03-28 02:31:55.091",
		"2017-04-05T18:15:01+08:00",
		int(1490668315),
		int64(1490715713123456),
		int64(1490668315323498123),
		"1",
		nil,
	}

	for i, input := range tests {
		force := int64(i)
		gotVal, err := convertDate(input, forceMicrosecondOption{uint64(force), true})
		if err != nil {
			assert.Equal(t, input, gotVal)
			continue
		}
		gotTimeStr, ok := gotVal.(string)
		if !ok {
			t.Fatalf("assert error, gotVal is not string, %v", gotVal)
		}
		gotTime, err := times.StrToTime(gotTimeStr)
		assert.NoError(t, err)
		gotTimeStamp := gotTime.UnixNano()
		var inputStr string
		switch input.(type) {
		case int:
			var t time.Time
			in := input.(int)
			length := len(strconv.FormatInt(int64(in), 10))
			if length == 10 {
				t = time.Unix(int64(in), 0)
			} else if length == 16 {
				t = time.Unix(0, int64(in)*int64(time.Microsecond))
			} else if length == 19 {
				force = int64(0)
				t = time.Unix(0, int64(in))
			}
			inputStr = t.Format(time.RFC3339Nano)
		case int64:
			var t time.Time
			in := input.(int64)
			length := len(strconv.FormatInt(int64(in), 10))
			if length == 10 {
				t = time.Unix(int64(in), 0)
			} else if length == 16 {
				t = time.Unix(0, int64(in)*int64(time.Microsecond))
			} else if length == 19 {
				force = int64(0)
				t = time.Unix(0, int64(in))
			}
			inputStr = t.Format(time.RFC3339Nano)
		case string:
			inputStr = input.(string)
		default:
			t.Errorf("unknow type, %v", input)
		}
		expTime, err := times.StrToTime(inputStr)
		assert.NoError(t, err)
		expTimeStamp := expTime.UnixNano()

		assert.Equal(t, sender.TimestampPrecision, len(strconv.FormatInt(gotTimeStamp, 10)))
		assert.Equal(t, force, gotTimeStamp-expTimeStamp)
	}
}

// 修改前: 100000	     10861 ns/op
// 当前:  100000	     10128 ns/op
func Benchmark_TimeFormat(b *testing.B) {
	tests := []interface{}{
		"2017/03/28 15:41:53",
		"2017-03-28 02:31:55.091",
		"2017-04-05T18:15:01+08:00",
		int(1490668315),
		int64(1490715713123456),
		int64(1490668315323498123),
		"1",
		nil,
	}
	for i := 0; i < b.N; i++ {
		for i, input := range tests {
			convertDate(input, forceMicrosecondOption{uint64(i), true})
		}
	}
}

func TestValidSchema(t *testing.T) {
	tests := []struct {
		v             interface{}
		t             string
		numberAsFloat bool
		exp           bool
	}{
		{
			v:   1,
			t:   "long",
			exp: true,
		},
		{
			v:   2.1,
			t:   "long",
			exp: false,
		},
		{
			v:             2.1,
			t:             "long",
			numberAsFloat: true,
			exp:           true,
		},
		{
			v:   json.Number("1.0"),
			t:   "long",
			exp: false,
		},
		{
			v:             json.Number("1.1"),
			t:             "long",
			numberAsFloat: true,
			exp:           true,
		},
		{
			v:   json.Number("2.0"),
			t:   "long",
			exp: false,
		},
		{
			v:   json.Number("0"),
			t:   "long",
			exp: true,
		},
		{
			v:   1,
			t:   "float",
			exp: true,
		},
		{
			v:   2.0,
			t:   "float",
			exp: true,
		},
		{
			v:   json.Number("1.0"),
			t:   "float",
			exp: true,
		},
		{
			v:   `{"x":1}`,
			t:   "jsonstring",
			exp: true,
		},
		{
			v:   `{"x":1`,
			t:   "jsonstring",
			exp: false,
		},
		{
			v:   `{"x":`,
			t:   "string",
			exp: true,
		},
		{
			v:   "true",
			t:   PandoraTypeBool,
			exp: true,
		},
		{
			v:   map[string]interface{}{"ahh": 123},
			t:   PandoraTypeMap,
			exp: true,
		},
		{
			v:   []string{"12"},
			t:   PandoraTypeMap,
			exp: false,
		},
		{
			v:   []string{"12"},
			t:   PandoraTypeArray,
			exp: true,
		},
		{
			v:   time.Now(),
			t:   PandoraTypeDate,
			exp: true,
		},
		{
			v:   time.Now().Format(time.RFC3339Nano),
			t:   PandoraTypeDate,
			exp: true,
		},
		{
			v:   time.Now().Format(time.RFC3339),
			t:   PandoraTypeDate,
			exp: true,
		},
	}
	for idx, ti := range tests {
		got := validSchema(ti.t, ti.v, ti.numberAsFloat)
		if got != ti.exp {
			t.Errorf("case %v %v exp %v but got %v", idx, ti, ti.exp, got)
		}
	}
}

func TestParseUserSchema(t *testing.T) {
	tests := []struct {
		schema string
		exp    UserSchema
	}{
		{
			schema: "a",
			exp: UserSchema{
				DefaultAll: false,
				Fields: map[string]string{
					"a": "a",
				},
			},
		},
		{
			schema: "a b,x z y,...",
			exp: UserSchema{
				DefaultAll: true,
				Fields: map[string]string{
					"a": "b",
				},
			},
		},
		{
			schema: " ",
			exp: UserSchema{
				DefaultAll: true,
				Fields:     map[string]string{},
			},
		},
		{
			schema: "a b,x,z y,",
			exp: UserSchema{
				DefaultAll: false,
				Fields: map[string]string{
					"a": "b",
					"x": "x",
					"z": "y",
				},
			},
		},
	}
	for idx, ti := range tests {
		us := parseUserSchema(fmt.Sprintf("repo%v", idx), ti.schema)
		if !reflect.DeepEqual(ti.exp, us) {
			t.Errorf("TestParseUserSchema error exp %v but got %v", ti.exp, us)
		}
	}
}

func TestUpdatePandoraSchema(t *testing.T) {
	pandora, pt := mockPandora.NewMockPandoraWithPrefix("/v2")
	opt := &PandoraOption{
		name:           "p_TestUpdatePandoraSchema",
		repoName:       "TestUpdatePandoraSchema",
		region:         "nb",
		endpoint:       "http://127.0.0.1:" + pt,
		ak:             "ak",
		sk:             "sk",
		schema:         "x3 x3change,x4,...",
		schemaFree:     true,
		updateInterval: time.Second,
		tokenLock:      new(sync.RWMutex),
	}
	s, err := newPandoraSender(opt)
	if err != nil {
		t.Fatal(err)
	}
	d := Data{}
	d["x1"] = "hh"
	err = s.Send([]Data{d})
	if st, ok := err.(*StatsError); ok {
		err = st.ErrorDetail
	}
	if err != nil {
		t.Error(err)
	}
	exp := "x1=hh"
	if pandora.Body != exp {
		t.Errorf("send data error exp %v but %v", exp, pandora.Body)
	}
	d["x2"] = 2
	err = s.Send([]Data{d})
	if st, ok := err.(*StatsError); ok {
		err = st.ErrorDetail
	}
	if err != nil {
		t.Error(err)
	}
	exp = "x1=hh x2=2"
	if pandora.Body != exp {
		t.Errorf("send data error exp %v but %v", exp, pandora.Body)
	}
	expschema := []pipeline.RepoSchemaEntry{
		pipeline.RepoSchemaEntry{
			Key:       "x1",
			ValueType: PandoraTypeString,
		},
		pipeline.RepoSchemaEntry{
			Key:       "x2",
			ValueType: PandoraTypeLong,
		},
	}
	assert.Equal(t, expschema, pandora.Schemas)
	d["x1"] = "hh"
	d["x2"] = 2
	d["x3"] = 2.1
	err = s.Send([]Data{d})
	if st, ok := err.(*StatsError); ok {
		err = st.ErrorDetail
	}
	if err != nil {
		t.Error(err)
	}
	exp = "x1=hh x2=2 x3change=2.1"
	assert.Equal(t, exp, pandora.Body)

	expschema = []pipeline.RepoSchemaEntry{
		pipeline.RepoSchemaEntry{
			Key:       "x1",
			ValueType: PandoraTypeString,
		},
		pipeline.RepoSchemaEntry{
			Key:       "x2",
			ValueType: PandoraTypeLong,
		},
		pipeline.RepoSchemaEntry{
			Key:       "x3change",
			ValueType: PandoraTypeFloat,
		},
	}
	assert.Equal(t, expschema, pandora.Schemas)
	log.Println("\n\n ===========second =======")
	pandora.Body = ""
	pandora.Schemas = nil
	//第二轮测试
	opt = &PandoraOption{
		name:           "p_TestUpdatePandoraSchema",
		repoName:       "TestUpdatePandoraSchema",
		region:         "nb",
		endpoint:       "http://127.0.0.1:" + pt,
		ak:             "ak",
		sk:             "sk",
		schema:         "x3 x3change,x4",
		schemaFree:     true,
		updateInterval: time.Second,
		tokenLock:      new(sync.RWMutex),
	}
	s, err = newPandoraSender(opt)
	if err != nil {
		t.Fatal(err)
	}
	d["x1"] = "hh"
	tm := time.Now().Format(time.RFC3339)
	d["x3"] = tm
	err = s.Send([]Data{d})
	if st, ok := err.(*StatsError); ok {
		err = st.ErrorDetail
	}
	if err != nil {
		t.Error(err)
	}
	exp = "x3change=" + tm
	assert.Equal(t, exp, pandora.Body)

	expschema = []pipeline.RepoSchemaEntry{
		pipeline.RepoSchemaEntry{
			Key:       "x3change",
			ValueType: PandoraTypeDate,
		},
	}
	assert.Equal(t, expschema, pandora.Schemas)

	d["x3"] = tm
	d["x4"] = 1
	err = s.Send([]Data{d})
	if st, ok := err.(*StatsError); ok {
		err = st.ErrorDetail
	}
	if err != nil {
		t.Error(err)
	}
	exp = "x3change=" + tm + " x4=1"
	assert.Equal(t, exp, pandora.Body)

	expschema = []pipeline.RepoSchemaEntry{
		pipeline.RepoSchemaEntry{
			Key:       "x3change",
			ValueType: PandoraTypeDate,
		},
		pipeline.RepoSchemaEntry{
			Key:       "x4",
			ValueType: PandoraTypeLong,
		},
	}
	assert.Equal(t, expschema, pandora.Schemas)
}

func TestAlignTimestamp(t *testing.T) {
	tests := []struct {
		input   int64
		counter int64
		output  int64
	}{
		{
			input:   int64(1),
			counter: int64(0),
			output:  int64(1000000000000000000),
		},
		{ //秒 -> 微秒
			input:   int64(1498720797),
			counter: int64(0),
			output:  int64(1498720797000000000),
		},
		{ //秒 + 扰动 -> 微秒
			input:   int64(1498720797),
			counter: int64(10000),
			output:  int64(1498720797000010000),
		},
		{ //毫秒 -> 微秒
			input:   int64(1498720797123),
			counter: int64(0),
			output:  int64(1498720797123000000),
		},
		{ //毫秒 + 扰动 -> 微秒
			input:   int64(1498720797123),
			counter: int64(100),
			output:  int64(1498720797123000100),
		},
		{ //微秒 -> 微秒
			input:   int64(1498720797123123),
			counter: int64(0),
			output:  int64(1498720797123123000),
		},
		{ //微秒 + 扰动 -> 微秒
			input:   int64(1498720797123123),
			counter: int64(100),
			output:  int64(1498720797123123100),
		},
		{ //保留纳秒
			input:   int64(1498720797123123123),
			counter: int64(100),
			output:  int64(1498720797123123123),
		},
	}
	for _, test := range tests {
		output := alignTimestamp(test.input, uint64(test.counter))
		if output != test.output {
			t.Errorf("input: %v\noutput: %v\nexp: %v\ncounter: %v", test.input, output, test.output, test.counter)
		}
	}
}

func TestConvertDataPandoraSender(t *testing.T) {
	pandora, pt := mockPandora.NewMockPandoraWithPrefix("/v2")
	opt := &PandoraOption{
		name:             "TestConvertDataPandoraSender",
		repoName:         "TestConvertDataPandoraSender",
		region:           "nb",
		endpoint:         "http://127.0.0.1:" + pt,
		ak:               "ak",
		sk:               "sk",
		schema:           "",
		updateInterval:   time.Second,
		reqRateLimit:     0,
		flowRateLimit:    0,
		gzip:             false,
		autoCreate:       "x1 long",
		schemaFree:       true,
		forceDataConvert: true,
		tokenLock:        new(sync.RWMutex),
	}
	s, err := newPandoraSender(opt)
	if err != nil {
		t.Fatal(err)
	}
	d := Data{}
	d["x1"] = "123.2"
	err = s.Send([]Data{d})
	if st, ok := err.(*StatsError); ok {
		err = st.ErrorDetail
	}
	if err != nil {
		t.Error(err)
	}
	if !strings.Contains(pandora.Body, "x1=123") {
		t.Error("not x1 find error")
	}
}

func TestPandoraSenderTime(t *testing.T) {
	pandora, pt := mockPandora.NewMockPandoraWithPrefix("/v2")
	conf1 := conf.MapConf{
		"force_microsecond":         "false",
		"ft_memory_channel":         "false",
		"ft_strategy":               "backup_only",
		"ignore_invalid_field":      "true",
		"logkit_send_time":          "true",
		"pandora_extra_info":        "false",
		"pandora_ak":                "ak",
		"pandora_auto_convert_date": "true",
		"pandora_gzip":              "true",
		"pandora_host":              "http://127.0.0.1:" + pt,
		"pandora_region":            "nb",
		"pandora_repo_name":         "TestPandoraSenderTime",
		"pandora_schema_free":       "true",
		"pandora_sk":                "sk",
		"runner_name":               "runner.20171117110730",
		"sender_type":               "pandora",
		"name":                      "TestPandoraSenderTime",
		"KeyPandoraSchemaUpdateInterval": "1s",
	}
	s, err := NewSender(conf1)
	if err != nil {
		t.Fatal(err)
	}
	d := Data{}
	d["x1"] = "123.2"
	err = s.Send([]Data{d})
	if st, ok := err.(*StatsError); ok {
		err = st.ErrorDetail
	}
	if err != nil {
		t.Error(err)
	}
	lastTime := time.Now()
	resp := pandora.Body
	params := strings.Split(resp, " ")
	if len(params) == 2 {
		logkitSendTime := strings.Split(params[0], "=")
		if len(logkitSendTime) == 2 {
			assert.Equal(t, sender.KeyLogkitSendTime, logkitSendTime[0])
			lastTime, err = time.Parse(time.RFC3339Nano, logkitSendTime[1])
			assert.NoError(t, err)
		} else {
			t.Fatal("response body logkitSendTime error, exp logkit_send_time=<value>, but got " + params[0])
		}
		x1Value := strings.Split(params[1], "=")
		if len(x1Value) == 2 {
			assert.Equal(t, "x1", x1Value[0])
			assert.Equal(t, "123.2", x1Value[1])
		} else {
			t.Fatal("response body x1 value error, exp x1=123.2, but got " + params[1])
		}
	} else {
		t.Fatal("response body error, exp logkit_send_time=<value> x1=123.2, but got " + resp)
	}
	time.Sleep(1 * time.Second)

	d = Data{}
	d["x1"] = "123.2"
	err = s.Send([]Data{d})
	if st, ok := err.(*StatsError); ok {
		err = st.ErrorDetail
	}
	if err != nil {
		t.Error(err)
	}
	curTime := time.Now()
	resp = pandora.Body
	params = strings.Split(resp, " ")
	if len(params) == 2 {
		logkitSendTime := strings.Split(params[0], "=")
		if len(logkitSendTime) == 2 {
			assert.Equal(t, sender.KeyLogkitSendTime, logkitSendTime[0])
			curTime, err = time.Parse(time.RFC3339Nano, logkitSendTime[1])
			assert.NoError(t, err)
		} else {
			t.Fatal("response body logkitSendTime error, exp logkit_send_time=<value>, but got " + params[0])
		}
		x1Value := strings.Split(params[1], "=")
		if len(x1Value) == 2 {
			assert.Equal(t, "x1", x1Value[0])
			assert.Equal(t, "123.2", x1Value[1])
		} else {
			t.Fatal("response body x1 value error, exp x1=123.2, but got " + params[1])
		}
	} else {
		t.Fatal("response body error, exp logkit_send_time=<value> x1=123.2, but got " + resp)
	}
	assert.Equal(t, true, curTime.Sub(lastTime).Seconds() >= 1.0)

	conf2 := conf.MapConf{
		"force_microsecond":         "false",
		"ft_memory_channel":         "false",
		"ft_strategy":               "backup_only",
		"ignore_invalid_field":      "true",
		"logkit_send_time":          "false",
		"pandora_extra_info":        "false",
		"pandora_ak":                "ak",
		"pandora_auto_convert_date": "true",
		"pandora_gzip":              "true",
		"pandora_host":              "http://127.0.0.1:" + pt,
		"pandora_region":            "nb",
		"pandora_repo_name":         "TestPandoraSenderTime",
		"pandora_schema_free":       "true",
		"pandora_sk":                "sk",
		"runner_name":               "runner.20171117110730",
		"sender_type":               "pandora",
		"name":                      "TestPandoraSenderTime",
		"KeyPandoraSchemaUpdateInterval": "1s",
	}
	s, err = NewSender(conf2)
	if err != nil {
		t.Fatal(err)
	}
	d = Data{}
	d["x1"] = "123.2"
	err = s.Send([]Data{d})
	if st, ok := err.(*StatsError); ok {
		err = st.ErrorDetail
	}
	if err != nil {
		t.Error(err)
	}
	resp = pandora.Body
	assert.Equal(t, resp, "x1=123.2")
}

func TestErrPandoraSender(t *testing.T) {
	_, pt := mockPandora.NewMockPandoraWithPrefix("/v2")
	opt := &PandoraOption{
		name:           "p_TestErrPandoraSender",
		repoName:       "TestErrPandoraSender",
		region:         "nb",
		endpoint:       "http://127.0.0.1:" + pt,
		ak:             "ak",
		sk:             "sk",
		schema:         "PointFailedSendx1",
		autoCreate:     "PointFailedSendx1 s",
		updateInterval: time.Second,
		reqRateLimit:   0,
		flowRateLimit:  0,
		gzip:           false,
		tokenLock:      new(sync.RWMutex),
	}
	s, err := newPandoraSender(opt)
	if err != nil {
		t.Fatal(err)
	}
	d1 := Data{"x2": 1.2}
	d2 := Data{}
	d2["x3"] = 2
	d2["x4"] = []float64{1.1, 2.2, 3.3}
	d2["x5"] = map[string]interface{}{
		"x6": 123,
		"x7": map[string]interface{}{
			"x8": []string{"abc"},
			"x9": true,
		},
	}
	d3 := Data{"PointFailedSendx1": "x1Val"}
	err = s.Send([]Data{d1, d2, d3})
	assert.Error(t, err)
	st, ok := err.(*StatsError)
	if !ok {
		t.Errorf("expect StatsError, but got: %v", err)
	}

	sendError, ok := st.ErrorDetail.(*reqerr.SendError)
	if !ok {
		t.Errorf("expect SendError, but got: %v", err)
	}
	assert.Equal(t, []map[string]interface{}{{"PointFailedSendx1": "x1Val"}}, sendError.GetFailDatas())
	assert.Equal(t, int64(1), st.Errors)
	assert.Equal(t, int64(2), st.Success)
}

func TestAliasPandoraSender(t *testing.T) {
	_, pt := mockPandora.NewMockPandoraWithPrefix("/v2")
	opt := &PandoraOption{
		name:           "p_TestErrPandoraSender",
		repoName:       "TestErrPandoraSender",
		region:         "nb",
		endpoint:       "http://127.0.0.1:" + pt,
		ak:             "ak",
		sk:             "sk",
		schema:         "x1 PointFailedSendx1",
		autoCreate:     "PointFailedSendx1 s",
		updateInterval: time.Second,
		reqRateLimit:   0,
		flowRateLimit:  0,
		gzip:           false,
		tokenLock:      new(sync.RWMutex),
	}
	s, err := newPandoraSender(opt)
	if err != nil {
		t.Fatal(err)
	}
	dataSlice := []struct {
		data   Data
		expect int
	}{
		{
			Data{"x2": 1.2},
			1,
		},
		{
			Data{
				"x3": 2,
				"x4": []float64{1.1, 2.2, 3.3},
				"x5": map[string]interface{}{
					"x6": 123,
					"x7": map[string]interface{}{
						"x8": []string{"abc"},
						"x9": true,
					},
				},
			},
			3,
		},
		{
			Data{"x1": "x1Val"},
			1,
		},
	}
	var datas []Data
	for _, d := range dataSlice {
		datas = append(datas, d.data)
	}
	err = s.Send(datas)
	assert.Error(t, err)
	assert.Equal(t, 3, len(datas))
	for idx, data := range datas {
		assert.Equal(t, dataSlice[idx].expect, len(data))
	}
}
