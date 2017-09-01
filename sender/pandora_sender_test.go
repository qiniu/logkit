package sender

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/qiniu/logkit/times"
	"github.com/qiniu/logkit/utils"

	"sync"

	"github.com/labstack/echo"
	"github.com/qiniu/log"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"
	"github.com/qiniu/pandora-go-sdk/pipeline"
	"github.com/stretchr/testify/assert"
)

type mock_pandora struct {
	Prefix      string
	Port        string
	Body        string
	Schemas     []pipeline.RepoSchemaEntry
	GetRepoErr  bool
	PostSleep   int
	SetMux      sync.Mutex
	PostDataNum int
}

//NewMockPandoraWithPrefix 测试的mock pandora server
func NewMockPandoraWithPrefix(prefix string) (*mock_pandora, string) {
	pandora := &mock_pandora{Prefix: prefix, SetMux: sync.Mutex{}}

	mux := echo.New()
	mux.GET(prefix+"/ping", pandora.GetPing())
	mux.POST(prefix+"/repos/:reponame", pandora.PostRepos_())
	mux.PUT(prefix+"/repos/:reponame", pandora.PutRepos_())
	mux.POST(prefix+"/repos/:reponame/data", pandora.PostRepos_Data())
	mux.GET(prefix+"/repos/:reponame", pandora.GetRepos_())

	var port = 9000
	for {
		address := ":" + strconv.Itoa(port)
		ch := make(chan error, 1)
		go func() {
			defer close(ch)
			err := http.ListenAndServe(address, mux)
			if err != nil {
				ch <- err
			}
		}()
		flag := 0
		start := time.Now()
		for {
			select {
			case _ = <-ch:
				flag = 1
			default:
				if time.Now().Sub(start) > time.Second {
					flag = 2
				}
			}
			if flag != 0 {
				break
			}
		}
		if flag == 2 {
			log.Infof("start to listen and serve at %v with prefix %v", address, prefix)
			break
		}
		port++
	}
	pandora.Port = strconv.Itoa(port)
	return pandora, pandora.Port
}

func (s *mock_pandora) GetPing() echo.HandlerFunc {
	return func(c echo.Context) error {
		ret := "I am " + s.Prefix
		log.Println("get ping,", ret, s.Port)
		return nil
	}
}

type cmdArgs struct {
	CmdArgs []string
}
type PostReposReq struct {
	Schema []pipeline.RepoSchemaEntry `json:"schema"`
	Region string                     `json:"region"`
}

func (s *mock_pandora) PostRepos_() echo.HandlerFunc {
	return func(c echo.Context) error {
		log.Println("PostRepos_ request", c.Get("reponame"))
		var req1 PostReposReq
		if err := c.Bind(&req1); err != nil {
			return err
		}
		s.Schemas = req1.Schema
		return nil
	}
}

func (s *mock_pandora) PostRepos_Data() echo.HandlerFunc {
	return func(c echo.Context) error {
		s.SetMux.Lock()
		defer s.SetMux.Unlock()
		if s.PostSleep > 0 {
			time.Sleep(time.Duration(s.PostSleep) * time.Second)
		}
		var bytesx []byte
		var r *bufio.Reader

		log.Println(c.Get("reponame"), "post data!!!")
		req := c.Request()
		if req.Header.Get("Content-Encoding") == "gzip" {
			reqBody, err := gzip.NewReader(req.Body)
			if err != nil {
				return c.JSON(http.StatusInternalServerError, utils.NewErrorResponse(errors.New("gzip reader error")))
			}
			reqBody.Close()
			r = bufio.NewReader(reqBody)
			log.Println("gzip got")
		} else {
			r = bufio.NewReader(req.Body)
		}
		bytesx, err := ioutil.ReadAll(r)
		if err != nil {
			log.Println("post repo readall error")
			return c.NoContent(http.StatusInternalServerError)
		}
		s.Body = string(bytesx)
		sep := strings.Fields(s.Body)
		sort.Strings(sep)
		s.Body = strings.Join(sep, " ")
		log.Println("get datas: ", s.Body)
		if strings.Contains(s.Body, "E18111") {
			return c.JSON(http.StatusNotFound, utils.NewErrorResponse(errors.New("E18111 mock_pandora error")))
		}
		s.PostDataNum++
		return nil
	}
}

type GetRepoResult struct {
	Region      string                     `json:"region"`
	Schema      []pipeline.RepoSchemaEntry `json:"schema"`
	Group       string                     `json:"group"`
	DerivedFrom string                     `json:"derivedFrom" bson:"-"`
}

func (s *mock_pandora) GetRepos_() echo.HandlerFunc {
	return func(c echo.Context) error {
		if s.GetRepoErr {
			return c.String(http.StatusBadRequest, "this is mock_pandora let GetRepo Error")
		}
		ret := GetRepoResult{
			Schema: s.Schemas,
		}
		return c.JSON(http.StatusOK, ret)
	}
}

func (s *mock_pandora) PutRepos_() echo.HandlerFunc {
	return func(c echo.Context) error {
		log.Println("PutRepos_ request")
		var err error
		var req1 PostReposReq
		err = c.Bind(&req1)
		if err != nil {
			return err
		}
		s.Schemas = req1.Schema
		return nil
	}
}

func (s *mock_pandora) ChangeSchema(Schema []pipeline.RepoSchemaEntry) {
	s.Schemas = Schema
}

func (s *mock_pandora) LetGetRepoError(f bool) {
	s.GetRepoErr = f
}

func TestPandoraSender(t *testing.T) {
	pandora, pt := NewMockPandoraWithPrefix("/v2")
	pandora.LetGetRepoError(true)
	opt := &PandoraOption{
		name:           "p",
		repoName:       "TestPandoraSender",
		region:         "nb",
		endpoint:       "http://127.0.0.1:" + pt,
		ak:             "ak",
		sk:             "sk",
		schema:         "ab, abc a1,d",
		autoCreate:     "ab *s,a1 f*,ac *long,d DATE*",
		updateInterval: time.Second,
		reqRateLimit:   0,
		flowRateLimit:  0,
		gzip:           true,
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
	if err == nil {
		t.Error(fmt.Errorf("should get as send LetGetRepoError error but nil"))
	}
	sts, ok := err.(*utils.StatsError)
	if !ok {
		t.Error("should pasred as State Error")
	}
	err = sts.ErrorDetail
	se, ok := err.(*reqerr.SendError)
	if !ok {
		t.Error("should pasred as Send Error")
	}
	pandora.LetGetRepoError(false)
	err = s.Send(ConvertDatas(se.GetFailDatas()))
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
	timeVal := int64(1477373632504876)
	timeexp := time.Unix(0, timeVal*int64(time.Microsecond)).Format(time.RFC3339Nano)
	exp := "a1=1.2 ab=hh ac=2 d=" + timeexp
	if pandora.Body != exp {
		t.Errorf("send data error exp %v but %v", exp, pandora.Body)
	}
	d = Data{"ab": "h1"}
	err = s.Send([]Data{d})
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
	if err != nil {
		t.Error(err)
	}

	exp = "a1=0 ab=h ac=0 d=2016-11-01T12:00:00.123456" + timestr
	if pandora.Body != exp {
		t.Errorf("send data error exp %v but %v", exp, pandora.Body)
	}

	dataJson := `{"ab":"REQ","ac":200,"d":14774559431867215}`

	d = Data{}
	jsonDecoder := json.NewDecoder(bytes.NewReader([]byte(dataJson)))
	jsonDecoder.UseNumber()
	err = jsonDecoder.Decode(&d)
	if err != nil {
		t.Error(err)
	}
	err = s.Send([]Data{d})
	if err != nil {
		t.Error(err)
	}
	exptime := time.Unix(0, int64(1477455943186721)*int64(time.Microsecond)).Format(time.RFC3339Nano)
	exp = "a1=0 ab=REQ ac=200 d=" + exptime
	if pandora.Body != exp {
		t.Errorf("send data error exp %v but %v", exp, pandora.Body)
	}
	pandora.ChangeSchema([]pipeline.RepoSchemaEntry{
		pipeline.RepoSchemaEntry{
			Key:       "ab",
			ValueType: "string",
			Required:  true,
		},
		pipeline.RepoSchemaEntry{
			Key:       "a1",
			ValueType: "float",
			Required:  true,
		},
		pipeline.RepoSchemaEntry{
			Key:       "ac",
			ValueType: "long",
			Required:  true,
		},
		pipeline.RepoSchemaEntry{
			Key:       "d",
			ValueType: "date",
			Required:  true,
		},
		pipeline.RepoSchemaEntry{
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
	if err != nil {
		t.Error(err)
	}

	timeVal = int64(1477373632504888)
	timeexp = time.Unix(0, timeVal*int64(time.Microsecond)).Format(time.RFC3339Nano)
	exp = "a1=1.1 ab=a ac=0 ax=b d=" + timeexp
	assert.Equal(t, exp, pandora.Body)

}

func TestNestPandoraSender(t *testing.T) {
	pandora, pt := NewMockPandoraWithPrefix("/v2")
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
	if err != nil {
		t.Error(err)
	}

	exp := "x1=hh x2=1.2 x3=2 x4=[1.1,2.2,3.3] x5={\"x6\":123,\"x7\":{\"x8\":[\"abc\"],\"x9\":true}}"
	if pandora.Body != exp {
		t.Errorf("send data error exp %v but %v", exp, pandora.Body)
	}
}

func TestUUIDPandoraSender(t *testing.T) {
	pandora, pt := NewMockPandoraWithPrefix("/v2")
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
	}
	s, err := newPandoraSender(opt)
	if err != nil {
		t.Fatal(err)
	}
	d := Data{}
	d["x1"] = "hh"
	err = s.Send([]Data{d})
	if err != nil {
		t.Error(err)
	}
	if !strings.Contains(pandora.Body, "x1=hh") {
		t.Error("not x1 find error")
	}
	if !strings.Contains(pandora.Body, PandoraUUID) {
		t.Error("no uuid found")
	}
	fmt.Println(pandora.Body)
}

func TestStatsSender(t *testing.T) {
	pandora, pt := NewMockPandoraWithPrefix("/v2")
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
	}
	s, err := newPandoraSender(opt)
	if err != nil {
		t.Fatal(err)
	}
	d := Data{}
	d["x1"] = "hh"
	err = s.Send([]Data{d})
	if err != nil {
		t.Error(err)
	}
	if !strings.Contains(pandora.Body, "x1=hh") {
		t.Error("not x1 find error")
	}
	var sd Sender = s
	stsd, ok := sd.(StatsSender)
	assert.Equal(t, true, ok)
	stats := stsd.Stats()
	expstats := utils.StatsInfo{
		Success: 1,
	}
	assert.Equal(t, stats, expstats)
}

func TestConvertDate(t *testing.T) {
	tnow := time.Now()
	var tt interface{}
	tt = tnow.UnixNano()
	newtime, err := convertDate(tt, forceMicrosecondOption{0, true})
	if err != nil {
		t.Error(err)
	}
	ntnow := tnow.UnixNano() / int64(time.Microsecond) * int64(time.Microsecond)
	exp := time.Unix(0, ntnow).Format(time.RFC3339Nano)
	if newtime != exp {
		t.Errorf("convertDate error exp %v,got %v", exp, newtime)
	}
	tt = exp
	newtime, err = convertDate(tt, forceMicrosecondOption{0, true})
	if err != nil {
		t.Error(err)
	}
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
	tests := []struct {
		timeStr string
		exp     string
	}{
		{
			timeStr: "2017/03/28 15:41:53",
			exp:     "2017-03-28T15:41:53Z",
		},
		{
			timeStr: "2017/03/28 15:41:53.123456",
			exp:     "2017-03-28T15:41:53.123456Z",
		},
		{
			timeStr: "2017-03-28 02:31:55.091",
			exp:     "2017-03-28T02:31:55.091Z",
		},
		{
			timeStr: "2017-03-28 02:31:55",
			exp:     "2017-03-28T02:31:55Z",
		},
		{
			timeStr: "2017-04-05T18:15:01+08:00",
			exp:     "2017-04-05T10:15:01Z",
		},
	}
	for _, ti := range tests {
		val, err := convertDate(ti.timeStr, forceMicrosecondOption{0, true})
		if err != nil {
			t.Error(err)
		}
		gotDateStr := val.(string)
		tt, _ := time.Parse(time.RFC3339Nano, gotDateStr)
		got := tt.UTC().Format(time.RFC3339Nano)
		if ti.exp != got {
			t.Fatalf("TestSuppurtedTimeFormat error exp %v but got %v", ti.exp, got)
		}
	}
}

func TestValidSchema(t *testing.T) {
	tests := []struct {
		v   interface{}
		t   string
		exp bool
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
			v:   json.Number("1.0"),
			t:   "long",
			exp: false,
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
	}
	for idx, ti := range tests {
		got := validSchema(ti.t, ti.v)
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
	pandora, pt := NewMockPandoraWithPrefix("/v2")
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
	}
	s, err := newPandoraSender(opt)
	if err != nil {
		t.Fatal(err)
	}
	d := Data{}
	d["x1"] = "hh"
	err = s.Send([]Data{d})
	if err != nil {
		t.Error(err)
	}
	exp := "x1=hh"
	if pandora.Body != exp {
		t.Errorf("send data error exp %v but %v", exp, pandora.Body)
	}
	d["x2"] = 2
	err = s.Send([]Data{d})
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
	}
	s, err = newPandoraSender(opt)
	if err != nil {
		t.Fatal(err)
	}
	d["x1"] = "hh"
	tm := time.Now().Format(time.RFC3339)
	d["x3"] = tm
	err = s.Send([]Data{d})
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
			output:  int64(1000000000000000),
		},
		{ //秒 -> 微秒
			input:   int64(1498720797),
			counter: int64(0),
			output:  int64(1498720797000000),
		},
		{ //秒 + 扰动 -> 微秒
			input:   int64(1498720797),
			counter: int64(10000),
			output:  int64(1498720797010000),
		},
		{ //毫秒 -> 微秒
			input:   int64(1498720797123),
			counter: int64(0),
			output:  int64(1498720797123000),
		},
		{ //毫秒 + 扰动 -> 微秒
			input:   int64(1498720797123),
			counter: int64(100),
			output:  int64(1498720797123100),
		},
		{ //微秒 -> 微秒
			input:   int64(1498720797123123),
			counter: int64(0),
			output:  int64(1498720797123123),
		},
		{ //微秒 + 扰动 -> 微秒
			input:   int64(1498720797123123),
			counter: int64(100),
			output:  int64(1498720797123123),
		},
		{ //保留纳秒
			input:   int64(1498720797123123123),
			counter: int64(100),
			output:  int64(1498720797123123123),
		},
	}

	for _, test := range tests {
		output := alignTimestamp(test.input, test.counter)
		if output != test.output {
			t.Errorf("test align timestamp fail\ngot:%v\nexp:%v\n", output, test.output)
		}
	}
}
