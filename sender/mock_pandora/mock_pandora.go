package mock_pandora

import (
	"bufio"
	"compress/gzip"
	"errors"
	"io/ioutil"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/json-iterator/go"
	"github.com/labstack/echo"

	"github.com/qiniu/log"
	"github.com/qiniu/pandora-go-sdk/pipeline"

	. "github.com/qiniu/logkit/utils/models"
)

type mockPandora struct {
	Prefix      string
	Port        string
	Body        string
	BodyMux     *sync.RWMutex
	Schemas     []pipeline.RepoSchemaEntry
	GetRepoErr  bool
	PostSleep   int
	SetMux      sync.Mutex
	PostDataNum int
	DumpDataNum int
}

//NewMockPandoraWithPrefix 测试的mock pandora server
func NewMockPandoraWithPrefix(prefix string) (*mockPandora, string) {
	pandora := &mockPandora{Prefix: prefix, SetMux: sync.Mutex{}, BodyMux: new(sync.RWMutex)}

	mux := echo.New()
	mux.GET(prefix+"/ping", pandora.GetPing())
	mux.POST(prefix+"/repos/:reponame", pandora.PostRepos_())
	mux.PUT(prefix+"/repos/:reponame", pandora.PutRepos_())
	mux.POST(prefix+"/repos/:reponame/data", pandora.PostRepos_Data())
	mux.GET(prefix+"/repos/:reponame", pandora.GetRepos_())

	mux.GET(prefix+"/ping", pandora.GetPing())
	mux.POST(prefix+"/stream/:reponame", pandora.PostRepos_())
	mux.PUT(prefix+"/stream/:reponame", pandora.PutRepos_())
	mux.POST(prefix+"/stream/:reponame/data", pandora.PostRepos_Data())
	mux.GET(prefix+"/stream/:reponame", pandora.GetRepos_())

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

func (s *mockPandora) GetPing() echo.HandlerFunc {
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

func (s *mockPandora) PostRepos_() echo.HandlerFunc {
	return func(c echo.Context) error {
		log.Println("PostRepos_ request", c.Get("reponame"))
		var req1 PostReposReq
		if err := c.Bind(&req1); err != nil {
			return err
		}
		s.SetMux.Lock()
		defer s.SetMux.Unlock()
		s.Schemas = req1.Schema
		return nil
	}
}

func (s *mockPandora) PostRepos_Data() echo.HandlerFunc {
	return func(c echo.Context) error {
		log.Println("PostRepos_Data request")
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
				return c.JSON(http.StatusInternalServerError, NewErrorResponse(errors.New("gzip reader error")))
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
		strByte := string(bytesx)
		sep := strings.Fields(strByte)
		sort.Strings(sep)
		s.BodyMux.Lock()
		defer s.BodyMux.Unlock()
		s.Body = strings.Join(sep, " ")

		strByte = strings.TrimSpace(strByte)
		if len(strByte) < 1 {
			c.Response().Header().Set(ContentTypeHeader, ApplicationJson)
			c.Response().WriteHeader(http.StatusNotFound)

			return jsoniter.NewEncoder(c.Response()).Encode(map[string]string{"error": "E18006: empty entity"})
		}

		if strings.HasPrefix(strByte, "PointFailedSend") {
			c.Response().Header().Set(ContentTypeHeader, ApplicationJson)
			c.Response().WriteHeader(http.StatusNotFound)

			return jsoniter.NewEncoder(c.Response()).Encode(errors.New("pandora send points error"))
		}

		if strings.Contains(s.Body, "E18111:") {
			c.Response().Header().Set(ContentTypeHeader, ApplicationJson)
			c.Response().WriteHeader(http.StatusNotFound)

			return jsoniter.NewEncoder(c.Response()).Encode(map[string]string{"error": "E18111: One or more field keys do not resident in the specified repo schema: no such field key <this is mock pandora error>"})
		}

		if strings.Contains(s.Body, "E18110:BackupQueue.Depth") {
			return c.JSON(http.StatusNotFound, NewErrorResponse(errors.New("E18110: mockPandora error")))
		}

		if strings.Contains(s.Body, "It's-an-error") && !strings.Contains(s.Body, KeyPandoraStash) {
			c.Response().Header().Set(ContentTypeHeader, ApplicationJson)
			c.Response().WriteHeader(http.StatusNotFound)

			return jsoniter.NewEncoder(c.Response()).Encode(map[string]string{"error": "E18125: invalid DataSchema"})
		}

		if strings.Contains(s.Body, "22222") && strings.Contains(s.Body, "test_pandora_stream_binary_unpack") {
			c.Response().Header().Set(ContentTypeHeader, ApplicationJson)
			c.Response().WriteHeader(http.StatusNotFound)

			return jsoniter.NewEncoder(c.Response()).Encode(map[string]string{"error": "E18005: Pandora Entity Too Large"})
		}

		if strings.Contains(s.Body, "E18110") {
			log.Println("get datas: ", s.Body)
			c.Response().Header().Set(ContentTypeHeader, ApplicationJson)
			c.Response().WriteHeader(http.StatusNotFound)
			return jsoniter.NewEncoder(c.Response()).Encode(map[string]string{"error": "E18110: mockPandora error"})
		}

		if len(s.Body) > DefaultMaxBatchSize {
			c.Response().Header().Set(ContentTypeHeader, ApplicationJson)
			c.Response().WriteHeader(http.StatusNotFound)
			return jsoniter.NewEncoder(c.Response()).Encode(map[string]string{"error": "E18005: mockPandora error"})
		}

		if strings.Contains(s.Body, "typeBinaryUnpack") && !strings.Contains(s.Body, KeyPandoraStash) {
			c.Response().Header().Set(ContentTypeHeader, ApplicationJson)
			c.Response().WriteHeader(http.StatusBadRequest)
			return jsoniter.NewEncoder(c.Response()).Encode(map[string]string{"error": "E18110: mockPandora error"})
		}
		s.PostDataNum++
		s.DumpDataNum += len(strings.Split(strings.TrimSpace(strByte), "\n"))
		return nil
	}
}

type GetRepoResult struct {
	Region      string                     `json:"region"`
	Schema      []pipeline.RepoSchemaEntry `json:"schema"`
	Group       string                     `json:"group"`
	DerivedFrom string                     `json:"derivedFrom" bson:"-"`
}

func (s *mockPandora) GetRepos_() echo.HandlerFunc {
	return func(c echo.Context) error {
		if s.GetRepoErr {
			return c.String(http.StatusBadRequest, "this is mockPandora let GetRepo Error")
		}
		s.SetMux.Lock()
		defer s.SetMux.Unlock()
		ret := GetRepoResult{
			Schema: s.Schemas,
		}
		return c.JSON(http.StatusOK, ret)
	}
}

func (s *mockPandora) PutRepos_() echo.HandlerFunc {
	return func(c echo.Context) error {
		log.Println("PutRepos_ request")
		var err error
		var req1 PostReposReq
		err = c.Bind(&req1)
		if err != nil {
			return err
		}
		s.SetMux.Lock()
		defer s.SetMux.Unlock()
		s.Schemas = req1.Schema
		return nil
	}
}

func (s *mockPandora) ChangeSchema(Schema []pipeline.RepoSchemaEntry) {
	s.SetMux.Lock()
	defer s.SetMux.Unlock()
	s.Schemas = Schema
}

func (s *mockPandora) LetGetRepoError(f bool) {
	s.GetRepoErr = f
}
