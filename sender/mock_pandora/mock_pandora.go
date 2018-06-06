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

type mock_pandora struct {
	Prefix      string
	Port        string
	Body        string
	BodyMux     *sync.RWMutex
	Schemas     []pipeline.RepoSchemaEntry
	GetRepoErr  bool
	PostSleep   int
	SetMux      sync.Mutex
	PostDataNum int
}

//NewMockPandoraWithPrefix 测试的mock pandora server
func NewMockPandoraWithPrefix(prefix string) (*mock_pandora, string) {
	pandora := &mock_pandora{Prefix: prefix, SetMux: sync.Mutex{}, BodyMux: new(sync.RWMutex)}

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
		s.SetMux.Lock()
		defer s.SetMux.Unlock()
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
		sep := strings.Fields(string(bytesx))
		sort.Strings(sep)
		s.BodyMux.Lock()
		defer s.BodyMux.Unlock()
		s.Body = strings.Join(sep, " ")
		log.Println("get datas: ", s.Body)
		if strings.Contains(s.Body, "E18111") {
			return c.JSON(http.StatusNotFound, NewErrorResponse(errors.New("E18111 mock_pandora error")))
		} else if strings.Contains(s.Body, "typeBinaryUnpack") && !strings.Contains(s.Body, KeyPandoraStash) {
			c.Response().Header().Set(ContentTypeHeader, ApplicationJson)
			c.Response().WriteHeader(http.StatusBadRequest)
			return jsoniter.NewEncoder(c.Response()).Encode(map[string]string{"error": "E18111 mock_pandora error"})
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
		s.SetMux.Lock()
		defer s.SetMux.Unlock()
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
		s.SetMux.Lock()
		defer s.SetMux.Unlock()
		s.Schemas = req1.Schema
		return nil
	}
}

func (s *mock_pandora) ChangeSchema(Schema []pipeline.RepoSchemaEntry) {
	s.SetMux.Lock()
	defer s.SetMux.Unlock()
	s.Schemas = Schema
}

func (s *mock_pandora) LetGetRepoError(f bool) {
	s.GetRepoErr = f
}
