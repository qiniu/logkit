package mgr

import (
	"encoding/base64"
	"fmt"
	"strings"

	"net/http"

	"github.com/labstack/echo"
	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/utils"
)

type M map[string]interface{}

type ServerConfig struct {
	Prefix string
}

type Service struct {
	Prefix string
}

// PostParseReq 请求体
type PostParseReq struct {
	AK      string `json:"ak"`
	SK      string `json:"sk"`
	Repo    string `json:"repo"`
	LogType string `json:"logType"`
	Log     string `json:"sampleLog"`

	Grok_mode            string `json:"grok_mode"`
	Grok_patterns        string `json:"grok_patterns"`
	Grok_custom_patterns string `json:"grok_custom_patterns"`
}

// PostParseRet 返回值
type PostParseRet struct {
	SamplePoints []sender.Data `json:"SamplePoints"`
}

// post /logkit/parse 接受解析请求
func (rs *RestService) PostParse() echo.HandlerFunc {
	return func(c echo.Context) error {
		req := PostParseReq{}
		if err := c.Bind(&req); err != nil {
			return err
		}
		switch req.LogType {
		case "json", "JSON":
			log, err := base64.StdEncoding.DecodeString(req.Log)
			if err != nil {
				return err
			}
			ret, err := ParseJson(string(log))
			if err != nil {
				return err
			}
			return c.JSON(http.StatusOK, ret)
		case "csv", "CSV":
			ret, err := ParseCSV(req.Log)
			if err != nil {
				return err
			}
			return c.JSON(http.StatusOK, ret)
		case "raw", "RAW":
			ret, err := ParseRaw(req.Log)
			if err != nil {
				return err
			}
			return c.JSON(http.StatusOK, ret)
		case "grok", "GROK":
			grokPattern, err := base64.StdEncoding.DecodeString(req.Grok_patterns)
			customPattern, err := base64.StdEncoding.DecodeString(req.Grok_custom_patterns)
			if err != nil {
				return err
			}
			if string(grokPattern) == "" || req.Log == "" {
				return fmt.Errorf("grok pattern or sample log is empty")
			}
			ret, err := ParseGrok(req.Grok_mode,
				string(grokPattern),
				string(customPattern),
				req.Log,
			)
			if err != nil {
				return err
			}
			return c.JSON(http.StatusOK, ret)
		default:
			return fmt.Errorf("unknown req.LogType %v", req.LogType)
		}
		return nil
	}
}

// ParseGrok 解析grok类型的日志
func ParseGrok(mode, pattern, customPattern, log string) (ret PostParseRet, err error) {
	cfg := conf.MapConf{
		parser.KeyGrokPatterns:       pattern,
		parser.KeyGrokCustomPatterns: customPattern,
		parser.KeyGrokMode:           mode,
	}

	grokParser, err := parser.NewGrokParser(cfg)
	if err != nil {
		return
	}
	data, err := grokParser.Parse([]string{log})
	serr, _ := err.(*utils.StatsError)
	if serr.Errors > 0 {
		err = fmt.Errorf("E! %v", serr)
		return
	}

	ret = PostParseRet{
		SamplePoints: data,
	}

	return ret, nil
}

func ParseJson(log string) (ret PostParseRet, err error) {
	jsonParser, err := parser.NewJsonParser(conf.MapConf{})
	if err != nil {
		return
	}
	data, err := jsonParser.Parse(strings.Split(log, "\n"))
	serr, _ := err.(*utils.StatsError)
	if serr.Errors > 0 {
		err = fmt.Errorf("E! %v", serr)
		return
	}
	ret = PostParseRet{
		SamplePoints: data,
	}
	return ret, nil
}

// ParseCSV 解析csv类型的日志
func ParseCSV(log string) (ret PostParseRet, err error) {
	//not implement
	err = fmt.Errorf("Parse CSV not implement yet")
	return
}

// ParseCSV 解析csv类型的日志
func ParseRaw(log string) (ret PostParseRet, err error) {
	rawParser, err := parser.NewRawlogParser(conf.MapConf{})
	if err != nil {
		return
	}
	data, err := rawParser.Parse(strings.Split(log, "\n"))
	if err != nil {
		return
	}
	ret = PostParseRet{
		SamplePoints: data,
	}
	return
}
