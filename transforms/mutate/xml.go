package mutate

import (
	"errors"
	"fmt"
	"strings"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/clbanning/mxj"
)

type Xml struct {
	Key   string `json:"key"`
	New   string `json:"new"`
	stats StatsInfo
}

func (g *Xml) Transform(datas []Data) ([]Data, error) {
	var err, ferr error
	errCount := 0
	keys := GetKeys(g.Key)
	news := GetKeys(g.New)

	for i := range datas {
		val, gerr := GetMapValue(datas[i], keys...)
		if gerr != nil {
			errCount++
			err = fmt.Errorf("transform key %v not exist in data", g.Key)
			continue
		}
		strval, ok := val.(string)
		if !ok {
			errCount++
			err = fmt.Errorf("transform key %v data type is not string", g.Key)
			continue
		}
		strval = strings.TrimSpace(strval)
		if len(strval) < 1 {
			continue
		}
		xmlVal, perr := parseXml(strval)
		if perr != nil {
			errCount++
			err = perr
			continue
		}
		if len(news) == 0 {
			DeleteMapValue(datas[i], keys...)
			news = keys
		}
		serr := SetMapValue(datas[i], xmlVal, false, news...)
		if serr != nil {
			errCount++
			err = fmt.Errorf("the new key %v already exists ", g.New)
		}
	}

	if err != nil {
		g.stats.LastError = err.Error()
		ferr = fmt.Errorf("find total %v erorrs in transform xml, last error info is %v", errCount, err)
	}
	g.stats.Errors += int64(errCount)
	g.stats.Success += int64(len(datas) - errCount)
	return datas, ferr
}

func (g *Xml) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("xml transformer not support rawTransform")
}

func parseXml(xmlStr string) (data map[string]interface{}, err error) {
	return mxj.NewMapXml([]byte(xmlStr), true)
}

func (g *Xml) Description() string {
	//return "parse xmlString to xml data"
	return "解析xml, 将xml格式转变为map结构"
}

func (g *Xml) Type() string {
	return "xml"
}

func (g *Xml) SampleConfig() string {
	return `{
       "type":"xml",
       "key":"myParseKey",
       "new":"myNewKey"
    }`
}

func (g *Xml) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
		transforms.KeyFieldNewRequired,
	}
}

func (g *Xml) Stage() string {
	return transforms.StageAfterParser
}

func (g *Xml) Stats() StatsInfo {
	return g.stats
}

func init() {
	transforms.Add("xml", func() transforms.Transformer {
		return &Xml{}
	})
}
