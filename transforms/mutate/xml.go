package mutate

import (
	"errors"
	"fmt"
	"strings"

	"github.com/clbanning/mxj"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	_ transforms.StatsTransformer = &Xml{}
	_ transforms.Transformer      = &Xml{}
	_ transforms.Initializer      = &Xml{}
)

type Xml struct {
	Key   string `json:"key"`
	New   string `json:"new"`
	stats StatsInfo

	keys []string
	news []string
}

func (g *Xml) Init() error {
	g.keys = GetKeys(g.Key)
	g.news = GetKeys(g.New)
	return nil
}

func (g *Xml) Transform(datas []Data) ([]Data, error) {
	var err, fmtErr error
	errNum := 0
	if g.keys == nil {
		g.Init()
	}
	for i := range datas {
		val, getErr := GetMapValue(datas[i], g.keys...)
		if getErr != nil {
			errNum, err = transforms.SetError(errNum, getErr, transforms.GetErr, g.Key)
			continue
		}
		strVal, ok := val.(string)
		if !ok {
			typeErr := fmt.Errorf("transform key %v data type is not string", g.Key)
			errNum, err = transforms.SetError(errNum, typeErr, transforms.General, "")
			continue
		}
		strVal = strings.TrimSpace(strVal)
		if len(strVal) < 1 {
			continue
		}
		xmlVal, perr := parseXml(strVal)
		if perr != nil {
			errNum, err = transforms.SetError(errNum, perr, transforms.General, "")
			continue
		}
		if len(g.news) == 0 {
			DeleteMapValue(datas[i], g.keys...)
			g.news = g.keys
		}
		setErr := SetMapValue(datas[i], xmlVal, false, g.news...)
		if setErr != nil {
			errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, g.New)
		}
	}

	g.stats, fmtErr = transforms.SetStatsInfo(err, g.stats, int64(errNum), int64(len(datas)), g.Type())
	return datas, fmtErr
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

func (g *Xml) SetStats(err string) StatsInfo {
	g.stats.LastError = err
	return g.stats
}

func init() {
	transforms.Add("xml", func() transforms.Transformer {
		return &Xml{}
	})
}
