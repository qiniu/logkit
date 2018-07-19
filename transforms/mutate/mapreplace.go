package mutate

import (
	"errors"
	"fmt"
	"io/ioutil"
	"reflect"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/json-iterator/go"
	"github.com/qiniu/pandora-go-sdk/pipeline"
)

var (
	_ transforms.StatsTransformer = &MapReplacer{}
	_ transforms.Transformer      = &MapReplacer{}
)

type MapReplacer struct {
	Key     string `json:"key"`
	Map     string `json:"map"`
	MapFile string `json:"map_file"`
	New     string `json:"new"`
	rp      map[string]string
	stats   StatsInfo
}

func (g *MapReplacer) Init() error {
	if g.Map != "" {
		g.rp = GetMapList(g.Map)
		if len(g.rp) < 1 {
			return fmt.Errorf("map %v is invalid or empty", g.Map)
		}
		return nil
	}
	if g.MapFile == "" {
		return errors.New("map or map_file is all empty")
	}
	data, err := ioutil.ReadFile(g.MapFile)
	if err != nil {
		return fmt.Errorf("read %v err %v", g.MapFile, err)
	}
	g.rp = make(map[string]string)
	err = jsoniter.Unmarshal(data, &g.rp)
	if err != nil {
		return fmt.Errorf("read %v as mapdata err %v", g.MapFile, err)
	}
	return nil
}

func (g *MapReplacer) convert(value string) (string, bool) {
	ret, ok := g.rp[value]
	if !ok {
		return value, false
	}
	return ret, true
}

func (g *MapReplacer) Transform(datas []Data) ([]Data, error) {
	var err, fmtErr error
	errNum := 0
	keys := GetKeys(g.Key)
	news := GetKeys(g.New)

	for i := range datas {
		val, getErr := GetMapValue(datas[i], keys...)
		if getErr != nil {
			errNum, err = transforms.SetError(errNum, getErr, transforms.GetErr, g.Key)
			continue
		}
		strVal, ok := val.(string)
		if !ok {
			newVal, subErr := dataConvert(val, DslSchemaEntry{ValueType: pipeline.PandoraTypeString})
			if subErr != nil {
				typeErr := fmt.Errorf("transform key %v try to convert data %v to string err %v", g.Key, newVal, subErr)
				errNum, err = transforms.SetError(errNum, typeErr, transforms.General, "")
				continue
			}
			strVal, ok = newVal.(string)
			if !ok {
				var rtp string
				if newVal == nil {
					rtp = "nil"
				} else {
					rtp = reflect.TypeOf(newVal).Name()
				}
				typeErr := fmt.Errorf("transform key %v data type is not string, but %s", g.Key, rtp)
				errNum, err = transforms.SetError(errNum, typeErr, transforms.General, "")
				continue
			}
		}

		if len(news) == 0 {
			news = keys
		}
		setVal, set := g.convert(strVal)
		if !set {
			continue
		}
		setErr := SetMapValue(datas[i], setVal, false, news...)
		if setErr != nil {
			errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, g.Key)
		}
	}

	g.stats, fmtErr = transforms.SetStatsInfo(err, g.stats, int64(errNum), int64(len(datas)), g.Type())
	return datas, fmtErr
}

func (g *MapReplacer) RawTransform(datas []string) ([]string, error) {
	return datas, fmt.Errorf("not support RawTransform")
}

func (g *MapReplacer) Description() string {
	//return "mapreplace replace according to a map"
	return `根据映射关系将字符串全量替换，如关系{"1":"abc"}文件, 则表示将1替换为abc`
}

func (g *MapReplacer) Type() string {
	return "mapreplace"
}

func (g *MapReplacer) SampleConfig() string {
	return `{
		"type":"mapreplace",
		"key":"MapReplaceFieldKey",
		"new":"MapReplaceFieldNewKey"
		"map":"abc 123, xyz nihao",
		"map_file":"/your/path/to/mapfile"
	}`
}

func (g *MapReplacer) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
		transforms.KeyFieldNew,
		{
			KeyName:      "map",
			ChooseOnly:   false,
			Default:      "",
			Required:     false,
			Placeholder:  "映射关系",
			DefaultNoUse: true,
			Description:  "映射关系字符串(map)",
			ToolTip:      "key value用空格隔开，中间用逗号(,)连接",
			Type:         transforms.TransformTypeString,
		},
		{
			KeyName:      "map_file",
			ChooseOnly:   false,
			Default:      "",
			Required:     false,
			Placeholder:  "映射关系文件路径",
			DefaultNoUse: true,
			Description:  "映射关系文件路径(map_file)",
			ToolTip:      "文件是json格式的map,字符串对应字符串",
			Type:         transforms.TransformTypeString,
		},
	}
}

func (g *MapReplacer) Stage() string {
	return transforms.StageAfterParser

}

func (g *MapReplacer) Stats() StatsInfo {
	return g.stats
}

func (g *MapReplacer) SetStats(err string) StatsInfo {
	g.stats.LastError = err
	return g.stats
}

func init() {
	transforms.Add("mapreplace", func() transforms.Transformer {
		return &MapReplacer{}
	})
}
