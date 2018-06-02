package mutate

import (
	"errors"
	"fmt"
	"io/ioutil"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/json-iterator/go"
)

type MapReplacer struct {
	Key     string `json:"key"`
	Map     string `json:"map"`
	MapFile string `json:"map_file"`
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

func (g *MapReplacer) convert(value string) string {
	ret, ok := g.rp[value]
	if !ok {
		return value
	}
	return ret
}

func (g *MapReplacer) Transform(datas []Data) ([]Data, error) {
	var err, ferr error
	errnums := 0
	keys := GetKeys(g.Key)
	for i := range datas {
		val, gerr := GetMapValue(datas[i], keys...)
		if gerr != nil {
			errnums++
			err = fmt.Errorf("transform key %v not exist in data", g.Key)
			continue
		}
		strval, ok := val.(string)
		if !ok {
			errnums++
			err = fmt.Errorf("transform key %v data type is not string", g.Key)
			continue
		}
		SetMapValue(datas[i], g.convert(strval), false, keys...)
	}

	if err != nil {
		g.stats.LastError = err.Error()
		ferr = fmt.Errorf("find total %v erorrs in transform mapreplace, last error info is %v", errnums, err)
	}
	g.stats.Errors += int64(errnums)
	g.stats.Success += int64(len(datas) - errnums)
	return datas, ferr
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
		"map":"abc 123, xyz nihao",
		"map_file":"/your/path/to/mapfile"
	}`
}

func (g *MapReplacer) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
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

func init() {
	transforms.Add("mapreplace", func() transforms.Transformer {
		return &MapReplacer{}
	})
}
