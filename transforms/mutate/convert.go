package mutate

import (
	"errors"
	"fmt"

	"github.com/qiniu/logkit/transforms"
	"github.com/qiniu/logkit/utils"
	. "github.com/qiniu/logkit/utils/models"
	"github.com/qiniu/pandora-go-sdk/pipeline"
)

type Converter struct {
	DSL   string `json:"dsl"`
	stats utils.StatsInfo
}

func (g *Converter) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("convert transformer not support rawTransform")
}

func (g *Converter) Transform(datas []Data) ([]Data, error) {
	var err, ferr error
	errnums := 0
	schemas, err := pipeline.DSLtoSchema(g.DSL)
	if err != nil {
		ferr = fmt.Errorf("convert typedsl %s to schema error: %v", g.DSL, err)
		g.stats.LastError = ferr.Error()
		errnums = len(datas)
	} else {
		keyss := map[int][]string{}
		for i, sc := range schemas {
			keys := utils.GetKeys(sc.Key)
			keyss[i] = keys
		}
		for i := range datas {
			for k, keys := range keyss {
				val, gerr := utils.GetMapValue(datas[i], keys...)
				if gerr != nil {
					errnums++
					err = fmt.Errorf("transform key %v not exist in data", schemas[k].Key)
					continue
				}
				val, err = pipeline.DataConvert(val, schemas[k])
				if err != nil {
					errnums++
				}
				utils.SetMapValue(datas[i], val, false, keys...)
			}
		}
	}
	if err != nil {
		g.stats.LastError = err.Error()
		ferr = fmt.Errorf("find total %v erorrs in transform convert, last error info is %v", errnums, err)
	}
	g.stats.Errors += int64(errnums)
	g.stats.Success += int64(len(datas) - errnums)
	return datas, ferr
}

func (g *Converter) Description() string {
	//return "convert can use dsl to convert multi-field data to specify data type"
	return "将dsl指定的多个数据字段和类型转换为指定的数据格式"
}

func (g *Converter) Type() string {
	return "convert"
}

func (g *Converter) SampleConfig() string {
	return `{
		"type":"convert",
		"dsl":"fieldone string"
	}`
}

func (g *Converter) ConfigOptions() []Option {
	return []Option{
		transforms.KeyStageAfterOnly,
		transforms.KeyFieldName,
		{
			KeyName:      "dsl",
			ChooseOnly:   false,
			Default:      "",
			Required:     true,
			Placeholder:  "fieldone string",
			DefaultNoUse: true,
			Description:  "数据转换的dsl描述(dsl)",
			Type:         transforms.TransformTypeString,
		},
	}
}

func (g *Converter) Stage() string {
	return transforms.StageAfterParser
}

func (g *Converter) Stats() utils.StatsInfo {
	return g.stats
}

func init() {
	transforms.Add("convert", func() transforms.Transformer {
		return &Converter{}
	})
}
