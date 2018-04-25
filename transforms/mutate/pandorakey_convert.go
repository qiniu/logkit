package mutate

import (
	"errors"
	"fmt"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

type PandoraKeyConvert struct {
	stats StatsInfo
}

func (g *PandoraKeyConvert) RawTransform(datas []string) ([]string, error) {
	return datas, errors.New("pandora_key_convert transformer not support rawTransform")
}

func (g *PandoraKeyConvert) Transform(datas []Data) ([]Data, error) {
	var err, ferr error
	errnums := 0
	for i, v := range datas {
		datas[i] = deepConvertKey(v)
	}
	if err != nil {
		g.stats.LastError = err.Error()
		ferr = fmt.Errorf("find total %v erorrs in transform pandora_key_convert, last error info is %v", errnums, err)
	}
	g.stats.Errors += int64(errnums)
	g.stats.Success += int64(len(datas) - errnums)
	return datas, ferr
}

func deepConvertKey(data map[string]interface{}) map[string]interface{} {
	newData := make(map[string]interface{})
	for k, v := range data {
		nk := PandoraKey(k)
		if nv, ok := v.(map[string]interface{}); ok {
			v = deepConvertKey(nv)
		}
		newData[nk] = v
	}
	return newData
}

func (g *PandoraKeyConvert) Description() string {
	//return "pandora_key_convert can convert data key name to valid pandora key"
	return "将数据中的key名称中不合Pandora字段名规则的字符转为下划线, 如 a.b/c 改为 a_b_c"
}

func (g *PandoraKeyConvert) Type() string {
	return "pandora_key_convert"
}

func (g *PandoraKeyConvert) SampleConfig() string {
	return `{
		"type":"pandora_key_convert"
	}`
}

func (g *PandoraKeyConvert) ConfigOptions() []Option {
	return []Option{}
}

func (g *PandoraKeyConvert) Stage() string {
	return transforms.StageAfterParser
}

func (g *PandoraKeyConvert) Stats() StatsInfo {
	return g.stats
}

func init() {
	transforms.Add("pandora_key_convert", func() transforms.Transformer {
		return &PandoraKeyConvert{}
	})
}
