package mutate

import (
	"fmt"
	"strings"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

const (
	ModeUpper = "upper"
	ModeLower = "lower"

	KeyCase = "key"
	KeyMode = "mode"
)

var (
	_ transforms.StatsTransformer = &Case{}
	_ transforms.Transformer      = &Case{}

	OptionCaseKey = Option{
		KeyName:      KeyCase,
		ChooseOnly:   false,
		Default:      "",
		Required:     true,
		Placeholder:  "my_field_keyname",
		DefaultNoUse: true,
		Description:  "需要进行大小写转换的键(" + KeyCase + ")",
		ToolTip:      "对该字段的值进行大小写转换",
		Type:         transforms.TransformTypeString,
	}
	OptionCaseMode = Option{
		KeyName:       KeyMode,
		ChooseOnly:    true,
		ChooseOptions: []interface{}{ModeUpper, ModeLower},
		Default:       ModeLower,
		Required:      true,
		DefaultNoUse:  false,
		Description:   "转换模式(" + KeyMode + ")",
	}
)

type Case struct {
	Mode   string `json:"mode"`
	Key    string `json:"key"`
	CStage string `json:"stage"`
	stats  StatsInfo
}

func (c *Case) Description() string {
	return `对于日志数据中的每条记录，进行大小写转换。`
}

func (c *Case) SampleConfig() string {
	return `{
       "type":"redis",
		"mode":"upper",
       "key":"myParseKey",
    }`
}

func (c *Case) ConfigOptions() []Option {
	return []Option{
		OptionCaseKey,
		OptionCaseMode,
		transforms.KeyStage,
	}
}

func (c *Case) Type() string {
	return "case"
}

func (c *Case) RawTransform(datas []string) ([]string, error) {
	var err, fmtErr error
	errNum := 0
	for i := range datas {
		strVal := datas[i]
		var newVal string
		switch c.Mode {
		case ModeUpper:
			newVal = strings.ToUpper(strVal)
		case ModeLower:
			newVal = strings.ToLower(strVal)
		default:
			newVal = strVal
			errNum, err = transforms.SetError(errNum, fmt.Errorf("case transformer not support this mode[%s]", c.Mode), transforms.General, "")
		}
		datas[i] = newVal
	}
	c.stats, fmtErr = transforms.SetStatsInfo(err, c.stats, int64(errNum), int64(len(datas)), c.Type())
	return datas, fmtErr
}

func (c *Case) Stage() string {
	return transforms.StageAfterParser
}

func (c *Case) Stats() StatsInfo {
	return c.stats
}

func (c *Case) SetStats(err string) StatsInfo {
	c.stats.LastError = err
	return c.stats
}

func (c *Case) Transform(datas []Data) ([]Data, error) {
	var err, fmtErr error
	errNum := 0
	keys := GetKeys(c.Key)
	for i := range datas {
		val, getErr := GetMapValue(datas[i], keys...)
		if getErr != nil {
			errNum, err = transforms.SetError(errNum, getErr, transforms.GetErr, c.Key)
			continue
		}
		strVal, ok := val.(string)
		if !ok {
			typeErr := fmt.Errorf("transform key %v data type is not string", c.Key)
			errNum, err = transforms.SetError(errNum, typeErr, transforms.General, "")
			continue
		}
		var newVal string
		switch c.Mode {
		case ModeUpper:
			newVal = strings.ToUpper(strVal)
		case ModeLower:
			newVal = strings.ToLower(strVal)
		default:
			newVal = strVal
			errNum, err = transforms.SetError(errNum, fmt.Errorf("case transformer not support this mode[%s]", c.Mode), transforms.General, "")
		}
		setErr := SetMapValue(datas[i], newVal, false, keys...)
		if setErr != nil {
			errNum, err = transforms.SetError(errNum, setErr, transforms.SetErr, c.Key)
		}
	}
	c.stats, fmtErr = transforms.SetStatsInfo(err, c.stats, int64(errNum), int64(len(datas)), c.Type())
	return datas, fmtErr
}

func init() {
	transforms.Add("case", func() transforms.Transformer {
		return &Case{}
	})
}
