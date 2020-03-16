package mutate

import (
	"errors"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

const (
	KeySubStart = "start"
	KeySubEnd   = "end"
)

var (
	_ transforms.StatsTransformer = &Sub{}
	_ transforms.Transformer      = &Sub{}
	_ transforms.Initializer      = &Sub{}

	OptionSubStart = Option{
		KeyName:      KeySubStart,
		ChooseOnly:   false,
		Required:     false,
		Default:      0,
		Placeholder:  "0",
		Element:      InputNumber,
		DefaultNoUse: true,
		Description:  "字段提取的起始位置(start)",
		ToolTip:      "指定需要提取字段起始元素的位置（包含）默认为0。不支持负数",
		Type:         transforms.TransformTypeLong,
	}
	OptionSubEnd = Option{
		KeyName:      KeySubEnd,
		ChooseOnly:   false,
		Required:     false,
		Default:      1,
		Placeholder:  "1",
		Element:      InputNumber,
		DefaultNoUse: true,
		Description:  "字段提取的结束位置(end)",
		ToolTip:      "指定需要提取字段结束元素的位置（不包含）默认为1。不支持负数",
		Type:         transforms.TransformTypeLong,
	}
)

type Sub struct {
	Key    string `json:"key"`
	New    string `json:"new"`
	Start  int    `json:"start"`
	End    int    `json:"end"`
	CStage string `json:"stage"`
	stats  StatsInfo

	oldKeys []string
	newKeys []string

	numRoutine int
}

func (s *Sub) Init() error {
	if s.Start < 0 || s.End < 0 || s.Start >= s.End {
		return errors.New("transform[substring] invalid start or end index, please make sure start>=0, end>=0, start<end")
	}
	s.oldKeys = GetKeys(s.Key)
	if s.New == "" {
		s.newKeys = GetKeys(s.Key)
	} else {
		s.newKeys = GetKeys(s.New)
	}
	numRoutine := MaxProcs
	if numRoutine == 0 {
		numRoutine = 1
	}
	s.numRoutine = numRoutine
	return nil
}

func (s *Sub) Description() string {
	return `对于日志数据中的每条记录，对指定的键值进行定长字段提取。`
}

func (s *Sub) SampleConfig() string {
	return `{
       "type":"substring",
       "key":"my_field_keyname",
       "start":"0",
       "end":"10",
    }`
}

func (s *Sub) ConfigOptions() []Option {
	return []Option{
		transforms.KeyFieldName,
		transforms.KeyFieldNew,
		OptionSubStart,
		OptionSubEnd,
		transforms.KeyStage,
	}
}

func (s *Sub) Type() string {
	return "substring"
}

func (s *Sub) RawTransform(datas []string) ([]string, error) {
	if len(s.oldKeys) == 0 {
		if err := s.Init(); err != nil {
			return nil, err
		}
	}

	newVal := ""
	for idx, data := range datas {
		sLen := len(datas[idx])
		if s.Start >= sLen {
			newVal = data
		} else if s.End >= sLen {
			newVal = data[s.Start:]
		} else {
			newVal = data[s.Start:s.End]
		}
		datas[idx] = newVal
	}

	return datas, nil
}

func (s *Sub) Stage() string {
	if s.CStage == "" {
		return transforms.StageAfterParser
	}
	return s.CStage
}

func (s *Sub) Stats() StatsInfo {
	return s.stats
}

func (s *Sub) SetStats(err string) StatsInfo {
	s.stats.LastError = err
	return s.stats
}

func (s *Sub) Transform(datas []Data) ([]Data, error) {
	if len(s.oldKeys) == 0 {
		if err := s.Init(); err != nil {
			return nil, err
		}
	}
	var (
		errNum      = 0
		err, fmtErr error
		newVal      string
	)
	for idx := range datas {
		val, getErr := GetMapValue(datas[idx], s.oldKeys...)
		if getErr != nil {
			errNum, err = transforms.SetError(errNum, getErr, transforms.GetErr, s.Key)
			continue
		}
		strVal, ok := val.(string)
		if !ok {
			typeErr := errors.New("transform key " + s.Key + " data type is not string")
			errNum, err = transforms.SetError(errNum, typeErr, transforms.General, "")
			continue
		}
		sLen := len(strVal)
		if s.Start >= sLen {
			newVal = ""
		} else if s.End >= sLen {
			newVal = strVal[s.Start:]
		} else {
			newVal = strVal[s.Start:s.End]
		}
		setErr := SetMapValue(datas[idx], newVal, false, s.newKeys...)
		if setErr != nil {
			errNum, err = transforms.SetError(errNum, getErr, transforms.SetErr, s.Key)
			continue
		}
	}

	s.stats, fmtErr = transforms.SetStatsInfo(err, s.stats, int64(errNum), int64(len(datas)), s.Type())
	return datas, fmtErr
}

func init() {
	transforms.Add("substring", func() transforms.Transformer {
		return &Sub{}
	})
}
