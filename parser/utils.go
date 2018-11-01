package parser

import (
	"strconv"
	"strings"

	"github.com/qiniu/logkit/conf"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/qiniu/log"
	. "github.com/qiniu/logkit/parser/config"
)

const SECOND_PER_DAY = 24 * 60 * 60
const SECOND_PER_5MIN = 5 * 60

func Time1Day(t int64) string {
	return strconv.FormatInt(Time1DayInt(t), 10)
}

func Time1DayInt(t int64) int64 {
	return alignTime(t, SECOND_PER_DAY)
}

func Time5Min(t int64) string {
	return strconv.FormatInt(Time5MinInt(t), 10)
}

func Time5MinInt(t int64) int64 {
	return alignTime(t, SECOND_PER_5MIN)
}

func alignTime(t int64, base int64) int64 {
	return (t / base) * base
}

func newLabel(name, dataValue string) Label {
	return Label{
		Name:  name,
		Value: dataValue,
	}
}

func GetLabels(labelList []string, nameMap map[string]struct{}) (labels []Label) {
	labels = make([]Label, 0)
	for _, f := range labelList {
		parts := strings.Fields(f)
		if len(parts) < 2 {
			log.Errorf("label conf error: " + f + ", format should be \"labelName labelValue\", ignore this label...")
			continue
		}
		labelName, labelValue := parts[0], parts[1]
		if _, ok := nameMap[labelName]; ok {
			log.Errorf("label name %v was duplicated, ignore this lable <%v,%v>...", labelName, labelName, labelValue)
			continue
		}
		nameMap[labelName] = struct{}{}
		l := newLabel(labelName, labelValue)
		labels = append(labels, l)
	}
	return
}

func ConvertWebParserConfig(conf conf.MapConf) conf.MapConf {
	if conf == nil {
		return conf
	}

	rawCustomPatterns, _ := conf.GetStringOr(KeyGrokCustomPatterns, "")
	if rawCustomPatterns != "" {
		realCustomPatterns, err := DecodeString(rawCustomPatterns)
		if err != nil {
			log.Errorf("decode %v error: %v", rawCustomPatterns, err)
			return conf
		}
		conf[KeyGrokCustomPatterns] = string(realCustomPatterns)
	}

	splitter, _ := conf.GetStringOr(KeyCSVSplitter, "")
	if splitter != "" {
		splitter = strings.Replace(splitter, "\\t", "\t", -1)
		conf[KeyCSVSplitter] = splitter
	}

	return conf
}

func ParseTimeZoneOffset(zoneoffset string) (ret int) {
	zoneoffset = strings.TrimSpace(zoneoffset)
	if zoneoffset == "" {
		return
	}
	mi := false
	if strings.HasPrefix(zoneoffset, "-") {
		mi = true
	}
	zoneoffset = strings.Trim(zoneoffset, "+-")
	i, err := strconv.ParseInt(zoneoffset, 10, 64)
	if err != nil {
		log.Errorf("parse %v error %v, ignore zoneoffset...", zoneoffset, err)
		return
	}
	ret = int(i)
	if mi {
		ret = 0 - ret
	}
	return
}
