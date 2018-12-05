package parser

import (
	"strconv"
	"strings"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/conf"
	. "github.com/qiniu/logkit/parser/config"
	. "github.com/qiniu/logkit/utils/models"
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
