package parser

import (
	"strconv"
	"strings"
	"time"

	"github.com/qiniu/log"
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

type schemaErr struct {
	number int
	last   time.Time
}

func (s *schemaErr) Output(err error) {
	s.number++
	if time.Now().Sub(s.last) > 3*time.Second {
		log.Errorf("%v parse line errors occured, same as %v", s.number, err)
		s.number = 0
		s.last = time.Now()
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
