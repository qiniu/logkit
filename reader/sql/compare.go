package sql

import (
	"strconv"
	"strings"
	"time"

	"github.com/qiniu/logkit/utils/models"
)

// 查看时间是否符合, min为true则取出来为小于等于，min为false则取出来大于等于
func CompareTime(target, match string, timeStart, timeEnd []int, min bool) (valid bool) {
	for idx, record := range timeStart {
		if record == -1 {
			continue
		}

		if len(target) < timeEnd[idx] || len(match) < timeEnd[idx] {
			return false
		}

		// 比较大小
		curStr := target[record:timeEnd[idx]]
		curInt, err := strconv.Atoi(curStr)
		if err != nil {
			return false
		}
		matchStr := match[record:timeEnd[idx]]
		matchInt, err := strconv.Atoi(matchStr)
		if err != nil {
			return false
		}

		// 小于
		if curInt < matchInt {
			return min
		}

		if curInt > matchInt {
			return !min
		}

		// 相等
		valid = true
	}

	return true
}

// 查看时间是否相等
func EqualTime(target, magicRet string, timeStart, timeEnd []int) (valid bool) {
	for idx, record := range timeStart {
		if record == -1 {
			continue
		}

		if len(target) < timeEnd[idx] {
			return false
		}

		// 比较大小
		curStr := target[record:timeEnd[idx]]
		curInt, err := strconv.Atoi(curStr)
		if err != nil {
			return false
		}
		matchStr := magicRet[record:timeEnd[idx]]
		matchInt, err := strconv.Atoi(matchStr)
		if err != nil {
			return false
		}

		// 等于
		if curInt != matchInt {
			return false
		}

		valid = true
	}

	return true
}

func CompareRemainStr(target, magicRemainStr, magicRet string, magicRemainIndex []int) bool {
	if len(magicRemainIndex) > 0 && len(target) < magicRemainIndex[len(magicRemainIndex)-1] {
		return false
	}
	targetRemainStr := GetRemainStr(target, magicRemainIndex)
	if len(targetRemainStr) < len(magicRemainStr) {
		return false
	}

	// magicRet 中如果含有通配符，则只需剩余字符匹配即可
	if targetRemainStr[:len(magicRemainStr)] != magicRemainStr {
		return false
	}

	// matchData中有通配符
	if strings.HasSuffix(magicRet, Wildcards) {
		return true
	}

	if len(target) > len(magicRet) {
		targetRemainStr += target[len(magicRet):]
		if targetRemainStr != magicRemainStr {
			return false
		}
	}

	return true
}

func GetRemainStr(origin string, magicRemainIndex []int) (remainStr string) {
	if len(magicRemainIndex)%2 != 0 {
		return origin
	}

	for idx := 0; idx < len(magicRemainIndex); {
		remainStr += origin[magicRemainIndex[idx]:magicRemainIndex[idx+1]]
		idx = idx + 2
	}

	return remainStr
}

//-1 代表不存在; 1 代表更大; 0 代表相等
func CompareWithStartTimeInt(data models.Data, timestampKey string, startTimeInt int64) (int, bool) {
	timeData, ok := GetTimeIntFromData(data, timestampKey)
	if !ok {
		//如果出现了数据中没有时间的，实际上已经不合法了，那就获取，宁愿重复不愿遗漏
		return 1, false
	}
	if timeData > startTimeInt {
		return 1, true
	}
	return 0, true
}

//-1 代表不存在; 1 代表更大; 0 代表相等
func CompareWithStartTime(data models.Data, timestampKey string, startTime time.Time) (int, bool) {
	timeData, ok := GetTimeFromData(data, timestampKey)
	if !ok {
		//如果出现了数据中没有时间的，实际上已经不合法了，那就获取，宁愿重复不愿遗漏
		return 1, false
	}
	if timeData.After(startTime) {
		return 1, true
	}
	return 0, true
}

//-1 代表不存在; 1 代表更大; 0 代表相等
func CompareWithStartTimeStr(data models.Data, timestampKey, startTimeStr string) (int, bool) {
	timeDataStr, ok := GetTimeStrFromData(data, timestampKey)
	if !ok {
		//如果出现了数据中没有时间的，实际上已经不合法了，那就获取，宁愿重复不愿遗漏
		return 1, false
	}
	if timeDataStr > startTimeStr {
		return 1, true
	}
	return 0, true
}
