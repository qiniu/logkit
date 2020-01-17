package mutate

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode"

	"github.com/qiniu/logkit/utils/models"
)

const (
	errMsg = "will keep origin data in pandora_stash if disable_record_errdata field is false"
)

type Parser struct {
	KeepString bool
	Splitter   string
}

func (p *Parser) Parse(line string) ([]models.Data, error) {

	pairs, err := splitKV(line, p.Splitter)
	if err != nil {
		return nil, err
	}

	// 调整数据类型
	if len(pairs)%2 == 1 {
		return nil, errors.New(fmt.Sprintf("key value not match, %s", errMsg))
	}

	data := make([]models.Data, 0, 1)
	field := make(models.Data)
	for i := 0; i < len(pairs); i += 2 {
		// 消除双引号； 针对foo="" ,"foo=" 情况；其他情况如 a"b"c=d"e"f等首尾不出现引号的情况视作合法。
		kNum := strings.Count(pairs[i], "\"")
		vNum := strings.Count(pairs[i+1], "\"")
		if kNum%2 == 1 && vNum%2 == 1 {
			if strings.HasPrefix(pairs[i], "\"") && strings.HasSuffix(pairs[i+1], "\"") {
				pairs[i] = pairs[i][1:]
				pairs[i+1] = pairs[i+1][:len(pairs[i+1])-1]
			}
		}
		if kNum%2 == 0 && len(pairs[i]) > 1 {
			if strings.HasPrefix(pairs[i], "\"") && strings.HasSuffix(pairs[i], "\"") {
				pairs[i] = pairs[i][1 : len(pairs[i])-1]
			}
		}
		if vNum%2 == 0 && len(pairs[i+1]) > 1 {
			if strings.HasPrefix(pairs[i+1], "\"") && strings.HasSuffix(pairs[i+1], "\"") {
				pairs[i+1] = pairs[i+1][1 : len(pairs[i+1])-1]
			}
		}

		if len(pairs[i]) == 0 || len(pairs[i+1]) == 0 {
			return nil, fmt.Errorf("no value or key was parsed after logfmt, %s", errMsg)
		}

		value := pairs[i+1]
		if !p.KeepString {
			if fValue, err := strconv.ParseFloat(value, 64); err == nil {
				field[pairs[i]] = fValue
				continue
			}
			if bValue, err := strconv.ParseBool(value); err == nil {
				field[pairs[i]] = bValue
				continue
			}

		}
		field[pairs[i]] = value
	}
	if len(field) == 0 {
		return nil, fmt.Errorf("data is empty after parse, %s", errMsg)
	}

	data = append(data, field)
	return data, nil
}

func splitKV(line string, sep string) ([]string, error) {
	data := make([]string, 0, 100)

	if !strings.Contains(line, sep) {
		return nil, errors.New(fmt.Sprintf("no splitter exist, %s", errMsg))
	}

	kvArr := make([]string, 0, 100)
	isKey := true
	vhead := 0
	lastSpace := 0
	pos := 0
	sepLen := len(sep)

	// key或value值中包含sep的情况；默认key中不包含sep；导致algorithm = 1+1=2会变成合法
	for pos+sepLen <= len(line) {
		if unicode.IsSpace(rune(line[pos : pos+1][0])) {
			nextSep := strings.Index(line[pos+1:], sep)
			if nextSep == -1 {
				break
			}
			if strings.TrimSpace(line[pos+1:pos+1+nextSep]) != "" {
				lastSpace = pos
				pos++
				continue
			}
		}
		if line[pos:pos+sepLen] == sep {
			if isKey {
				kvArr = append(kvArr, strings.TrimSpace(line[vhead:pos]))
				isKey = false
			} else {
				if lastSpace <= vhead {
					pos++
					continue
				}
				kvArr = append(kvArr, strings.TrimSpace(line[vhead:lastSpace]))
				kvArr = append(kvArr, strings.TrimSpace(line[lastSpace:pos]))
			}
			vhead = pos + sepLen
			pos = pos + sepLen - 1
		}
		pos++
	}
	if vhead < len(line) {
		kvArr = append(kvArr, strings.TrimSpace(line[vhead:]))
	}
	data = append(data, kvArr...)
	return data, nil
}
