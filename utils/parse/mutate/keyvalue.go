package mutate

import (
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
	var (
		field   = make(models.Data)
		decoder = NewDecoder(line)
		key     string
		value   string
	)

	for decoder.ScanValue(p.Splitter) {
		// 消除双引号； 针对foo="" ,"foo=" 情况；其他情况如 a"b"c=d"e"f等首尾不出现引号的情况视作合法。
		key = decoder.key
		value = decoder.value
		kNum := strings.Count(key, "\"")
		vNum := strings.Count(value, "\"")
		if kNum%2 == 1 && vNum%2 == 1 {
			if strings.HasPrefix(key, "\"") && strings.HasSuffix(value, "\"") {
				key = key[1:]
				value = value[:len(value)-1]
			}
		}
		if kNum%2 == 0 && len(key) > 1 {
			if strings.HasPrefix(key, "\"") && strings.HasSuffix(key, "\"") {
				key = key[1 : len(key)-1]
			}
		}
		if vNum%2 == 0 && len(value) > 1 {
			if strings.HasPrefix(value, "\"") && strings.HasSuffix(value, "\"") {
				value = value[1 : len(value)-1]
			}
		}

		if len(key) == 0 {
			return nil, fmt.Errorf("no value or key was parsed after logfmt, %s", errMsg)
		}

		dValue := decoder.value
		if !p.KeepString {
			if fValue, err := strconv.ParseFloat(dValue, 64); err == nil {
				field[key] = fValue
				continue
			}
			if bValue, err := strconv.ParseBool(dValue); err == nil {
				field[key] = bValue
				continue
			}
		}
		field[key] = value
	}
	if len(field) == 0 {
		return nil, fmt.Errorf("data is empty after parse, %s", errMsg)
	}

	return []models.Data{field}, nil
}

func splitKV(line string, sep string) []string {

	kvArr := make([]string, 0)
	d := NewDecoder(line)
	for d.ScanValue(sep) {
		kvArr = append(kvArr, d.Key())
		kvArr = append(kvArr, d.Value())
	}
	return kvArr
}

type Decoder struct {
	line   string
	sepPos int
	key    string
	value  string
}

func NewDecoder(line string) *Decoder {
	return &Decoder{
		line: line,
	}
}

func (d *Decoder) ScanValue(sep string) bool {
	if len(d.line) == 0 {
		return false
	}
	if d.sepPos == 0 {
		d.sepPos = strings.Index(d.line, sep)
	}
	if d.sepPos == -1 {
		return false
	}
	d.key = strings.TrimSpace(d.line[:d.sepPos])
	firstSpace := strings.IndexFunc(d.line[d.sepPos:], unicode.IsSpace)
	if firstSpace != -1 {
		nextSep := strings.Index(d.line[d.sepPos+firstSpace:], sep)
		if nextSep != -1 {
			lastSpace := strings.LastIndexFunc(strings.TrimRightFunc(d.line[:d.sepPos+firstSpace+nextSep], unicode.IsSpace), unicode.IsSpace)
			d.value = strings.TrimSpace(d.line[d.sepPos+len(sep) : lastSpace])
			d.line = d.line[lastSpace+1:]
			d.sepPos = d.sepPos + firstSpace + nextSep - lastSpace - 1
			return true
		}
	}
	d.value = strings.TrimSpace(d.line[d.sepPos+len(sep):])
	d.line = ""
	return true
}

func (d *Decoder) Value() string {
	return d.value
}

func (d *Decoder) Key() string {
	return d.key
}
