package mutate

import (
	"fmt"
	"runtime/debug"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/qiniu/log"
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
			return nil, fmt.Errorf("key was empty after parse, %s", errMsg)
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
	// 延迟处理的函数
	defer func() {
		// 发生宕机时，获取panic传递的上下文并打印
		if err := recover(); err != nil {
			log.Errorf("ScanValue PANIC:\n err:%s\n stack:%s\n key:%s\n line:%s\n sep:%s\n", err, debug.Stack(), d.key, d.line, sep)
		}
	}()

	if len(d.line) == 0 {
		return false
	}
	if d.sepPos == 0 {
		d.sepPos = getSepPos(d.line, sep)
	}
	if d.sepPos <= 0 || d.sepPos >= len(d.line) {
		return false
	}
	d.key = strings.TrimSpace(d.line[:d.sepPos])
	firstSpace := getSpacePos(d.line[d.sepPos:], 1)
	if firstSpace != -1 {
		nextSep := getSepPos(d.line[d.sepPos+firstSpace:], sep)
		// 找第二个key，key不能为空,两个sep之间必须有空格
		preSepPos := d.sepPos + len(sep)
		nextSepPos := d.sepPos + firstSpace + nextSep
		for nextSep != -1 {
			if strings.TrimFunc(d.line[preSepPos+len(sep):nextSepPos], unicode.IsSpace) != "" {
				break
			}
			preSepPos = nextSepPos + len(sep)
			nextSep = getSepPos(d.line[preSepPos:], sep)
			nextSepPos = preSepPos + nextSep
		}
		if nextSep != -1 {
			lastSpace := getSpacePos(strings.TrimRightFunc(d.line[d.sepPos+len(sep):nextSepPos], unicode.IsSpace), -1)
			if lastSpace != -1 {
				d.value = strings.TrimSpace(d.line[d.sepPos+len(sep) : d.sepPos+len(sep)+lastSpace])
				d.line = d.line[d.sepPos+len(sep)+lastSpace+1:]
				d.sepPos = nextSepPos - d.sepPos - len(sep) - lastSpace - 1
				return true
			}
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

func getSepPos(line, sep string) int {
	quoterCount := 0
	for i, s := range line {
		if i+len(sep) > len(line) {
			break
		}
		if s == '"' {
			quoterCount++
		} else if line[i:i+len(sep)] == sep && quoterCount%2 == 0 {
			return i
		}
	}
	return strings.LastIndex(line, sep)
}

// direction=1 get first space position
// direction=-1 get last space position
func getSpacePos(line string, direction int) int {
	quoterCount := 0
	if direction > 0 {
		for i, r := range line {
			if r == '"' {
				quoterCount++
			} else if unicode.IsSpace(r) && quoterCount%2 == 0 {
				return i
			}
		}
	} else {
		for i := len(line); i > 0; {
			r, size := utf8.DecodeLastRuneInString(line[0:i])
			i -= size
			if r == '"' {
				quoterCount++
			} else if unicode.IsSpace(r) && quoterCount%2 == 0 {
				return i
			}
		}
	}
	return -1
}
