package grok

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/vjeantet/grok"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	"github.com/qiniu/logkit/times"
	. "github.com/qiniu/logkit/utils/models"
)

const (
	ModeMulti = "multi"
)

const (
	LONG   = "long"
	FLOAT  = "float"
	STRING = "string"
	DATE   = "date"
	DROP   = "drop"
)

const MaxGrokMultiLineBuffer = 64 * 1024 * 1024 // 64MB

var (
	// matches named captures that contain a modifier.
	//   ie,
	//     %{NUMBER:bytes:long}
	//     %{IPORHOST:clientip:date}
	//     %{HTTPDATE:ts1:float}
	modifierRe = regexp.MustCompile(`%{\w+:(\w+):(long|string|date|float|drop)}`)
	// matches a plain pattern name. ie, %{NUMBER}
	patternOnlyRe = regexp.MustCompile(`%{(\w+)}`)
)

func init() {
	parser.RegisterConstructor(parser.TypeGrok, NewParser)
}

type Parser struct {
	name                 string
	labels               []parser.Label
	mode                 string
	disableRecordErrData bool

	timeZoneOffset int

	Patterns []string // 正式的pattern名称
	// namedPatterns is a list of internally-assigned names to the patterns
	// specified by the user in Patterns.
	// They will look like:
	//   GROK_INTERNAL_PATTERN_0, GROK_INTERNAL_PATTERN_1, etc.
	namedPatterns      []string
	CustomPatterns     string
	CustomPatternFiles []string

	// typeMap is a map of patterns -> 字段名 -> 类型,
	//   ie, {
	//          "%{TESTLOG}":
	//             {
	//                "bytes": "long",
	//                "clientip": "string"
	//             }
	//       }
	typeMap map[string]map[string]string

	// patterns is a map of builtin of the parsed patterns from CustomPatterns
	// and CustomPatternFiles.
	//   ie, {
	//          "DURATION":      "%{NUMBER}[nuµm]?s"
	//          "RESPONSE_CODE": "%{NUMBER:rc:date}"
	//       }
	patterns map[string]string
	g        *grok.Grok
}

func NewParser(c conf.MapConf) (parser.Parser, error) {
	name, _ := c.GetStringOr(parser.KeyParserName, "")
	patterns, err := c.GetStringList(parser.KeyGrokPatterns)
	if err != nil {
		return nil, fmt.Errorf("parse key %v error %v", parser.KeyGrokPatterns, err)
	}
	mode, _ := c.GetStringOr(parser.KeyGrokMode, "")
	labelList, _ := c.GetStringListOr(parser.KeyLabels, []string{})
	timeZoneOffsetRaw, _ := c.GetStringOr(parser.KeyTimeZoneOffset, "")
	timeZoneOffset := parser.ParseTimeZoneOffset(timeZoneOffsetRaw)
	nameMap := make(map[string]struct{})
	labels := parser.GetLabels(labelList, nameMap)

	customPatterns, _ := c.GetStringOr(parser.KeyGrokCustomPatterns, "")
	customPatternFiles, _ := c.GetStringListOr(parser.KeyGrokCustomPatternFiles, []string{})

	disableRecordErrData, _ := c.GetBoolOr(parser.KeyDisableRecordErrData, false)

	p := &Parser{
		name:                 name,
		labels:               labels,
		mode:                 mode,
		Patterns:             patterns,
		CustomPatterns:       customPatterns,
		CustomPatternFiles:   customPatternFiles,
		timeZoneOffset:       timeZoneOffset,
		disableRecordErrData: disableRecordErrData,
	}
	err = p.compile()
	if err != nil {
		return nil, err
	}
	return p, nil
}

func (p *Parser) compile() error {
	p.typeMap = make(map[string]map[string]string)
	p.patterns = make(map[string]string)
	gk, err := grok.NewWithConfig(&grok.Config{NamedCapturesOnly: true, RemoveEmptyValues: true})
	if err != nil {
		return err
	}
	p.g = gk

	// Give Patterns fake names so that they can be treated as named
	// "custom patterns"
	p.namedPatterns = make([]string, len(p.Patterns))
	for i, pattern := range p.Patterns {
		name := fmt.Sprintf("GROK_INTERNAL_PATTERN_%d", i)
		p.CustomPatterns += "\n" + name + " " + pattern + "\n"
		p.namedPatterns[i] = "%{" + name + "}"
	}

	// Combine user-supplied CustomPatterns with DEFAULT_PATTERNS and parse
	// them together as the same type of pattern.
	p.CustomPatterns = DEFAULT_PATTERNS + p.CustomPatterns
	if len(p.CustomPatterns) != 0 {
		scanner := bufio.NewScanner(strings.NewReader(p.CustomPatterns))
		err := p.addCustomPatterns(scanner)
		if err != nil {
			return err
		}
	}

	// Parse any custom pattern files supplied.
	for _, filename := range p.CustomPatternFiles {
		file, err := os.Open(filename)
		if err != nil {
			return err
		}

		scanner := bufio.NewScanner(bufio.NewReader(file))
		err = p.addCustomPatterns(scanner)
		if err != nil {
			return err
		}
	}

	return p.compileCustomPatterns()
}

func (gp *Parser) Name() string {
	return gp.name
}

func (gp *Parser) Type() string {
	return parser.TypeGrok
}

func (gp *Parser) Parse(lines []string) ([]Data, error) {
	datas := []Data{}
	se := &StatsError{}
	for idx, line := range lines {
		//grok不应该踢出掉空格，因为grok的Pattern可能按照空格来配置，只需要判断是不是全空扔掉。
		if len(strings.TrimSpace(line)) <= 0 {
			se.DatasourceSkipIndex = append(se.DatasourceSkipIndex, idx)
			continue
		}
		data, err := gp.parseLine(line)
		if err != nil {
			se.AddErrors()
			se.ErrorDetail = err
			if !gp.disableRecordErrData {
				errData := make(Data)
				errData[KeyPandoraStash] = line
				datas = append(datas, errData)
			} else {
				se.DatasourceSkipIndex = append(se.DatasourceSkipIndex, idx)
			}
			continue
		}
		if len(data) < 1 { //数据不为空的时候发送
			continue
		}
		log.Debugf("D! parse result(%v)", data)
		datas = append(datas, data)
		se.AddSuccess()
	}
	return datas, se
}

func (p *Parser) parseLine(line string) (Data, error) {
	if p.mode == ModeMulti {
		line = strings.Replace(line, "\n", " ", -1)
	}
	var err error
	var values map[string]string
	var patternName string
	for _, pattern := range p.namedPatterns {
		if values, err = p.g.Parse(pattern, line); err != nil {
			log.Debugf("E! %v", err)
			return nil, err
		}
		//此处匹配到就break的好处时匹配结果唯一，若要改为不break，那要考虑如果有多个串同时满足时，结果如何选取的问题，应该考虑优先选择匹配的结果多的数据。
		if len(values) != 0 {
			patternName = pattern
			break
		}
	}
	if len(values) < 1 {
		log.Errorf("%v no value was parsed after grok pattern %v", line, p.Patterns)
		return nil, fmt.Errorf("%v no value was parsed after grok pattern %v", line, p.Patterns)
	}
	data := Data{}
	for k, v := range values {
		if k == "" || v == "" {
			continue
		}

		// t is the modifier of the field
		var t string
		// check if pattern has some modifiers
		if types, ok := p.typeMap[patternName]; ok {
			t = types[k]
		}

		// if we didn't find a type OR timestamp modifier, assume string
		if t == "" {
			t = STRING
		}

		switch t {
		case LONG:
			iv, err := strconv.ParseInt(v, 10, 64)
			if err != nil {
				log.Warnf("E! Error parsing %s to long: %s, ignore this field...", v, err)
			} else {
				data[k] = iv
			}
		case FLOAT:
			fv, err := strconv.ParseFloat(v, 64)
			if err != nil {
				log.Warnf("E! Error parsing %s to float: %s, ignore this field...", v, err)
			} else {
				data[k] = fv
			}
		case DATE:
			ts, err := times.StrToTime(v)
			if err == nil {
				ts = ts.Add(time.Duration(p.timeZoneOffset) * time.Hour)
				rfctime := ts.Format(time.RFC3339Nano)
				data[k] = rfctime
			} else {
				log.Warnf("E! Error parsing %s to time layout [%s]: %s, ignore this field...", v, t, err)
			}

		case DROP:
		// goodbye!
		default: //default is STRING
			data[k] = strings.TrimSpace(strings.Trim(v, `"`))
		}
	}

	if len(data) <= 0 {
		return data, fmt.Errorf("builtin data was ignored in this line? Check WARN log and fix your grok pattern")
	}

	for _, l := range p.labels {
		data[l.Name] = l.Value
	}
	return data, nil
}

func (p *Parser) addCustomPatterns(scanner *bufio.Scanner) error {
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		line = trimInvalidSpace(line)
		if len(line) > 0 && line[0] != '#' {
			names := strings.SplitN(line, " ", 2)
			if len(names) < 2 {
				return fmt.Errorf("the pattern %v is invalid, and has been ignored", line)
			}
			p.patterns[names[0]] = names[1]
		}
	}
	return nil
}

func (p *Parser) compileCustomPatterns() error {
	var err error
	// check if the pattern contains a subpattern that is already defined
	// replace it with the subpattern for modifier inheritance.
	for i := 0; i < 2; i++ {
		for name, pattern := range p.patterns {
			subNames := patternOnlyRe.FindAllStringSubmatch(pattern, -1)
			for _, subName := range subNames {
				if subPattern, ok := p.patterns[subName[1]]; ok {
					pattern = strings.Replace(pattern, subName[0], subPattern, 1)
				}
			}
			p.patterns[name] = pattern
		}
	}

	// check if pattern contains modifiers. Parse them out if it does.
	for name, pattern := range p.patterns {
		if modifierRe.MatchString(pattern) {
			// this pattern has modifiers, so parse out the modifiers
			pattern, err = p.parseTypedCaptures(name, pattern)
			if err != nil {
				return err
			}
			p.patterns[name] = pattern
		}
	}

	return p.g.AddPatternsFromMap(p.patterns)
}

func trimInvalidSpace(pattern string) string {
	reg := regexp.MustCompile(`%{((.*?:)*?.*?)}`)
	substringIndex := reg.FindAllStringSubmatchIndex(pattern, -1)
	curIndex := 0
	var clearString string = ""
	for _, val := range substringIndex {
		if curIndex < val[2] {
			clearString += pattern[curIndex:val[2]]
		}
		subString := pattern[val[2]:val[3]]
		subStringSlice := strings.Split(subString, ":")
		subLen := len(subStringSlice)
		for index, chr := range subStringSlice {
			clearString += strings.TrimSpace(chr)
			if index != subLen-1 {
				clearString += ":"
			} else {
				clearString += "}"
			}
		}
		curIndex = val[3] + 1
	}
	if curIndex < len(pattern) {
		clearString += pattern[curIndex:]
	}
	return clearString
}

// parseTypedCaptures parses the capture modifiers, and then deletes the
// modifier from the line so that it is a valid "grok" pattern again.
func (p *Parser) parseTypedCaptures(name, pattern string) (string, error) {
	matches := modifierRe.FindAllStringSubmatch(pattern, -1)

	// grab the name of the capture pattern
	patternName := "%{" + name + "}"
	// create type map for this pattern
	p.typeMap[patternName] = make(map[string]string)

	// boolean to verify that each pattern only has a single ts- data type.
	for _, match := range matches {

		p.typeMap[patternName][match[1]] = match[2]

		// the modifier is not a valid part of a "grok" pattern, so remove it
		// from the pattern.
		pattern = strings.Replace(pattern, ":"+match[2]+"}", "}", 1)
	}

	return pattern, nil
}
