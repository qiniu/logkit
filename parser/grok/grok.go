package grok

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/vjeantet/grok"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	. "github.com/qiniu/logkit/parser/config"
	"github.com/qiniu/logkit/times"
	. "github.com/qiniu/logkit/utils/models"
)

const (
	ModeMulti = "multi"
)

const MaxGrokMultiLineBuffer = 64 * 1024 * 1024 // 64MB

func init() {
	parser.RegisterConstructor(TypeGrok, NewParser)
}

type Parser struct {
	name                 string
	labels               []GrokLabel
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

	// patterns is a map of all of the parsed patterns from CustomPatterns
	// and CustomPatternFiles.
	//   ie, {
	//          "DURATION":      "%{NUMBER}[nuµm]?s"
	//          "RESPONSE_CODE": "%{NUMBER:rc:date}"
	//       }
	patterns map[string]string
	g        *grok.Grok

	numRoutine  int
	keepRawData bool
}

func NewParser(c conf.MapConf) (parser.Parser, error) {
	name, _ := c.GetStringOr(KeyParserName, "")
	patterns, err := c.GetStringList(KeyGrokPatterns)
	if err != nil {
		return nil, errors.New("parse key " + KeyGrokPatterns + " error " + err.Error())
	}
	mode, _ := c.GetStringOr(KeyGrokMode, "")
	labelList, _ := c.GetStringListOr(KeyLabels, []string{})
	timeZoneOffsetRaw, _ := c.GetStringOr(KeyTimeZoneOffset, "")
	timeZoneOffset := ParseTimeZoneOffset(timeZoneOffsetRaw)
	nameMap := make(map[string]struct{})
	labels := GetGrokLabels(labelList, nameMap)

	customPatterns, _ := c.GetStringOr(KeyGrokCustomPatterns, "")
	customPatternFiles, _ := c.GetStringListOr(KeyGrokCustomPatternFiles, []string{})

	disableRecordErrData, _ := c.GetBoolOr(KeyDisableRecordErrData, false)
	keepRawData, _ := c.GetBoolOr(KeyKeepRawData, false)

	numRoutine := MaxProcs
	if numRoutine == 0 {
		numRoutine = 1
	}

	p := &Parser{
		name:                 name,
		labels:               labels,
		mode:                 mode,
		Patterns:             patterns,
		CustomPatterns:       customPatterns,
		CustomPatternFiles:   customPatternFiles,
		timeZoneOffset:       timeZoneOffset,
		disableRecordErrData: disableRecordErrData,
		numRoutine:           numRoutine,
		keepRawData:          keepRawData,
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

func (p *Parser) Name() string {
	return p.name
}

func (p *Parser) Type() string {
	return TypeGrok
}

func (p *Parser) Parse(lines []string) ([]Data, error) {
	var (
		linesLen   = len(lines)
		datas      = make([]Data, linesLen)
		se         = &StatsError{}
		numRoutine = p.numRoutine

		sendChan   = make(chan parser.ParseInfo)
		resultChan = make(chan parser.ParseResult)
		wg         = new(sync.WaitGroup)
	)
	if linesLen < numRoutine {
		numRoutine = linesLen
	}

	for i := 0; i < numRoutine; i++ {
		wg.Add(1)
		go parser.ParseLine(sendChan, resultChan, wg, true, p.parse)
	}

	go func() {
		wg.Wait()
		close(resultChan)
	}()

	go func() {
		for idx, line := range lines {
			sendChan <- parser.ParseInfo{
				Line:  line,
				Index: idx,
			}
		}
		close(sendChan)
	}()

	var parseResultSlice = make(parser.ParseResultSlice, linesLen)
	for resultInfo := range resultChan {
		parseResultSlice[resultInfo.Index] = resultInfo
	}

	se.DatasourceSkipIndex = make([]int, linesLen)
	var datasourceIndex = 0
	var dataIndex = 0
	for _, parseResult := range parseResultSlice {
		if len(parseResult.Line) == 0 {
			se.DatasourceSkipIndex[datasourceIndex] = parseResult.Index
			datasourceIndex++
			continue
		}

		if parseResult.Err != nil {
			se.AddErrors()
			se.LastError = parseResult.Err.Error()
			errData := make(Data)
			if !p.disableRecordErrData {
				errData[KeyPandoraStash] = parseResult.Line
			} else if !p.keepRawData {
				se.DatasourceSkipIndex[datasourceIndex] = parseResult.Index
				datasourceIndex++
			}
			if p.keepRawData {
				errData[KeyRawData] = parseResult.Line
			}
			if !p.disableRecordErrData || p.keepRawData {
				datas[dataIndex] = errData
				dataIndex++
			}
			continue
		}
		if len(parseResult.Data) < 1 { //数据为空时候不发送
			continue
		}
		log.Debugf("D! parse result(%v)", parseResult.Data)
		se.AddSuccess()
		if p.keepRawData {
			parseResult.Data[KeyRawData] = parseResult.Line
		}
		datas[dataIndex] = parseResult.Data
		dataIndex++
	}

	datas = datas[:dataIndex]
	se.DatasourceSkipIndex = se.DatasourceSkipIndex[:datasourceIndex]
	if se.Errors == 0 && len(se.DatasourceSkipIndex) == 0 {
		return datas, nil
	}
	return datas, se
}

func (p *Parser) parse(line string) (Data, error) {
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
		err = fmt.Errorf("%v no value was parsed after grok pattern %v", TruncateStrSize(line, DefaultTruncateMaxSize), p.Patterns)
		log.Errorf("Parser[%v]: error %v", p.name, err)
		return nil, err
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
			if strings.HasPrefix(v, `"`) && strings.HasSuffix(v, `"`) {
				v = strings.Trim(v, `"`)
			}
			data[k] = strings.TrimSpace(v)
		}
	}

	if len(data) <= 0 {
		return data, errors.New("all data was ignored in this line? Check WARN log and fix your grok pattern")
	}

	for _, l := range p.labels {
		if _, ok := data[l.Name]; ok {
			continue
		}
		data[l.Name] = l.Value
	}
	return data, nil
}

func (p *Parser) addCustomPatterns(scanner *bufio.Scanner) error {
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		line = TrimInvalidSpace(line)
		if len(line) > 0 && line[0] != '#' {
			names := strings.SplitN(line, " ", 2)
			if len(names) < 2 {
				return errors.New("the pattern " + line + " is invalid, and has been ignored")
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
			subNames := PatternOnlyRe.FindAllStringSubmatch(pattern, -1)
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
		if ModifierRe.MatchString(pattern) {
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

// parseTypedCaptures parses the capture modifiers, and then deletes the
// modifier from the line so that it is a valid "grok" pattern again.
func (p *Parser) parseTypedCaptures(name, pattern string) (string, error) {
	matches := ModifierRe.FindAllStringSubmatch(pattern, -1)

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
