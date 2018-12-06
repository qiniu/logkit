package csv

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	"github.com/json-iterator/go"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	. "github.com/qiniu/logkit/parser/config"
	"github.com/qiniu/logkit/times"
	. "github.com/qiniu/logkit/utils/models"
)

const MaxParserSchemaErrOutput = 5

var jsontool = jsoniter.Config{
	EscapeHTML:             true,
	UseNumber:              true,
	ValidateJsonRawMessage: true,
}.Froze()

type Parser struct {
	name                 string
	schema               []field
	labels               []GrokLabel
	delim                string
	isAutoRename         bool
	timeZoneOffset       int
	disableRecordErrData bool
	allowMoreName        string
	allmoreStartNUmber   int
	allowNotMatch        bool
	ignoreInvalid        bool
	numRoutine           int
	keepRawData          bool
}

type field struct {
	name       string
	dataType   DataType
	typeChange map[string]DataType
	allin      bool
}

func init() {
	parser.RegisterConstructor(TypeCSV, NewParser)
}

func NewParser(c conf.MapConf) (parser.Parser, error) {
	name, _ := c.GetStringOr(KeyParserName, "")
	splitter, _ := c.GetStringOr(KeyCSVSplitter, "\t")

	schema, err := c.GetString(KeyCSVSchema)
	if err != nil {
		return nil, err
	}
	timeZoneOffsetRaw, _ := c.GetStringOr(KeyTimeZoneOffset, "")
	timeZoneOffset := ParseTimeZoneOffset(timeZoneOffsetRaw)
	isAutoRename, _ := c.GetBoolOr(KeyCSVAutoRename, false)

	fieldList, err := parseSchemaFieldList(schema)
	if err != nil {
		return nil, err
	}
	fields, err := parseSchemaFields(fieldList)
	if err != nil {
		return nil, err
	}
	nameMap := map[string]struct{}{}
	for _, newField := range fields {
		_, exist := nameMap[newField.name]
		if exist {
			return nil, errors.New("column conf error: duplicated column " + newField.name)
		}
		nameMap[newField.name] = struct{}{}
	}
	labelList, _ := c.GetStringListOr(KeyLabels, []string{})
	if len(labelList) < 1 {
		labelList, _ = c.GetStringListOr(KeyCSVLabels, []string{}) //向前兼容老的配置
	}
	labels := GetGrokLabels(labelList, nameMap)

	disableRecordErrData, _ := c.GetBoolOr(KeyDisableRecordErrData, false)

	allowNotMatch, _ := c.GetBoolOr(KeyCSVAllowNoMatch, false)
	allowMoreName, _ := c.GetStringOr(KeyCSVAllowMore, "")
	if allowMoreName != "" {
		allowNotMatch = true
	}
	allmoreStartNumber, _ := c.GetIntOr(KeyCSVAllowMoreStartNum, 0)
	ignoreInvalid, _ := c.GetBoolOr(KeyCSVIgnoreInvalidField, false)
	keepRawData, _ := c.GetBoolOr(KeyKeepRawData, false)
	numRoutine := MaxProcs
	if numRoutine == 0 {
		numRoutine = 1
	}
	return &Parser{
		name:                 name,
		schema:               fields,
		labels:               labels,
		delim:                splitter,
		isAutoRename:         isAutoRename,
		timeZoneOffset:       timeZoneOffset,
		disableRecordErrData: disableRecordErrData,
		allowNotMatch:        allowNotMatch,
		allowMoreName:        allowMoreName,
		ignoreInvalid:        ignoreInvalid,
		allmoreStartNUmber:   allmoreStartNumber,
		numRoutine:           numRoutine,
		keepRawData:          keepRawData,
	}, nil
}

func parseSchemaFieldList(schema string) (fieldList []string, err error) {
	fieldList = make([]string, 0)
	schema = strings.TrimSpace(schema)
	start := 0
	nestedDepth := 0
	for end, c := range schema {
		switch c {
		case '{':
			nestedDepth++
			if nestedDepth > 1 {
				err = errors.New("parse fieldList error: jsonmap in jsonmap is not supported by now")
				return
			}
		case '}':
			nestedDepth--
			if nestedDepth < 0 {
				err = errors.New("parse fieldList error: find } befor {")
				return
			}
		case ',':
			if nestedDepth == 0 {
				if start > end {
					err = errors.New("parse fieldList error: start index is larger than end")
					return
				}
				field := strings.TrimSpace(schema[start:end])
				fieldList = append(fieldList, field)
				start = end + 1
			}
		}
	}
	if nestedDepth != 0 {
		err = errors.New("parse fieldList error: { and } not match")
		return
	}
	if start < len(schema) {
		fieldList = append(fieldList, strings.TrimSpace(schema[start:]))
	}
	return
}

func parseSchemaRawField(f string) (newField field, err error) {
	parts := strings.Fields(f)
	if len(parts) < 2 {
		err = errors.New("column conf error: " + f + ", format should be \"columnName dataType\"")
		return
	}
	columnName, dataType := parts[0], parts[1]
	switch strings.ToLower(dataType) {
	case "s", "string":
		dataType = "string"
	case "f", "float":
		dataType = "float"
	case "l", "long":
		dataType = "long"
	case "d", "date":
		dataType = "date"
	}
	return newCsvField(columnName, DataType(dataType))
}

func parseSchemaJsonField(f string) (fd field, err error) {
	splitSpace := strings.IndexByte(f, ' ')
	key := f[:splitSpace]
	rawfield := strings.TrimSpace(f[splitSpace:])
	rawfield = strings.TrimSpace(rawfield[len(TypeJSONMap):])
	allin := true
	fields := make([]field, 0)
	typeChange := make(map[string]DataType)
	if len(rawfield) > 0 {
		allin = false
		if !strings.HasPrefix(rawfield, "{") || !strings.HasSuffix(rawfield, "}") {
			err = fmt.Errorf("%v didn't use {key valuetype} to specify jsonmap field", f)
			return
		}
		rawfield := strings.TrimSpace(rawfield[1 : len(rawfield)-1])
		if strings.HasSuffix(rawfield, "...") {
			allin = true
			rawfield = strings.TrimSuffix(rawfield, "...")
		}
		fieldList := strings.Split(rawfield, ",")
		fields, err = parseSchemaFields(fieldList)
		if err != nil {
			return
		}
		for _, f := range fields {
			typeChange[f.name] = DataType(f.dataType)
		}
	}
	fd = field{
		name:       key,
		dataType:   TypeJSONMap,
		typeChange: typeChange,
		allin:      allin,
	}
	return
}

func isJsonMap(f string) bool {
	if len(f) <= 0 {
		return false
	}
	spaceIndex := strings.IndexByte(f, ' ')
	if spaceIndex < 0 {
		return false
	}
	rawfield := strings.TrimSpace(f[spaceIndex:])
	return strings.HasPrefix(rawfield, string(TypeJSONMap))
}

func parseSchemaFields(fieldList []string) ([]field, error) {
	var (
		fields     = make([]field, len(fieldList))
		fieldIndex = 0
		err        error
	)

	for _, f := range fieldList {
		f = strings.TrimSpace(f)
		fi := field{}
		if isJsonMap(f) {
			fi, err = parseSchemaJsonField(f)
		} else {
			if len(f) <= 0 {
				continue
			}
			fi, err = parseSchemaRawField(f)
		}
		if err != nil {
			return nil, err
		}
		fields[fieldIndex] = fi
		fieldIndex++
	}
	return fields[:fieldIndex], nil
}

func dataTypeNotSupperted(dataType DataType) error {
	return errors.New("type not supported " + string(dataType) + " csv parser currently support string long float date jsonmap 5 types")
}

func newCsvField(name string, dataType DataType) (field, error) {
	switch dataType {
	case TypeFloat, TypeLong, TypeString, TypeDate:
		return field{
			name:     name,
			dataType: dataType,
		}, nil
	default:
		return field{}, dataTypeNotSupperted(dataType)
	}
}

func (f field) MakeValue(raw string, timeZoneOffset int) (interface{}, error) {
	return makeValue(raw, f.dataType, timeZoneOffset)
}

func makeValue(raw string, valueType DataType, timeZoneOffset int) (interface{}, error) {
	switch valueType {
	case TypeFloat:
		if raw == "" {
			return 0.0, nil
		}
		return strconv.ParseFloat(raw, 64)
	case TypeLong:
		if raw == "" {
			return 0, nil
		}
		return strconv.ParseInt(raw, 10, 64)
	case TypeDate:
		if raw == "" {
			return time.Now(), nil
		}
		ts, err := times.StrToTime(raw)
		if err == nil {
			return ts.Add(time.Duration(timeZoneOffset) * time.Hour).Format(time.RFC3339Nano), nil
		}
		return ts, err
	case TypeString:
		return raw, nil
	default:
		// 不应该走到这个分支上
		return nil, dataTypeNotSupperted(valueType)
	}
}

func checkValue(v interface{}) (interface{}, error) {
	switch x := v.(type) {
	case int, int64, float64, string:
		return x, nil
	default:
		vtype := reflect.TypeOf(v)
		if vtype != nil {
			return nil, dataTypeNotSupperted(DataType(vtype.Name()))
		}
		return nil, dataTypeNotSupperted(DataType("null"))
	}
}

func convertValue(v interface{}, valueType DataType) (interface{}, error) {
	value := fmt.Sprintf("%v", v)
	switch valueType {
	case TypeFloat:
		return strconv.ParseFloat(value, 64)
	case TypeLong:
		return strconv.ParseInt(value, 10, 64)
	case TypeString:
		return value, nil
	default:
		vtype := reflect.TypeOf(v)
		if vtype != nil {
			return nil, dataTypeNotSupperted(DataType(vtype.Name()))
		}
		return nil, dataTypeNotSupperted(DataType("null"))
	}
}

func (f field) ValueParse(value string, timeZoneOffset int) (Data, error) {
	if f.dataType != TypeString {
		value = strings.TrimSpace(value)
	}
	datas := Data{}
	switch f.dataType {
	case TypeJSONMap:
		if value == "" {
			return Data{}, nil
		}
		m := make(map[string]interface{})
		if err := jsontool.Unmarshal([]byte(value), &m); err != nil {
			return Data{}, errors.New("unmarshal json map type error: " + err.Error())
		}
		for k, v := range m {
			if v == nil {
				continue
			}
			key := f.name + "-" + k
			valueType, ok := f.typeChange[k]
			if ok {
				nv, err := convertValue(v, valueType)
				if err != nil {
					return datas, fmt.Errorf("data type exchange error cannot make %v to %v %v", v, valueType, err)
				}
				datas[key] = nv
				continue
			}
			if f.allin {
				nv, err := checkValue(v)
				if err != nil {
					nv = v
				}
				datas[key] = nv
			}
		}
	default:
		v, err := f.MakeValue(value, timeZoneOffset)
		if err != nil {
			return Data{}, err
		}
		datas[f.name] = v
	}
	return datas, nil
}

func (p *Parser) Name() string {
	return p.name
}

func (p *Parser) Type() string {
	return TypeCSV
}

func getUnmachedMessage(parts []string, schemas []field) (ret string) {
	length := len(parts)
	if length > len(schemas) {
		length = len(schemas)
	}
	ret = "matched: "
	for i := 0; i < length; i++ {
		ret += "[" + schemas[i].name + "]=>[" + parts[i] + "],"
	}
	ret += "  unmatched "
	if length < len(parts) {
		ret += "log: "
		for i := length; i < len(parts); i++ {
			ret += "[" + parts[i] + "]"
		}
	} else {
		ret += "schema: "
		for i := length; i < len(schemas); i++ {
			ret += "[" + schemas[i].name + "]"
		}
	}
	return
}

func (p *Parser) parse(line string) (d Data, err error) {
	d = make(Data)
	parts := strings.Split(line, p.delim)
	if len(parts) != len(p.schema) && !p.allowNotMatch {
		return nil, fmt.Errorf("schema length not match: schema length %v, actual column length %v, %s", len(p.schema), len(parts), getUnmachedMessage(parts, p.schema))
	}
	moreNum := p.allmoreStartNUmber
	for i, part := range parts {
		part = strings.TrimSpace(part)
		if i >= len(p.schema) && p.allowMoreName == "" {
			continue
		}
		if i >= len(p.schema) {
			d[p.allowMoreName+strconv.Itoa(moreNum)] = part
			moreNum++
		} else {
			dts, err := p.schema[i].ValueParse(part, p.timeZoneOffset)
			if err != nil {
				err = fmt.Errorf("schema [%v] type [%v] value [%v] detail: %v", p.schema[i].name, p.schema[i].dataType, part, err)
				if p.ignoreInvalid {
					log.Warnf("ignore field: %v", err)
					continue
				}
				return nil, err
			}
			for k, v := range dts {
				d[k] = v
			}
		}
	}
	for _, l := range p.labels {
		if _, ok := d[l.Name]; !ok {
			d[l.Name] = l.Value
		}
	}
	if p.isAutoRename {
		d = RenameData(d)
	}
	return d, nil
}

func Rename(datas []Data) []Data {
	newData := make([]Data, len(datas))
	for idx, d := range datas {
		newData[idx] = RenameData(d)
	}
	return newData
}

func RenameData(data Data) Data {
	newData := make(Data)
	for key, val := range data {
		newKey := strings.Replace(key, "-", "_", -1)
		newData[newKey] = val
	}
	return newData
}

func HasSpace(spliter string) bool {
	for _, v := range spliter {
		if unicode.IsSpace(v) {
			return true
		}
	}
	return false
}

func (p *Parser) Parse(lines []string) ([]Data, error) {
	var (
		lineLen    = len(lines)
		datas      = make([]Data, lineLen)
		se         = &StatsError{}
		numRoutine = p.numRoutine

		sendChan   = make(chan parser.ParseInfo)
		resultChan = make(chan parser.ParseResult)
		wg         = new(sync.WaitGroup)
	)
	if lineLen < numRoutine {
		numRoutine = lineLen
	}

	for i := 0; i < numRoutine; i++ {
		wg.Add(1)
		go parser.ParseLine(sendChan, resultChan, wg, !HasSpace(p.delim), p.parse)
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

	var parseResultSlice = make(parser.ParseResultSlice, lineLen)
	for resultInfo := range resultChan {
		parseResultSlice[resultInfo.Index] = resultInfo
	}

	se.DatasourceSkipIndex = make([]int, lineLen)
	datasourceIndex := 0
	dataIndex := 0
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
		if len(parseResult.Data) < 1 { //数据为空时不发送
			se.LastError = "parsed no data by line " + parseResult.Line
			se.AddErrors()
			continue
		}
		se.AddSuccess()
		if p.keepRawData {
			parseResult.Data[KeyRawData] = parseResult.Line
		}
		datas[dataIndex] = parseResult.Data
		dataIndex++
	}

	se.DatasourceSkipIndex = se.DatasourceSkipIndex[:datasourceIndex]
	datas = datas[:dataIndex]
	if se.Errors == 0 {
		return datas, nil
	}
	return datas, se
}
