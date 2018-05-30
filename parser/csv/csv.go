package csv

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/json-iterator/go"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	"github.com/qiniu/logkit/times"
	. "github.com/qiniu/logkit/utils/models"
)

// Type 类型常量
type Type string

const (
	TypeFloat   Type = "float"
	TypeLong    Type = "long"
	TypeString  Type = "string"
	TypeDate    Type = "date"
	TypeJsonMap Type = "jsonmap"
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
	labels               []parser.Label
	delim                string
	isAutoRename         bool
	timeZoneOffset       int
	disableRecordErrData bool
	allowMoreName        string
	allmoreStartNUmber   int
	allowNotMatch        bool
	ignoreInvalid        bool
}

type field struct {
	name       string
	dataType   Type
	typeChange map[string]Type
	allin      bool
}

func init() {
	parser.RegisterConstructor(parser.TypeCSV, NewParser)
}

func NewParser(c conf.MapConf) (parser.Parser, error) {
	name, _ := c.GetStringOr(parser.KeyParserName, "")
	splitter, _ := c.GetStringOr(parser.KeyCSVSplitter, "\t")

	schema, err := c.GetString(parser.KeyCSVSchema)
	if err != nil {
		return nil, err
	}
	timeZoneOffsetRaw, _ := c.GetStringOr(parser.KeyTimeZoneOffset, "")
	timeZoneOffset := parser.ParseTimeZoneOffset(timeZoneOffsetRaw)
	isAutoRename, _ := c.GetBoolOr(parser.KeyAutoRename, false)

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
	labelList, _ := c.GetStringListOr(parser.KeyLabels, []string{})
	if len(labelList) < 1 {
		labelList, _ = c.GetStringListOr(parser.KeyCSVLabels, []string{}) //向前兼容老的配置
	}
	labels := parser.GetLabels(labelList, nameMap)

	disableRecordErrData, _ := c.GetBoolOr(parser.KeyDisableRecordErrData, false)

	allowNotMatch, _ := c.GetBoolOr(parser.KeyCSVAllowNoMatch, false)
	allowMoreName, _ := c.GetStringOr(parser.KeyCSVAllowMore, "")
	if allowMoreName != "" {
		allowNotMatch = true
	}
	allmoreStartNumber, _ := c.GetIntOr(parser.KeyCSVAllowMoreStartNum, 0)
	ignoreInvalid, _ := c.GetBoolOr(parser.KeyCSVIgnoreInvalidField, false)
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
	return newCsvField(columnName, Type(dataType))
}
func parseSchemaJsonField(f string) (fd field, err error) {
	splitSpace := strings.IndexByte(f, ' ')
	key := f[:splitSpace]
	rawfield := strings.TrimSpace(f[splitSpace:])
	rawfield = strings.TrimSpace(rawfield[len(TypeJsonMap):])
	allin := true
	fields := make([]field, 0)
	typeChange := make(map[string]Type)
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
			typeChange[f.name] = Type(f.dataType)
		}
	}
	fd = field{
		name:       key,
		dataType:   TypeJsonMap,
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
	return strings.HasPrefix(rawfield, string(TypeJsonMap))
}

func parseSchemaFields(fieldList []string) (fields []field, err error) {
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
			return
		}
		fields = append(fields, fi)
	}
	return
}

func dataTypeNotSupperted(dataType Type) error {
	return errors.New("type not supported " + string(dataType) + " csv parser currently support string long float date jsonmap 5 types")
}

func newCsvField(name string, dataType Type) (f field, err error) {
	switch dataType {
	case TypeFloat, TypeLong, TypeString, TypeDate:
		f = field{
			name:     name,
			dataType: dataType,
		}
	default:
		err = dataTypeNotSupperted(dataType)
	}
	return
}

func (f field) MakeValue(raw string, timeZoneOffset int) (interface{}, error) {
	return makeValue(raw, f.dataType, timeZoneOffset)
}

func makeValue(raw string, valueType Type, timeZoneOffset int) (interface{}, error) {
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

func checkValue(v interface{}) (f interface{}, err error) {
	switch x := v.(type) {
	case int, int64, float64, string:
		f = x
	default:
		vtype := reflect.TypeOf(v)
		if vtype != nil {
			return nil, dataTypeNotSupperted(Type(vtype.Name()))
		}
		return nil, dataTypeNotSupperted(Type("null"))
	}
	return
}

func convertValue(v interface{}, valueType Type) (ret interface{}, err error) {
	value := fmt.Sprintf("%v", v)
	switch valueType {
	case TypeFloat:
		ret, err = strconv.ParseFloat(value, 64)
	case TypeLong:
		ret, err = strconv.ParseInt(value, 10, 64)
	case TypeString:
		ret = value
	default:
		vtype := reflect.TypeOf(v)
		if vtype != nil {
			return nil, dataTypeNotSupperted(Type(vtype.Name()))
		}
		return nil, dataTypeNotSupperted(Type("null"))
	}
	return
}

func (f field) ValueParse(value string, timeZoneOffset int) (datas Data, err error) {
	if f.dataType != TypeString {
		value = strings.TrimSpace(value)
	}
	datas = Data{}
	switch f.dataType {
	case TypeJsonMap:
		if value == "" {
			return
		}
		m := make(map[string]interface{})
		if err = jsontool.Unmarshal([]byte(value), &m); err != nil {
			err = fmt.Errorf("unmarshal json map type error: %v", err)
			return
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
			return nil, err
		}
		datas[f.name] = v
	}
	return
}

func (p *Parser) Name() string {
	return p.name
}

func (p *Parser) Type() string {
	return parser.TypeCSV
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
	return d, nil
}

func (p *Parser) Rename(datas []Data) []Data {
	newData := make([]Data, 0)
	for _, d := range datas {
		data := make(Data)
		for key, val := range d {
			newKey := strings.Replace(key, "-", "_", -1)
			data[newKey] = val
		}
		newData = append(newData, data)
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
	datas := []Data{}
	se := &StatsError{}
	for idx, line := range lines {
		if !HasSpace(p.delim) {
			line = strings.TrimSpace(line)
		}
		if len(line) <= 0 {
			se.DatasourceSkipIndex = append(se.DatasourceSkipIndex, idx)
			continue
		}
		d, err := p.parse(line)
		if err != nil {
			log.Debug(err)
			se.AddErrors()
			se.ErrorDetail = err
			if !p.disableRecordErrData {
				errData := make(Data)
				errData[KeyPandoraStash] = line
				datas = append(datas, errData)
			} else {
				se.DatasourceSkipIndex = append(se.DatasourceSkipIndex, idx)
			}
			continue
		}
		datas = append(datas, d)
		se.AddSuccess()
	}
	if p.isAutoRename {
		datas = p.Rename(datas)
	}
	return datas, se
}
