package parser

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/times"
	"github.com/qiniu/logkit/utils"

	"github.com/json-iterator/go"
	"github.com/qiniu/log"
)

// Type 类型常量
type CsvType string

const (
	TypeFloat   CsvType = "float"
	TypeLong    CsvType = "long"
	TypeString  CsvType = "string"
	TypeDate    CsvType = "date"
	TypeJsonMap CsvType = "jsonmap"
)

const (
	KeyCSVSchema   = "csv_schema"      // csv 每个列的列名和类型 long/string/float/date
	KeyCSVSplitter = "csv_splitter"    // csv 的分隔符
	KeyCSVLabels   = "csv_labels"      // csv 额外增加的标签信息，比如机器信息等
	KeyAutoRename  = "csv_auto_rename" // 是否将不合法的字段名称重命名一下, 比如 header-host 重命名为 header_host
)

const MaxParserSchemaErrOutput = 5

var jsontool = jsoniter.Config{
	EscapeHTML:             true,
	UseNumber:              true,
	ValidateJsonRawMessage: true,
}.Froze()

type CsvParser struct {
	name           string
	schema         []field
	labels         []Label
	delim          string
	isAutoRename   bool
	timeZoneOffset int
}

type field struct {
	name       string
	dataType   CsvType
	typeChange map[string]CsvType
	allin      bool
}

func NewCsvParser(c conf.MapConf) (LogParser, error) {
	name, _ := c.GetStringOr(KeyParserName, "")
	splitter, _ := c.GetStringOr(KeyCSVSplitter, "\t")

	schema, err := c.GetString(KeyCSVSchema)
	if err != nil {
		return nil, err
	}
	timeZoneOffsetRaw, _ := c.GetStringOr(KeyTimeZoneOffset, "")
	timeZoneOffset := parseTimeZoneOffset(timeZoneOffsetRaw)
	isAutoRename, _ := c.GetBoolOr(KeyAutoRename, false)

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
	labels := GetLabels(labelList, nameMap)

	return &CsvParser{
		name:           name,
		schema:         fields,
		labels:         labels,
		delim:          splitter,
		isAutoRename:   isAutoRename,
		timeZoneOffset: timeZoneOffset,
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
	return newCsvField(columnName, CsvType(dataType))
}
func parseSchemaJsonField(f string) (fd field, err error) {
	splitSpace := strings.IndexByte(f, ' ')
	key := f[:splitSpace]
	rawfield := strings.TrimSpace(f[splitSpace:])
	rawfield = strings.TrimSpace(rawfield[len(TypeJsonMap):])
	allin := true
	fields := make([]field, 0)
	typeChange := make(map[string]CsvType)
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
			typeChange[f.name] = CsvType(f.dataType)
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

func dataTypeNotSupperted(dataType CsvType) error {
	return errors.New("type not supported " + string(dataType) + " csv parser currently support string long float date jsonmap 5 types")
}

func newLabel(name, dataValue string) Label {
	return Label{
		Name:  name,
		Value: dataValue,
	}
}

func newCsvField(name string, dataType CsvType) (f field, err error) {
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

func makeValue(raw string, valueType CsvType, timeZoneOffset int) (interface{}, error) {
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
		return strings.TrimSpace(raw), nil
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
			return nil, dataTypeNotSupperted(CsvType(vtype.Name()))
		}
		return nil, dataTypeNotSupperted(CsvType("null"))
	}
	return
}

func convertValue(v interface{}, valueType CsvType) (ret interface{}, err error) {
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
			return nil, dataTypeNotSupperted(CsvType(vtype.Name()))
		}
		return nil, dataTypeNotSupperted(CsvType("null"))
	}
	return
}

func (f field) ValueParse(value string, timeZoneOffset int) (datas sender.Data, err error) {
	datas = sender.Data{}
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

func (p *CsvParser) Name() string {
	return p.name
}

func (p *CsvParser) Type() string {
	return TypeCSV
}

func (p *CsvParser) parse(line string) (sender.Data, error) {
	d := make(sender.Data, len(p.schema)+len(p.labels))
	parts := strings.Split(line, p.delim)
	if len(parts) != len(p.schema) {
		return nil, fmt.Errorf("schema length not match: schema %v length %v, actual column %v length %v", p.schema, len(p.schema), parts, len(parts))
	}
	for i, part := range parts {
		dts, err := p.schema[i].ValueParse(strings.TrimSpace(part), p.timeZoneOffset)
		if err != nil {
			return nil, fmt.Errorf("schema %v type %v error %v detail: %v", p.schema[i].name, p.schema[i].dataType, part, err)
		}
		for k, v := range dts {
			d[k] = v
		}
	}
	for _, l := range p.labels {
		d[l.Name] = l.Value
	}
	return d, nil
}

func (p *CsvParser) Rename(datas []sender.Data) []sender.Data {
	newData := make([]sender.Data, 0)
	for _, d := range datas {
		data := make(sender.Data)
		for key, val := range d {
			newKey := strings.Replace(key, "-", "_", -1)
			data[newKey] = val
		}
		newData = append(newData, data)
	}
	return newData
}

func (p *CsvParser) Parse(lines []string) ([]sender.Data, error) {
	datas := []sender.Data{}
	se := &utils.StatsError{}
	for idx, line := range lines {
		d, err := p.parse(line)
		if err != nil {
			log.Debug(err)
			se.AddErrors()
			se.ErrorIndex = append(se.ErrorIndex, idx)
			se.ErrorDetail = err
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
