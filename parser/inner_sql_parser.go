package parser

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/times"
	"github.com/qiniu/logkit/utils"
)

const (
	KeyInnerSQLSchema   = "inner_sql_schema"
	KeyInnerMysqlSchema = "inner_mysql_schema" //兼容mysql
)

type InternalSQLParser struct {
	name      string
	labels    []label
	schemas   []InnerMysqlSchema
	schemaErr *schemaErr
}

type InnerMysqlSchema struct {
	Name string
	Type string
}

func parseInnerSQLSchema(schema string) (schemas []InnerMysqlSchema) {
	splts := strings.Split(schema, ",")
	for _, sp := range splts {
		sp = strings.TrimSpace(sp)
		if len(sp) < 1 {
			continue
		}
		ab := strings.Split(sp, " ")
		var ims InnerMysqlSchema
		if len(ab) > 1 {
			ims.Type = ab[len(ab)-1]
		} else {
			ims.Type = "string"
		}
		ims.Name = ab[0]
		schemas = append(schemas, ims)
	}
	return
}

func NewInternalSQLParser(c conf.MapConf) (LogParser, error) {
	name, _ := c.GetStringOr(KeyParserName, "")
	labelList, _ := c.GetStringListOr(KeyLabels, []string{})
	schema, err := c.GetString(KeyInnerSQLSchema)
	if err != nil {
		var nerr error
		schema, nerr = c.GetString(KeyInnerMysqlSchema)
		if nerr != nil {
			return nil, err
		}
	}
	schemas := parseInnerSQLSchema(schema)
	nameMap := map[string]struct{}{}
	for _, sc := range schemas {
		nameMap[sc.Name] = struct{}{}
	}
	labels := getLabels(labelList, nameMap)

	return &InternalSQLParser{
		name:    name,
		labels:  labels,
		schemas: schemas,
		schemaErr: &schemaErr{
			number: 0,
			last:   time.Now(),
		},
	}, nil
}

func (im *InternalSQLParser) Name() string {
	return im.name
}

func (im *InternalSQLParser) Parse(lines []string) ([]sender.Data, error) {
	datas := []sender.Data{}
	se := &utils.StatsError{}
	for _, line := range lines {
		data, err := im.parseLine(line)
		if err != nil {
			im.schemaErr.Output(err)
			se.AddErrors()
			continue
		}
		datas = append(datas, data)
		se.AddSuccess()
	}
	return datas, se
}

func convertDataType(data []byte, Type string) (ret interface{}, err error) {
	dataS := string(data)
	switch Type {
	case "string":
		ret = dataS
	case "long":
		ret, err = strconv.ParseInt(dataS, 10, 64)
	case "float":
		ret, err = strconv.ParseFloat(dataS, 64)
	case "date":
		ta, err := times.StrToTime(dataS)
		if err == nil {
			ret = ta.Format(time.RFC3339Nano)
		}
	default:
		err = fmt.Errorf("%v inner_mysql_schema is not supported", Type)
	}
	return
}

func (im *InternalSQLParser) parseLine(line string) (data sender.Data, err error) {
	data = sender.Data{}
	values, err := utils.TuoDecode([]byte(line))
	if err != nil {
		err = fmt.Errorf("parser %v parseLine %v line err %v", im.name, line, err)
		return
	}
	for idx, sc := range im.schemas {
		if idx >= len(values) {
			continue
		}
		data[sc.Name], err = convertDataType(values[idx], sc.Type)
		if err != nil {
			return
		}
	}
	for _, l := range im.labels {
		data[l.name] = l.dataValue
	}
	return
}
