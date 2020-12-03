package sql

import (
	"database/sql"
	"fmt"
	"runtime/debug"
	"strings"
	"time"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/utils/magic"
)

func OpenSql(dbtype, connectStr string) (db *sql.DB, err error) {
	db, err = sql.Open(dbtype, connectStr)
	if err != nil {
		return nil, fmt.Errorf("open %v failed: %v", dbtype, err)
	}
	if err = db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

func SchemaCheck(rawSchemas []string) (schemas map[string]string, err error) {
	schemas = make(map[string]string)
	for _, raw := range rawSchemas {
		rs := strings.Fields(raw)
		if len(rs) != 2 {
			err = fmt.Errorf("SQL schema %v not split by space, split lens is %v", raw, len(rs))
			return
		}
		key, vtype := rs[0], rs[1]
		vtype = strings.ToLower(vtype)
		switch vtype {
		case "string", "s":
			vtype = "string"
		case "float", "f":
			vtype = "float"
		case "long", "l":
			vtype = "long"
		default:
			err = fmt.Errorf("schema type %v not supported", vtype)
			return
		}
		schemas[key] = vtype
	}
	return
}

func UpdateSqls(rawsqls string, now time.Time) []string {
	encodedSQLs := strings.Split(rawsqls, SqlSpliter)
	sqls := make([]string, 0)
	for _, esql := range encodedSQLs {
		magicSQL := strings.TrimSpace(magic.GoMagic(esql, now))
		if len(magicSQL) <= 0 {
			continue
		}
		sqls = append(sqls, magicSQL)
	}
	return sqls
}

func Contains(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}

func GetInitScans(length int, rows *sql.Rows, schemas map[string]string, runnerName, name string) (scanArgs []interface{}, nochoiced []bool) {
	nochoice := make([]interface{}, length)
	nochoiced = make([]bool, length)
	for i := range scanArgs {
		nochoice[i] = new(interface{})
		nochoiced[i] = true
	}
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("Recovered in getInitScans err: %v\nstack: %s", r, debug.Stack())
			scanArgs = nochoice
			return
		}
	}()

	tps, err := rows.ColumnTypes()
	if err != nil {
		log.Error(err)
		scanArgs = nochoice
	}
	if len(tps) != length {
		log.Errorf("Runner[%s] %s getInitScans length is %d not equal to columetypes %d", runnerName, name, length, len(tps))
		scanArgs = nochoice
	}
	scanArgs = make([]interface{}, length)
	for i, v := range tps {
		nochoiced[i] = false
		scantype := v.ScanType().String()
		dataBaseType := v.DatabaseTypeName()
		if setDataBaseType(schemas, dataBaseType, v) {
			scanArgs[i] = new(interface{})
			continue
		}
		switch scantype {
		case "int64", "int32", "int16", "int", "int8":
			scanArgs[i] = new(interface{})
			if _, ok := schemas[v.Name()]; !ok {
				schemas[v.Name()] = "long"
			}
		case "float32", "float64":
			scanArgs[i] = new(float64)
		case "uint", "uint8", "uint16", "uint32", "uint64":
			scanArgs[i] = new(uint64)
		case "bool":
			scanArgs[i] = new(interface{})
			if _, ok := schemas[v.Name()]; !ok {
				schemas[v.Name()] = "bool"
			}
		case "[]uint8":
			scanArgs[i] = new([]byte)
		case "string", "RawBytes", "NullTime":
			scanArgs[i] = new(interface{})
			if _, ok := schemas[v.Name()]; !ok {
				schemas[v.Name()] = "string"
			}
		case "time.Time":
			scanArgs[i] = new(interface{})
			if _, ok := schemas[v.Name()]; !ok {
				schemas[v.Name()] = "date"
			}
		case "sql.NullInt64":
			scanArgs[i] = new(interface{})
			if _, ok := schemas[v.Name()]; !ok {
				schemas[v.Name()] = "long"
			}
		case "sql.NullFloat64":
			scanArgs[i] = new(interface{})
			if _, ok := schemas[v.Name()]; !ok {
				schemas[v.Name()] = "float"
			}
		default:
			scanArgs[i] = new(interface{})
			//Postgres Float的ScanType为interface,使用dataBaseType进一步判断
			if strings.Contains(dataBaseType, "FLOAT") {
				if _, ok := schemas[v.Name()]; !ok {
					schemas[v.Name()] = "float"
				}
			} else {
				nochoiced[i] = true
			}
		}
	}
	return scanArgs, nochoiced
}

func GetOffsetIndex(offsetKey string, columns []string) int {
	offsetKeyIndex := -1
	for idx, key := range columns {
		if len(offsetKey) > 0 && key == offsetKey {
			return idx
		}
	}
	return offsetKeyIndex
}

func setDataBaseType(schemas map[string]string, dataBaseType string, v *sql.ColumnType) bool {
	// mysql
	switch dataBaseType {
	case "DATE", "DATETIME", "TIMESTAMP", "TIME":
		schemas[v.Name()] = "date"
		return true
	case "UNIQUEIDENTIFIER": // sqlserver
		schemas[v.Name()] = "uniqueidentifier"
		return true
	default:
		return false
	}
}

func GetOffsetIndexWithTimeStamp(offsetKey, timestampKey string, columns []string) int {
	offsetKeyIndex := -1
	for idx, key := range columns {
		if len(offsetKey) > 0 && key == offsetKey {
			return idx
		}
		if len(timestampKey) > 0 && key == timestampKey {
			return idx
		}
	}
	return offsetKeyIndex
}
