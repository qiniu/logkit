package reader

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qiniu/log"
	"github.com/robfig/cron"

	"reflect"

	"encoding/binary"

	_ "github.com/denisenkom/go-mssqldb" //mssql 驱动
	_ "github.com/go-sql-driver/mysql"   //mysql 驱动
	"github.com/qiniu/logkit/conf"
)

const (
	mb                 = 1024 * 1024 // 1MB
	sqlOffsetConnector = "##"
	SQL_SPLITER        = ";"
)

type SqlReader struct {
	dbtype     string //数据库类型
	datasource string //数据源
	database   string //数据库名称
	rawsqls    string // 原始sql执行列表

	Cron      *cron.Cron //定时任务
	readBatch int        // 每次读取的数据量
	offsetKey string

	readChan chan []byte

	meta     *Meta    // 记录offset的元数据
	offsets  []int64  // 当前处理文件的sql的offset
	syncSQLs []string // 当前在查询的sqls
	schemas  map[string]string

	status  int32
	mux     sync.Mutex
	started bool

	execOnStart bool
}

const (
	StatusInit int32 = iota
	StatusStopped
	StatusStoping
	StatusRunning
)

func NewSQLReader(meta *Meta, conf conf.MapConf) (mr *SqlReader, err error) {
	var readBatch int
	var dbtype, dataSource, database, rawSqls, cronSchedule, offsetKey string
	var execOnStart bool
	dbtype, _ = conf.GetStringOr(KeyMode, ModeMysql)
	logpath, _ := conf.GetStringOr(KeyLogPath, "")

	switch dbtype {
	case ModeMysql:
		readBatch, _ = conf.GetIntOr(KeyMysqlReadBatch, 100)
		offsetKey, _ = conf.GetStringOr(KeyMysqlOffsetKey, "")
		dataSource, err = conf.GetString(KeyMysqlDataSource)
		if err != nil {
			dataSource = logpath
			if logpath == "" {
				return nil, err
			}
			err = nil
		}
		database, err = conf.GetString(KeyMysqlDataBase)
		if err != nil {
			return nil, err
		}
		rawSqls, err = conf.GetString(KeyMysqlSQL)
		if err != nil {
			return nil, err
		}
		cronSchedule, _ = conf.GetStringOr(KeyMysqlCron, "")
		execOnStart, _ = conf.GetBoolOr(KeyMysqlExecOnStart, true)
	case ModeMssql:
		readBatch, _ = conf.GetIntOr(KeyMssqlReadBatch, 100)
		offsetKey, _ = conf.GetStringOr(KeyMssqlOffsetKey, "")
		dataSource, err = conf.GetString(KeyMssqlDataSource)
		if err != nil {
			dataSource = logpath
			if logpath == "" {
				return nil, err
			}
			err = nil
		}
		database, err = conf.GetString(KeyMssqlDataBase)
		if err != nil {
			return nil, err
		}
		rawSqls, err = conf.GetString(KeyMssqlSQL)
		if err != nil {
			return nil, err
		}
		cronSchedule, _ = conf.GetStringOr(KeyMssqlCron, "")
		execOnStart, _ = conf.GetBoolOr(KeyMssqlExecOnStart, true)
	default:
		err = fmt.Errorf("%v mode not support in sql reader", dbtype)
		return
	}
	rawSchemas, _ := conf.GetStringListOr(KeySQLSchema, []string{})
	schemas, err := schemaCheck(rawSchemas)
	if err != nil {
		return
	}

	offsets, sqls, omitMeta := restoreMeta(meta, rawSqls)

	mr = &SqlReader{
		datasource:  dataSource,
		database:    database,
		rawsqls:     rawSqls,
		Cron:        cron.New(),
		readBatch:   readBatch,
		readChan:    make(chan []byte),
		meta:        meta,
		status:      StatusInit,
		offsetKey:   offsetKey,
		syncSQLs:    sqls,
		dbtype:      dbtype,
		mux:         sync.Mutex{},
		started:     false,
		execOnStart: execOnStart,
		schemas:     schemas,
	}
	// 如果meta初始信息损坏
	if !omitMeta {
		mr.offsets = offsets
	} else {
		mr.offsets = make([]int64, len(mr.syncSQLs))
	}
	//schedule    string     //定时任务配置串
	if len(cronSchedule) > 0 {
		err = mr.Cron.AddFunc(cronSchedule, mr.run)
		if err != nil {
			return
		}
		log.Infof("Runner[%v] %v Cron job added with schedule <%v>", mr.meta.RunnerName, mr.Name(), cronSchedule)
	}
	return mr, nil
}

func schemaCheck(rawSchemas []string) (schemas map[string]string, err error) {
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

func restoreMeta(meta *Meta, rawSqls string) (offsets []int64, sqls []string, omitMeta bool) {
	now := time.Now()
	sqls = updateSqls(rawSqls, now)
	omitMeta = true
	sqlAndOffsets, length, err := meta.ReadOffset()
	if err != nil {
		log.Errorf("Runner[%v] %v -meta data is corrupted err:%v, omit meta data", meta.RunnerName, meta.MetaFile(), err)
		return
	}
	tmps := strings.Split(sqlAndOffsets, sqlOffsetConnector)
	if int64(len(tmps)) != 2*length || int64(len(sqls)) != length {
		log.Errorf("Runner[%v] %v -meta file is not invalid sql meta file %v， omit meta data", meta.RunnerName, meta.MetaFile(), sqlAndOffsets)
		return
	}
	omitMeta = false
	offsets = make([]int64, length)
	for idx, sql := range sqls {
		syncSQL := strings.Replace(tmps[idx], "@", " ", -1)
		offset, err := strconv.ParseInt(tmps[idx+int(length)], 10, 64)
		if err != nil || sql != syncSQL {
			log.Errorf("Runner[%v] %v -meta file sql is out of date %v or parse offset err %v， omit this offset", meta.RunnerName, meta.MetaFile(), syncSQL, err)
		}
		offsets[idx] = offset
	}
	return
}

func convertMagic(magic string, now time.Time) (ret string) {
	switch magic {
	case "YYYY":
		return fmt.Sprintf("%d", now.Year())
	case "YY":
		return fmt.Sprintf("%d", now.Year())[2:]
	case "MM":
		m := int(now.Month())
		return fmt.Sprintf("%02d", m)
	case "M":
		m := int(now.Month())
		return fmt.Sprintf("%d", m)
	case "D":
		d := int(now.Day())
		return fmt.Sprintf("%d", d)
	case "DD":
		d := int(now.Day())
		return fmt.Sprintf("%02d", d)
	case "hh":
		h := now.Hour()
		return fmt.Sprintf("%02d", h)
	case "h":
		h := now.Hour()
		return fmt.Sprintf("%d", h)
	case "mm":
		m := now.Minute()
		return fmt.Sprintf("%02d", m)
	case "m":
		m := now.Minute()
		return fmt.Sprintf("%d", m)
	case "ss":
		s := now.Second()
		return fmt.Sprintf("%02d", s)
	case "s":
		s := now.Second()
		return fmt.Sprintf("%d", s)
	}
	return
}

// 渲染魔法变量
func goMagic(rawSql string, now time.Time) (ret string) {
	sps := strings.Split(rawSql, "@(") //@()，对于每个分片找右括号
	ret = sps[0]
	for idx := 1; idx < len(sps); idx++ {
		idxr := strings.Index(sps[idx], ")")
		if idxr == -1 {
			return rawSql
		}
		ret += convertMagic(sps[idx][0:idxr], now)
		if idxr+1 < len(sps[idx]) {
			ret += sps[idx][idxr+1:]
		}
	}
	return
}

func (mr *SqlReader) Name() string {
	return strings.ToUpper(mr.dbtype) + "_Reader:" + mr.database + "_" + hash(mr.rawsqls)
}

func (mr *SqlReader) Source() string {
	return mr.datasource + "_" + mr.database
}

func (mr *SqlReader) Close() (err error) {
	mr.Cron.Stop()
	if atomic.CompareAndSwapInt32(&mr.status, StatusRunning, StatusStoping) {
		log.Infof("Runner[%v] %v stopping", mr.meta.RunnerName, mr.Name())
	} else {
		close(mr.readChan)
	}
	return
}

//Start 仅调用一次，借用ReadLine启动，不能在new实例的时候启动，会有并发问题
func (mr *SqlReader) Start() {
	mr.mux.Lock()
	defer mr.mux.Unlock()
	if mr.started {
		return
	}
	mr.Cron.Start()
	if mr.execOnStart {
		go mr.run()
	}
	mr.started = true
	log.Infof("Runner[%v] %v pull data deamon started", mr.meta.RunnerName, mr.Name())
}

func (mr *SqlReader) ReadLine() (data string, err error) {
	if !mr.started {
		mr.Start()
	}
	timer := time.NewTimer(time.Second)
	select {
	case dat := <-mr.readChan:
		data = string(dat)
	case <-timer.C:
	}
	timer.Stop()
	return
}

func updateSqls(rawsqls string, now time.Time) []string {
	encodedSQLs := strings.Split(rawsqls, SQL_SPLITER)
	sqls := make([]string, 0)
	for _, esql := range encodedSQLs {
		magicSQL := strings.TrimSpace(goMagic(esql, now))
		if len(magicSQL) <= 0 {
			continue
		}
		sqls = append(sqls, magicSQL)
	}
	return sqls
}

//check if syncSQLs is out of date
func (mr *SqlReader) updateOffsets(sqls []string) {
	for idx, sql := range sqls {
		if idx >= len(mr.offsets) {
			mr.offsets = append(mr.offsets, 0)
			continue
		}
		if idx >= len(mr.syncSQLs) {
			continue
		}
		if mr.syncSQLs[idx] != sql {
			mr.offsets[idx] = 0
		}
	}
}

func (mr *SqlReader) run() {
	var err error
	// 防止并发run
	for {
		if atomic.LoadInt32(&mr.status) == StatusStopped {
			return
		}
		if atomic.CompareAndSwapInt32(&mr.status, StatusInit, StatusRunning) {
			break
		}
	}
	// running在退出状态改为Init
	defer func() {
		atomic.CompareAndSwapInt32(&mr.status, StatusRunning, StatusInit)
		if atomic.CompareAndSwapInt32(&mr.status, StatusStoping, StatusStopped) {
			close(mr.readChan)
		}
		if err == nil {
			log.Infof("Runner[%v] %v successfully finished", mr.meta.RunnerName, mr.Name())
		}
	}()

	var connectStr string
	switch mr.dbtype {
	case "mysql":
		connectStr = mr.datasource + "/" + mr.database
	case "mssql":
		connectStr = mr.datasource + ";database=" + mr.database
	}
	// 开始work逻辑
	for {
		if atomic.LoadInt32(&mr.status) == StatusStoping {
			log.Warnf("Runner[%v] %v stopped from running", mr.meta.RunnerName, mr.Name())
			return
		}
		err = mr.exec(connectStr)
		if err == nil {
			log.Infof("Runner[%v] %v successfully exec", mr.meta.RunnerName, mr.Name())
			return
		}
		log.Error(err)
		time.Sleep(3 * time.Second)
	}
}

func getInitScans(length int) []interface{} {
	scanArgs := make([]interface{}, length)
	for i := range scanArgs {
		scanArgs[i] = new(interface{})
	}
	return scanArgs
}

func (mr *SqlReader) getOffsetIndex(columns []string) int {
	offsetKeyIndex := -1
	for idx, key := range columns {
		if key == mr.offsetKey {
			return idx
		}
	}
	return offsetKeyIndex
}

func (mr *SqlReader) exec(connectStr string) (err error) {
	now := time.Now()
	db, err := sql.Open(mr.dbtype, connectStr)
	if err != nil {
		return fmt.Errorf("%v open %v failed: %v", mr.Name(), mr.dbtype, err)
	}
	defer db.Close()
	if err = db.Ping(); err != nil {
		return
	}
	//更新sqls
	sqls := updateSqls(mr.rawsqls, now)
	mr.updateOffsets(sqls)
	mr.syncSQLs = sqls
	log.Infof("Runner[%v] %v start to work, sqls %v offsets %v", mr.meta.RunnerName, mr.Name(), mr.syncSQLs, mr.offsets)

	for idx, rawSQL := range mr.syncSQLs {
		//分sql执行
		exit := false
		var isRawSQL bool
		for !exit {
			exit = true
			isRawSQL = false
			execSQL, err := mr.getSQL(idx)
			if err != nil {
				log.Errorf("Runner[%v] get SQL error %v, use raw SQL", mr.meta.RunnerName, err)
				execSQL = rawSQL
				isRawSQL = true
			}
			log.Infof("Runner[%v] reader <%v> exec sql <%v>", mr.meta.RunnerName, mr.Name(), execSQL)
			rows, err := db.Query(execSQL)
			if err != nil {
				log.Errorf("Runner[%v] %v prepare %v <%v> query error %v", mr.meta.RunnerName, mr.Name(), mr.dbtype, execSQL, err)
				continue
			}
			// Get column names
			columns, err := rows.Columns()
			if err != nil {
				log.Errorf("Runner[%v] %v prepare %v <%v> columns error %v", mr.meta.RunnerName, mr.Name(), mr.dbtype, execSQL, err)
				continue
			}
			log.Infof("Runner[%v] SQL ：<%v>, schemas: <%v>", mr.meta.RunnerName, execSQL, strings.Join(columns, ", "))
			scanArgs := getInitScans(len(columns))
			offsetKeyIndex := mr.getOffsetIndex(columns)
			// Fetch rows
			var maxOffset int64 = -1
			for rows.Next() {
				exit = false
				// get RawBytes from data
				err = rows.Scan(scanArgs...)
				if err != nil {
					log.Errorf("Runner[%v] %v scan rows error %v", mr.meta.RunnerName, mr.Name(), err)
					continue
				}
				data := make(map[string]interface{})
				for i := 0; i < len(scanArgs); i++ {
					vtype, ok := mr.schemas[columns[i]]
					if ok {
						switch vtype {
						case "long":
							val, serr := convertLong(scanArgs[i])
							if serr != nil {
								log.Errorf("convertLong for %v (%v) error %v, ignore this key...", columns[i], scanArgs[i], serr)
							} else {
								data[columns[i]] = &val
							}
						case "float":
							val, serr := convertFloat(scanArgs[i])
							if serr != nil {
								log.Errorf("convertFloat for %v (%v) error %v, ignore this key...", columns[i], scanArgs[i], serr)
							} else {
								data[columns[i]] = &val
							}
						}
					} else {
						val, serr := convertString(scanArgs[i])
						if serr != nil {
							log.Errorf("convertString for %v (%v) error %v, ignore this key...", columns[i], scanArgs[i], serr)
						}
						data[columns[i]] = val
					}
				}
				ret, err := json.Marshal(data)
				if err != nil {
					log.Errorf("Runner[%v] %v unmarshal sql data error %v", mr.meta.RunnerName, mr.Name(), err)
					continue
				}
				if atomic.LoadInt32(&mr.status) == StatusStoping {
					log.Warnf("Runner[%v] %v stopped from running", mr.meta.RunnerName, mr.Name())
					return nil
				}
				mr.readChan <- ret

				if offsetKeyIndex >= 0 {
					var tmpOffsetIndex int64
					tmpOffsetIndex, err = convertLong(scanArgs[offsetKeyIndex])
					if err != nil {
						log.Errorf("Runner[%v] %v offset key value parse error %v, offset was not recorded", mr.meta.RunnerName, mr.Name(), err)
						err = nil
					} else if tmpOffsetIndex > maxOffset {
						maxOffset = tmpOffsetIndex
					}
				} else {
					mr.offsets[idx]++
				}
			}
			if maxOffset > 0 {
				mr.offsets[idx] = maxOffset + 1
			}
			if exit {
				var newOffsetIdx int64
				exit, newOffsetIdx = mr.checkExit(idx, db)
				if !exit {
					mr.offsets[idx] += int64(mr.readBatch)
					if newOffsetIdx > mr.offsets[idx] {
						mr.offsets[idx] = newOffsetIdx
					}
				} else {
					log.Infof("Runner[%v] %v no data any more, exit...", mr.meta.RunnerName, mr.Name())
				}
			}
			if isRawSQL {
				log.Infof("Runner[%v] %v is raw SQL, exit after exec once...", mr.meta.RunnerName, mr.Name())
				break
			}
		}
	}
	return nil
}

func convertLong(v interface{}) (int64, error) {
	dpv := reflect.ValueOf(v)
	if dpv.Kind() != reflect.Ptr {
		return 0, errors.New("scanArgs not a pointer")
	}
	if dpv.IsNil() {
		return 0, errors.New("scanArgs is a nil pointer")
	}
	dv := reflect.Indirect(dpv)
	switch dv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return dv.Int(), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return int64(dv.Uint()), nil
	case reflect.String:
		return strconv.ParseInt(dv.String(), 10, 64)
	case reflect.Interface:
		idv := dv.Interface()
		if ret, ok := idv.(int64); ok {
			return ret, nil
		}
		if ret, ok := idv.(int); ok {
			return int64(ret), nil
		}
		if ret, ok := idv.(uint); ok {
			return int64(ret), nil
		}
		if ret, ok := idv.(uint64); ok {
			return int64(ret), nil
		}
		if ret, ok := idv.(string); ok {
			return strconv.ParseInt(ret, 10, 64)
		}
		if ret, ok := idv.(int8); ok {
			return int64(ret), nil
		}
		if ret, ok := idv.(int16); ok {
			return int64(ret), nil
		}
		if ret, ok := idv.(int32); ok {
			return int64(ret), nil
		}
		if ret, ok := idv.(uint8); ok {
			return int64(ret), nil
		}
		if ret, ok := idv.(uint16); ok {
			return int64(ret), nil
		}
		if ret, ok := idv.(uint32); ok {
			return int64(ret), nil
		}
		if ret, ok := idv.([]byte); ok {
			if len(ret) == 8 {
				return int64(binary.BigEndian.Uint64(ret)), nil
			} else {
				return strconv.ParseInt(string(ret), 10, 64)
			}
		}
		if idv == nil {
			return 0, nil
		}
		log.Errorf("sql reader convertLong for type %v is not supported", reflect.TypeOf(idv))
	}
	return 0, fmt.Errorf("%v type can not convert to int", dv.Kind())
}

func convertFloat(v interface{}) (float64, error) {
	dpv := reflect.ValueOf(v)
	if dpv.Kind() != reflect.Ptr {
		return 0, errors.New("scanArgs not a pointer")
	}
	if dpv.IsNil() {
		return 0, errors.New("scanArgs is a nil pointer")
	}
	dv := reflect.Indirect(dpv)
	switch dv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return float64(dv.Int()), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return float64(dv.Uint()), nil
	case reflect.Float32, reflect.Float64:
		return dv.Float(), nil
	case reflect.String:
		return strconv.ParseFloat(dv.String(), 64)
	case reflect.Interface:
		idv := dv.Interface()
		if ret, ok := idv.(float64); ok {
			return ret, nil
		}
		if ret, ok := idv.(float32); ok {
			return float64(ret), nil
		}
		if ret, ok := idv.(int64); ok {
			return float64(ret), nil
		}
		if ret, ok := idv.(int); ok {
			return float64(ret), nil
		}
		if ret, ok := idv.(uint); ok {
			return float64(ret), nil
		}
		if ret, ok := idv.(uint64); ok {
			return float64(ret), nil
		}
		if ret, ok := idv.(string); ok {
			return strconv.ParseFloat(ret, 64)
		}
		if ret, ok := idv.(int8); ok {
			return float64(ret), nil
		}
		if ret, ok := idv.(int16); ok {
			return float64(ret), nil
		}
		if ret, ok := idv.(int32); ok {
			return float64(ret), nil
		}
		if ret, ok := idv.(uint8); ok {
			return float64(ret), nil
		}
		if ret, ok := idv.(uint16); ok {
			return float64(ret), nil
		}
		if ret, ok := idv.(uint32); ok {
			return float64(ret), nil
		}
		if ret, ok := idv.([]byte); ok {
			return strconv.ParseFloat(string(ret), 64)
		}
		if idv == nil {
			return 0, nil
		}
		log.Errorf("sql reader convertFloat for type %v is not supported", reflect.TypeOf(idv))
	}
	return 0, fmt.Errorf("%v type can not convert to int", dv.Kind())
}

func convertString(v interface{}) (string, error) {
	dpv := reflect.ValueOf(v)
	if dpv.Kind() != reflect.Ptr {
		return "", errors.New("scanArgs not a pointer")
	}
	if dpv.IsNil() {
		return "", errors.New("scanArgs is a nil pointer")
	}
	dv := reflect.Indirect(dpv)
	switch dv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.Itoa(int(dv.Int())), nil
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.Itoa(int(dv.Uint())), nil
		// return int64(dv.Uint()), nil
	case reflect.String:
		return dv.String(), nil
	case reflect.Interface:
		idv := dv.Interface()
		if ret, ok := idv.(int64); ok {
			return strconv.Itoa(int(ret)), nil
		}
		if ret, ok := idv.(int); ok {
			return strconv.Itoa(int(ret)), nil
		}
		if ret, ok := idv.(uint); ok {
			return strconv.Itoa(int(ret)), nil
		}
		if ret, ok := idv.(uint64); ok {
			return strconv.Itoa(int(ret)), nil
		}
		if ret, ok := idv.(string); ok {
			return ret, nil
		}
		if ret, ok := idv.(int8); ok {
			return strconv.Itoa(int(ret)), nil
		}
		if ret, ok := idv.(int16); ok {
			return strconv.Itoa(int(ret)), nil
		}
		if ret, ok := idv.(int32); ok {
			return strconv.Itoa(int(ret)), nil
		}
		if ret, ok := idv.(uint8); ok {
			return strconv.Itoa(int(ret)), nil
		}
		if ret, ok := idv.(uint16); ok {
			return strconv.Itoa(int(ret)), nil
		}
		if ret, ok := idv.(uint32); ok {
			return strconv.Itoa(int(ret)), nil
		}
		if ret, ok := idv.([]byte); ok {
			return string(ret), nil
		}
		if idv == nil {
			return "", nil
		}
		log.Errorf("sql reader convertLong for type %v is not supported", reflect.TypeOf(idv))
	}
	return "", fmt.Errorf("%v type can not convert to string", dv.Kind())
}

func (mr *SqlReader) getSQL(idx int) (sql string, err error) {
	rawSQL := mr.syncSQLs[idx]
	rawSQL = strings.TrimSuffix(strings.TrimSpace(rawSQL), ";")
	switch mr.dbtype {
	case ModeMysql:
		if len(mr.offsetKey) > 0 {
			sql = fmt.Sprintf("%s WHERE %v >= %d AND %v < %d;", rawSQL, mr.offsetKey, mr.offsets[idx], mr.offsetKey, mr.offsets[idx]+int64(mr.readBatch))
		} else {
			sql = fmt.Sprintf("%s LIMIT %d,%d;", rawSQL, mr.offsets[idx], mr.offsets[idx]+int64(mr.readBatch))
		}
		return
	case ModeMssql:
		if len(mr.offsetKey) > 0 {
			sql = fmt.Sprintf("%s WHERE CAST(%v AS BIGINT) >= %d AND CAST(%v AS BIGINT) < %d;", rawSQL, mr.offsetKey, mr.offsets[idx], mr.offsetKey, mr.offsets[idx]+int64(mr.readBatch))
		} else {
			err = fmt.Errorf("%v dbtype is not support get SQL without id now", mr.dbtype)
		}
		return
	}
	err = fmt.Errorf("%v dbtype is not support get SQL now", mr.dbtype)
	return
}

func (mr *SqlReader) checkExit(idx int, db *sql.DB) (bool, int64) {
	if len(mr.offsetKey) <= 0 {
		return true, -1
	}
	rawSQL := mr.syncSQLs[idx]
	rawSQL = strings.TrimSuffix(strings.TrimSpace(rawSQL), ";")
	var tsql string
	if mr.dbtype == ModeMysql {
		tsql = fmt.Sprintf("%s WHERE %v >= %d order by %v limit 1;", rawSQL, mr.offsetKey, mr.offsets[idx], mr.offsetKey)
	} else {
		ix := strings.Index(rawSQL, "from")
		if ix < 0 {
			return true, -1
		}
		rawSQL = rawSQL[ix:]
		tsql = fmt.Sprintf("select top(1) * %v WHERE CAST(%v AS BIGINT) >= %v order by CAST(%v AS BIGINT);", rawSQL, mr.offsetKey, mr.offsets[idx], mr.offsetKey)
	}
	rows, err := db.Query(tsql)
	if err != nil {
		log.Error(err)
		return true, -1
	}
	defer rows.Close()
	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		log.Errorf("Runner[%v] %v prepare %v columns error %v", mr.meta.RunnerName, mr.Name(), mr.dbtype, err)
		return true, -1
	}

	scanArgs := getInitScans(len(columns))
	offsetKeyIndex := mr.getOffsetIndex(columns)
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return false, -1
		}
		if offsetKeyIndex >= 0 {
			offsetIdx, err := convertLong(scanArgs[offsetKeyIndex])
			if err != nil {
				return false, -1
			}
			return false, offsetIdx
		}
		return false, -1
	}
	return true, -1
}

//SyncMeta 从队列取数据时同步队列，作用在于保证数据不重复。
func (mr *SqlReader) SyncMeta() {
	encodeSQLs := make([]string, 0)
	for _, sql := range mr.syncSQLs {
		encodeSQLs = append(encodeSQLs, strings.Replace(sql, " ", "@", -1))
	}
	for _, offset := range mr.offsets {
		encodeSQLs = append(encodeSQLs, strconv.FormatInt(offset, 10))
	}
	all := strings.Join(encodeSQLs, sqlOffsetConnector)
	if err := mr.meta.WriteOffset(all, int64(len(mr.syncSQLs))); err != nil {
		log.Errorf("Runner[%v] %v SyncMeta error %v", mr.meta.RunnerName, mr.Name(), err)
	}
	return
}

func (mr *SqlReader) SetMode(mode string, v interface{}) error {
	return errors.New("SqlReader not support readmode")
}
