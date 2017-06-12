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

	_ "github.com/denisenkom/go-mssqldb" //mssql 驱动
	_ "github.com/go-sql-driver/mysql"   //mysql 驱动
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

func NewSQLReader(meta *Meta, readBatch int, dbtype, dataSource, database, rawSqls, cronSchedule, offsetKey string, execOnStart bool) (mr *SqlReader, err error) {
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
		log.Infof("%v Cron job added with schedule <%v>", mr.Name(), cronSchedule)
	}
	return mr, nil
}

func restoreMeta(meta *Meta, rawSqls string) (offsets []int64, sqls []string, omitMeta bool) {
	now := time.Now()
	sqls = updateSqls(rawSqls, now)
	omitMeta = true
	sqlAndOffsets, length, err := meta.ReadOffset()
	if err != nil {
		log.Errorf("%v -meta data is corrupted err:%v, omit meta data", meta.MetaFile(), err)
		return
	}
	tmps := strings.Split(sqlAndOffsets, sqlOffsetConnector)
	if int64(len(tmps)) != 2*length || int64(len(sqls)) != length {
		log.Errorf("%v -meta file is not invalid sql meta file %v， omit meta data", meta.MetaFile(), sqlAndOffsets)
		return
	}
	omitMeta = false
	offsets = make([]int64, length)
	for idx, sql := range sqls {
		syncSQL := strings.Replace(tmps[idx], "@", " ", -1)
		offset, err := strconv.ParseInt(tmps[idx+int(length)], 10, 64)
		if err != nil || sql != syncSQL {
			log.Errorf("%v -meta file sql is out of date %v or parse offset err %v， omit this offset", meta.MetaFile(), syncSQL, err)
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
		log.Infof("%v stopping", mr.Name())
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
	log.Printf("%v pull data deamon started", mr.Name())
}

func (mr *SqlReader) ReadLine() (data string, err error) {
	if !mr.started {
		mr.Start()
	}
	timer := time.NewTicker(time.Millisecond)
	select {
	case dat := <-mr.readChan:
		data = string(dat)
	case <-timer.C:
	}
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
		log.Infof("%v successfully finnished", mr.Name())
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
			log.Warnf("%v stopped from running", mr.Name())
			return
		}
		err := mr.exec(connectStr)
		if err == nil {
			log.Infof("%v successfully exec", mr.Name())
			return
		}
		log.Error(err)
		time.Sleep(3 * time.Second)
	}
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
	log.Infof("%v start to work, sqls %v offsets %v", mr.Name(), mr.syncSQLs, mr.offsets)

	for idx, rawSQL := range mr.syncSQLs {
		//分sql执行
		exit := false
		var isRawSQL bool
		for !exit {
			exit = true
			isRawSQL = false
			execSQL, err := mr.getSQL(idx)
			if err != nil {
				log.Errorf("get SQL error %v, use raw SQL", err)
				execSQL = rawSQL
				isRawSQL = true
			}
			log.Infof("reader <%v> exec sql <%v>", mr.Name(), execSQL)
			rows, err := db.Query(execSQL)
			if err != nil {
				log.Errorf("%v prepare %v <%v> query error %v", mr.Name(), mr.dbtype, execSQL, err)
				continue
			}
			// Get column names
			columns, err := rows.Columns()
			if err != nil {
				log.Errorf("%v prepare %v <%v> columns error %v", mr.Name(), mr.dbtype, execSQL, err)
				continue
			}
			columnsTypes, err := rows.ColumnTypes()
			if err != nil {
				log.Errorf("%v prepare %v <%v> ColumnTypes error %v", mr.Name(), mr.dbtype, execSQL, err)
				continue
			}
			log.Infof("SQL ：<%v>, schemas: <%v>", execSQL, strings.Join(columns, ", "))
			scanArgs := make([]interface{}, len(columns))
			for i := range scanArgs {
				scanArgs[i] = new(interface{})
			}
			offsetKeyIndex := -1
			for idx, key := range columns {
				if key == mr.offsetKey {
					offsetKeyIndex = idx
					break
				}
			}
			// Fetch rows
			for rows.Next() {
				exit = false
				// get RawBytes from data
				err = rows.Scan(scanArgs...)
				if err != nil {
					log.Errorf("%v scan rows error %v", mr.Name(), err)
					continue
				}
				data := make(map[string]interface{})
				for i := 0; i < len(scanArgs); i++ {
					data[columns[i]] = scanArgs[i]
				}
				ret, err := json.Marshal(data)
				if err != nil {
					log.Errorf("%v unmarshal sql data error %v", mr.Name(), err)
					continue
				}
				if atomic.LoadInt32(&mr.status) == StatusStoping {
					log.Warnf("%v stopped from running", mr.Name())
					return nil
				}
				mr.readChan <- ret

				if offsetKeyIndex >= 0 {
					if offsetKeyIndex >= len(columnsTypes) {
						log.Errorf("columnsTypes length %v less than offsetKeyIndex %v", len(columnsTypes), offsetKeyIndex)
					} else {
						mr.offsets[idx], err = convertInt(scanArgs[offsetKeyIndex], columnsTypes[offsetKeyIndex].ScanType())
						if err != nil {
							log.Errorf("%v offset key value parse error %v, offset was not recorded, columTypes: %v", mr.Name(), err, columnsTypes[offsetKeyIndex].ScanType())
							err = nil
						}
					}
				}
				mr.offsets[idx]++
			}
			if exit {
				exit = mr.checkExit(idx, db)
				if !exit {
					mr.offsets[idx] += int64(mr.readBatch)
				}
			}
			if isRawSQL {
				break
			}
		}
	}
	return nil
}

func convertInt(v interface{}, tp reflect.Type) (int64, error) {
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
		switch tp.Kind() {
		case reflect.Int64:
			return idv.(int64), nil
		case reflect.Int:
			return int64(idv.(int)), nil
		case reflect.Int8:
			return int64(idv.(int8)), nil
		case reflect.Int16:
			return int64(idv.(int16)), nil
		case reflect.Int32:
			return int64(idv.(int32)), nil
		case reflect.Uint:
			return int64(idv.(uint)), nil
		case reflect.Uint8:
			return int64(idv.(uint8)), nil
		case reflect.Uint16:
			return int64(idv.(uint16)), nil
		case reflect.Uint32:
			return int64(idv.(uint32)), nil
		case reflect.Uint64:
			return int64(idv.(uint64)), nil
		case reflect.String:
			sdv := idv.(string)
			return strconv.ParseInt(sdv, 10, 64)
		}
	}
	return 0, fmt.Errorf("%v type can not convert to int", dv.Kind())
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
			sql = fmt.Sprintf("%s WHERE %v >= %d AND %v < %d;", rawSQL, mr.offsetKey, mr.offsets[idx], mr.offsetKey, mr.offsets[idx]+int64(mr.readBatch))
		} else {
			err = fmt.Errorf("%v dbtype is not support get SQL without id now", mr.dbtype)
		}
		return
	}
	err = fmt.Errorf("%v dbtype is not support get SQL now", mr.dbtype)
	return
}

func (mr *SqlReader) checkExit(idx int, db *sql.DB) bool {
	if len(mr.offsetKey) <= 0 {
		return true
	}
	rawSQL := mr.syncSQLs[idx]
	rawSQL = strings.TrimSuffix(strings.TrimSpace(rawSQL), ";")
	var tsql string
	if mr.dbtype == ModeMysql {
		tsql = fmt.Sprintf("%s WHERE %v >= %d limit 1;", rawSQL, mr.offsetKey, mr.offsets[idx])
	} else {
		ix := strings.Index(rawSQL, "from")
		if ix < 0 {
			return true
		}
		rawSQL = rawSQL[ix:]
		tsql = fmt.Sprintf("select top(1) * %v WHERE %v >= %d;", rawSQL, mr.offsetKey, mr.offsets[idx])
	}
	rows, err := db.Query(tsql)
	if err != nil {
		log.Error(err)
		return true
	}
	defer rows.Close()
	for rows.Next() {
		return false
	}
	return true
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
		log.Errorf("%v SyncMeta error %v", mr.Name(), err)
	}
	return
}

func (mr *SqlReader) SetMode(mode string, v interface{}) error {
	return errors.New("SqlReader not support readmode")
}
