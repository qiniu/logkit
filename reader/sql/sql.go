package sql

import (
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/denisenkom/go-mssqldb" //mssql 驱动
	_ "github.com/go-sql-driver/mysql"   //mysql 驱动
	_ "github.com/lib/pq"                //postgres 驱动
	"github.com/robfig/cron"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/utils/models"
)

const (
	mb                   = 1024 * 1024 // 1MB
	sqlOffsetConnector   = "##"
	SQL_SPLITER          = ";"
	DefaultMySQLTable    = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA='DATABASE_NAME';"
	DefaultMySQLDatabase = "SHOW DATABASES;"
	DefaultPGSQLTable    = "SELECT TABLENAME FROM PG_TABLES WHERE SCHEMANAME='public';"
	DefaultMsSQLTable    = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_CATALOG='DATABASE_NAME';"

	SupportReminder = "history all magic only support @(YYYY) @(YY) @(MM) @(DD) @(hh) @(mm) @(ss)"
	Wildcards       = "*"

	DefaultDoneRecordsFile = "sql.records"
)

const (
	YEAR = iota
	MONTH
	DAY
	HOUR
	MINUTE
	SECOND
)

const (
	// 获取符合条件的table
	TABLE = iota
	// 获取符合条件的database
	DATABASE
	// 获取数据库表的总条数
	COUNT
)

var (
	_ reader.DataReader = &Reader{}
	_ reader.Reader     = &Reader{}
)

var MysqlSystemDB = []string{"information_schema", "performance_schema", "mysql", "sys"}

func init() {
	reader.RegisterConstructor(reader.ModeMySQL, NewReader)
	reader.RegisterConstructor(reader.ModeMSSQL, NewReader)
	reader.RegisterConstructor(reader.ModePostgreSQL, NewReader)
}

type readInfo struct {
	data  Data
	bytes int64
}

type Reader struct {
	dbtype      string //数据库类型
	datasource  string //数据源
	database    string //数据库名称
	rawDatabase string // 记录原始数据库
	rawsqls     string // 原始sql执行列表
	historyAll  bool   // 是否导入历史数据
	rawTable    string // 记录原始数据库表名
	table       string // 数据库表名

	Cron      *cron.Cron //定时任务
	readBatch int        // 每次读取的数据量
	offsetKey string

	readChan chan readInfo

	meta              *reader.Meta // 记录offset的元数据
	encoder           string       // 解码方式
	offsets           []int64      // 当前处理文件的sql的offset
	syncSQLs          []string     // 当前在查询的sqls
	syncRecords       DBRecords    // 将要append的记录
	doneRecords       DBRecords    // 已经读过的记录
	omitDoneDBRecords bool
	schemas           map[string]string

	status  int32
	mux     sync.Mutex
	started bool

	execOnStart  bool
	loop         bool
	loopDuration time.Duration
	magicLagDur  time.Duration
	count        int64
	CurrentCount int64

	stats     StatsInfo
	statsLock sync.RWMutex
	countLock sync.RWMutex
}

type DBRecords map[string]TableRecords

type TableRecords map[string]TableInfo

type TableInfo struct {
	size   int64
	offset int64
}

func (dbRecords *DBRecords) Set(value DBRecords) {
	if *dbRecords == nil {
		*dbRecords = make(DBRecords)
	}
	*dbRecords = value
}

func (dbRecords *DBRecords) SetTableRecords(db string, tableRecords TableRecords) {
	if *dbRecords == nil {
		*dbRecords = make(DBRecords)
	}
	(*dbRecords)[db] = tableRecords
}

func (dbRecords *DBRecords) GetTableRecords(db string) TableRecords {
	if *dbRecords != nil {
		return (*dbRecords)[db]
	}
	return nil
}

func (dbRecords *DBRecords) Reset() {
	*dbRecords = make(DBRecords)
}

func (tableRecords *TableRecords) Set(value TableRecords) {
	if *tableRecords == nil {
		*tableRecords = make(TableRecords)
	}
	*tableRecords = value
}

func (tableRecords *TableRecords) SetTableInfo(table string, tableInfo TableInfo) {
	if *tableRecords == nil {
		*tableRecords = make(TableRecords)
	}
	(*tableRecords)[table] = tableInfo
}

func (tableRecords *TableRecords) GetTableInfo(table string) TableInfo {
	var tableInfo TableInfo
	if *tableRecords != nil {
		tableInfo = (*tableRecords)[table]
	}
	return tableInfo
}

func (tableRecords *TableRecords) Reset() {
	*tableRecords = make(TableRecords)
}

func NewReader(meta *reader.Meta, conf conf.MapConf) (ret reader.Reader, err error) {
	var readBatch int
	var dbtype, dataSource, rawDatabase, rawSqls, cronSchedule, offsetKey, encoder, table string
	var execOnStart, historyAll bool
	dbtype, _ = conf.GetStringOr(reader.KeyMode, reader.ModeMySQL)
	logpath, _ := conf.GetStringOr(reader.KeyLogPath, "")

	switch dbtype {
	case reader.ModeMySQL:
		readBatch, _ = conf.GetIntOr(reader.KeyMysqlReadBatch, 100)
		offsetKey, _ = conf.GetStringOr(reader.KeyMysqlOffsetKey, "")
		dataSource, err = conf.GetString(reader.KeyMysqlDataSource)
		if err != nil {
			dataSource = logpath
			if logpath == "" {
				return nil, err
			}
			err = nil
		}
		rawDatabase, _ = conf.GetStringOr(reader.KeyMysqlDataBase, "")
		rawSqls, _ = conf.GetStringOr(reader.KeyMysqlSQL, "")
		cronSchedule, _ = conf.GetStringOr(reader.KeyMysqlCron, "")
		execOnStart, _ = conf.GetBoolOr(reader.KeyMysqlExecOnStart, true)
		encoder, _ = conf.GetStringOr(reader.KeyEncoding, "")
		historyAll, _ = conf.GetBoolOr(reader.KeyMysqlHistoryAll, false)
		table, _ = conf.GetStringOr(reader.KyeMysqlTable, "")
	case reader.ModeMSSQL:
		readBatch, _ = conf.GetIntOr(reader.KeyMssqlReadBatch, 100)
		offsetKey, _ = conf.GetStringOr(reader.KeyMssqlOffsetKey, "")
		dataSource, err = conf.GetString(reader.KeyMssqlDataSource)
		if err != nil {
			dataSource = logpath
			if logpath == "" {
				return nil, err
			}
			err = nil
		}
		rawDatabase, err = conf.GetString(reader.KeyMssqlDataBase)
		if err != nil {
			return nil, err
		}
		rawSqls, _ = conf.GetStringOr(reader.KeyMssqlSQL, "")
		cronSchedule, _ = conf.GetStringOr(reader.KeyMssqlCron, "")
		execOnStart, _ = conf.GetBoolOr(reader.KeyMssqlExecOnStart, true)
	case reader.ModePostgreSQL:
		readBatch, _ = conf.GetIntOr(reader.KeyPGsqlReadBatch, 100)
		offsetKey, _ = conf.GetStringOr(reader.KeyPGsqlOffsetKey, "")
		dataSource, err = conf.GetString(reader.KeyPGsqlDataSource)
		if err != nil {
			dataSource = logpath
			if logpath == "" {
				return nil, err
			}
			err = nil
		}
		sps := strings.Split(dataSource, " ")
		for _, v := range sps {
			v = strings.TrimSpace(v)
			if v == "" {
				continue
			}
			x1s := strings.Split(v, "=")
			if len(x1s) != 2 {
				err = fmt.Errorf("datasource %v is invalid, don't contain space beside symbol '='", dataSource)
				return nil, err
			}
		}
		rawDatabase, err = conf.GetString(reader.KeyPGsqlDataBase)
		if err != nil {
			got := false
			for _, v := range sps {
				if strings.Contains(v, "dbname") {
					rawDatabase = strings.TrimPrefix(v, "dbname=")
					got = true
					break
				}
			}
			if !got {
				return nil, err
			}
		}
		rawSqls, _ = conf.GetStringOr(reader.KeyPGsqlSQL, "")
		cronSchedule, _ = conf.GetStringOr(reader.KeyPGsqlCron, "")
		execOnStart, _ = conf.GetBoolOr(reader.KeyPGsqlExecOnStart, true)
	default:
		err = fmt.Errorf("%v mode not support in sql reader", dbtype)
		return nil, err
	}
	rawSchemas, _ := conf.GetStringListOr(reader.KeySQLSchema, []string{})
	magicLagDur, _ := conf.GetStringOr(reader.KeyMagicLagDuration, "")
	var mgld time.Duration
	if magicLagDur != "" {
		mgld, err = time.ParseDuration(magicLagDur)
		if err != nil {
			return nil, err
		}
	}
	schemas, err := schemaCheck(rawSchemas)
	if err != nil {
		return nil, err
	}

	var sqls []string
	omitMeta := true
	var offsets []int64
	if rawSqls != "" {
		offsets, sqls, omitMeta = restoreMeta(meta, rawSqls, mgld)
	}

	mr := &Reader{
		datasource:  dataSource,
		database:    rawDatabase,
		rawDatabase: rawDatabase,
		rawsqls:     rawSqls,
		Cron:        cron.New(),
		readBatch:   readBatch,
		readChan:    make(chan readInfo),
		meta:        meta,
		status:      reader.StatusInit,
		offsetKey:   offsetKey,
		syncSQLs:    sqls,
		dbtype:      dbtype,
		mux:         sync.Mutex{},
		started:     false,
		execOnStart: execOnStart,
		historyAll:  historyAll,
		rawTable:    table,
		table:       table,
		magicLagDur: mgld,
		schemas:     schemas,
		statsLock:   sync.RWMutex{},
		encoder:     encoder,
	}

	if mr.rawDatabase == "" {
		mr.rawDatabase = "*"
	}
	if mr.rawTable == "" {
		mr.rawTable = "*"
	}

	if mr.rawsqls == "" {
		valid := checkMagic(mr.database) && checkMagic(mr.table)
		if !valid {
			err = fmt.Errorf(SupportReminder)
			return nil, err
		}

		mr.omitDoneDBRecords = mr.doneRecords.restoreRecordsFile(mr.meta)
	}

	// 如果meta初始信息损坏
	if !omitMeta {
		mr.offsets = offsets
	} else {
		mr.offsets = make([]int64, len(mr.syncSQLs))
	}

	//schedule    string     //定时任务配置串
	if len(cronSchedule) > 0 {
		cronSchedule = strings.ToLower(cronSchedule)
		if strings.HasPrefix(cronSchedule, reader.Loop) {
			mr.loop = true
			mr.loopDuration, err = reader.ParseLoopDuration(cronSchedule)
			if err != nil {
				log.Errorf("Runner[%v] %v %v", mr.meta.RunnerName, mr.Name(), err)
				err = nil
			}
		} else {
			err = mr.Cron.AddFunc(cronSchedule, mr.run)
			if err != nil {
				return nil, err
			}
			log.Infof("Runner[%v] %v Cron job added with schedule <%v>", mr.meta.RunnerName, mr.Name(), cronSchedule)
		}

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

func restoreMeta(meta *reader.Meta, rawSqls string, magicLagDur time.Duration) (offsets []int64, sqls []string, omitMeta bool) {
	now := time.Now().Add(-magicLagDur)
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

func (tableRecords *TableRecords) restoreTableDone(meta *reader.Meta, database string, tables []string) bool {
	omitTableRecords := true
	tablesDoneRecord, err := meta.ReadDBDoneFile(database)
	if err != nil {
		log.Errorf("Runner[%v] %v -table done data is corrupted err:%v, omit table done data", meta.RunnerName, meta.DoneFilePath, err)
		return omitTableRecords
	}

	omitTableRecords = false
	for _, table := range tables {
		if contains(tablesDoneRecord, table) {
			tableRecords.SetTableInfo(table, TableInfo{
				size:   -1,
				offset: -1,
			})
		}
	}
	return omitTableRecords
}

func (dbRecords *DBRecords) restoreRecordsFile(meta *reader.Meta) bool {
	omitDoneDBRecords := true
	recordsDone, err := meta.ReadRecordsFile(DefaultDoneRecordsFile)
	if err != nil {
		log.Errorf("Runner[%v] %v -table done data is corrupted err:%v, omit table done data", meta.RunnerName, meta.DoneFilePath, err)
		return omitDoneDBRecords
	}

	omitDoneDBRecords = false
	for _, record := range recordsDone {
		tmpDBRecords := strings.Split(record, sqlOffsetConnector)
		if int64(len(tmpDBRecords)) != 2 {
			log.Errorf("Runner[%v] %v -meta records done file is not invalid sql records done file %v， omit meta data", meta.RunnerName, meta.MetaFile(), record)
			return omitDoneDBRecords
		}

		database := tmpDBRecords[0]
		var tableRecords TableRecords
		if dbRecords.GetTableRecords(database) != nil {
			tableRecords.Set((*dbRecords)[database])
		}

		tmpTablesRecords := TrimeList(strings.Split(tmpDBRecords[1], "@"))
		if int64(len(tmpTablesRecords)) < 1 {
			log.Errorf("Runner[%v] %v -meta records done file is not invalid sql records done file %v， omit meta data", meta.RunnerName, meta.MetaFile(), tmpDBRecords)
			return omitDoneDBRecords
		}

		for _, tableRecord := range tmpTablesRecords {
			tableRecordArr := strings.Split(tableRecord, ",")
			if int64(len(tableRecordArr)) != 4 {
				log.Errorf("Runner[%v] %v -meta records done file is not invalid sql records done file %v， omit meta data", meta.RunnerName, meta.MetaFile(), tableRecord)
				return omitDoneDBRecords
			}

			size, err := strconv.ParseInt(tableRecordArr[1], 10, 64)
			if err != nil {
				log.Errorf("Runner[%v] %v -meta file sql is out of date %v or parse size err %v， omit this offset", meta.RunnerName, meta.MetaFile(), tableRecordArr[1], err)
				size = -1
			}

			offset, err := strconv.ParseInt(tableRecordArr[1], 10, 64)
			if err != nil {
				log.Errorf("Runner[%v] %v -meta file sql is out of date %v or parse offset err %v， omit this offset", meta.RunnerName, meta.MetaFile(), tableRecordArr[1], err)
				offset = -1
			}

			tableInfo := TableInfo{
				size:   size,
				offset: offset,
			}
			tableRecords.SetTableInfo(tableRecordArr[0], tableInfo)
		}

		dbRecords.SetTableRecords(database, tableRecords)
	}

	return omitDoneDBRecords
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
	return ""
}

func convertMagicIndex(magic string, now time.Time) (ret string, index int) {
	switch magic {
	case "YYYY":
		return fmt.Sprintf("%d", now.Year()), YEAR
	case "YY":
		return fmt.Sprintf("%d", now.Year())[2:], YEAR
	case "MM":
		m := int(now.Month())
		return fmt.Sprintf("%02d", m), MONTH
	case "DD":
		d := int(now.Day())
		return fmt.Sprintf("%02d", d), DAY
	case "hh":
		h := now.Hour()
		return fmt.Sprintf("%02d", h), HOUR
	case "mm":
		m := now.Minute()
		return fmt.Sprintf("%02d", m), MINUTE
	case "ss":
		s := now.Second()
		return fmt.Sprintf("%02d", s), SECOND
	}
	return "", -1
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

	return ret
}

// 渲染魔法变量
func goMagicIndex(rawSql string, now time.Time) (ret string, startIndex, endIndex, timeIndex []int, err error) {
	startIndex = []int{-1, -1, -1, -1, -1, -1} // 记录YY,MM,DD,hh,mm,ss位置
	endIndex = make([]int, 6)                  // 记录YY,MM,DD,hh,mm,ss长度
	sps := strings.Split(rawSql, "@(")         //@()，对于每个分片找右括号
	ret = sps[0]
	recordIndex := len(ret)
	timeIndex = []int{0} // 按顺序记录时间的开始结束位置

	// 没有魔法变量的情况，例如 mytest*
	if len(sps) < 2 {
		if strings.Contains(ret, Wildcards) {
			timeIndex = append(timeIndex, recordIndex-1)
		} else {
			timeIndex = append(timeIndex, recordIndex)
		}
		return ret, startIndex, endIndex, timeIndex, nil
	}

	timeIndex = append(timeIndex, recordIndex)
	for idx := 1; idx < len(sps); idx++ {
		idxr := strings.Index(sps[idx], ")")
		if idxr == -1 {
			return rawSql, startIndex, endIndex, timeIndex, nil
		}
		spsStr := sps[idx][0:idxr]
		if len(spsStr) < 2 {
			err = fmt.Errorf(SupportReminder)
			return rawSql, startIndex, endIndex, timeIndex, err
		}
		res, index := convertMagicIndex(sps[idx][0:idxr], now)
		if index == -1 {
			err = fmt.Errorf(SupportReminder)
			return rawSql, startIndex, endIndex, timeIndex, err
		}

		// 记录日期起始位置
		startIndex[index] = recordIndex
		ret += res
		recordIndex = len(ret)

		// 记录日期终止位置
		endIndex[index] = recordIndex

		if idxr+1 < len(sps[idx]) {
			spsRemain := sps[idx][idxr+1:]
			ret += spsRemain
			if spsRemain == Wildcards {
				recordIndex = len(ret)
				continue
			}
			timeIndex = append(timeIndex, recordIndex)
			if strings.Contains(spsRemain, Wildcards) {
				timeIndex = append(timeIndex, len(ret)-1)
			} else {
				timeIndex = append(timeIndex, len(ret))
			}
			recordIndex = len(ret)
		}
	}

	return ret, startIndex, endIndex, timeIndex, nil
}

func checkMagic(rawSql string) (valid bool) {
	sps := strings.Split(rawSql, "@(") //@()，对于每个分片找右括号
	now := time.Now()

	for idx := 1; idx < len(sps); idx++ {
		idxr := strings.Index(sps[idx], ")")
		if idxr == -1 {
			return true
		}
		spsStr := sps[idx][0:idxr]
		if len(spsStr) < 2 {
			log.Errorf(SupportReminder)
			return false
		}

		_, index := convertMagicIndex(sps[idx][0:idxr], now)
		if index == -1 {
			log.Errorf(SupportReminder)
			return false
		}
	}

	return true
}

func (r *Reader) Name() string {
	return strings.ToUpper(r.dbtype) + "_Reader:" + r.database + "_" + Hash(r.rawsqls)
}

func (r *Reader) setStatsError(err string) {
	r.statsLock.Lock()
	defer r.statsLock.Unlock()
	r.stats.LastError = err
}

func (r *Reader) Status() StatsInfo {
	r.statsLock.RLock()
	defer r.statsLock.RUnlock()
	return r.stats
}

func (r *Reader) Source() string {
	//不能把DataSource弄出去，包含密码
	return r.dbtype + "_" + r.database
}

func (r *Reader) Close() (err error) {
	r.Cron.Stop()
	if atomic.CompareAndSwapInt32(&r.status, reader.StatusRunning, reader.StatusStopping) {
		log.Infof("Runner[%v] %v stopping", r.meta.RunnerName, r.Name())
	} else {
		atomic.CompareAndSwapInt32(&r.status, reader.StatusInit, reader.StatusStopped)
		close(r.readChan)
	}
	return nil
}

// Start 仅调用一次，借用 ReadData 启动，不能在 new 实例的时候启动，会有并发问题
func (r *Reader) Start() {
	r.mux.Lock()
	defer r.mux.Unlock()
	if r.started {
		return
	}
	if r.loop {
		go r.LoopRun()
	} else {
		r.Cron.Start()
		if r.execOnStart {
			go r.run()
		}
	}
	r.started = true
	log.Infof("Runner[%v] %v pull data daemon started", r.meta.RunnerName, r.Name())
}

func (r *Reader) ReadLine() (string, error) {
	return "", errors.New("method ReadLine is not supported, please use ReadData")
}

func (r *Reader) ReadData() (Data, int64, error) {
	if !r.started {
		r.Start()
	}

	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case info := <-r.readChan:
		return info.data, info.bytes, nil
	case <-timer.C:
	}

	return nil, 0, nil
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
func (r *Reader) updateOffsets(sqls []string) {
	for idx, sql := range sqls {
		if idx >= len(r.offsets) {
			r.offsets = append(r.offsets, 0)
			continue
		}
		if idx >= len(r.syncSQLs) {
			continue
		}
		if r.syncSQLs[idx] != sql {
			r.offsets[idx] = 0
		}
	}

	return
}

func (r *Reader) LoopRun() {
	for {
		if atomic.LoadInt32(&r.status) == reader.StatusStopped || atomic.LoadInt32(&r.status) == reader.StatusStopping {
			return
		}
		//run 函数里面处理stopping的逻辑
		r.run()
		time.Sleep(r.loopDuration)
	}
}

func (r *Reader) run() {
	var err error
	// 防止并发run
	for {
		if atomic.LoadInt32(&r.status) == reader.StatusStopped || atomic.LoadInt32(&r.status) == reader.StatusStopping {
			return
		}
		if atomic.CompareAndSwapInt32(&r.status, reader.StatusInit, reader.StatusRunning) {
			break
		}
		//节省CPU
		time.Sleep(time.Microsecond)
	}
	//double check
	if atomic.LoadInt32(&r.status) == reader.StatusStopped || atomic.LoadInt32(&r.status) == reader.StatusStopping {
		return
	}
	// running时退出 状态改为Init，以便 cron 调度下次运行
	// stopping时推出改为 stopped，不再运行
	defer func() {
		atomic.CompareAndSwapInt32(&r.status, reader.StatusRunning, reader.StatusInit)
		if atomic.CompareAndSwapInt32(&r.status, reader.StatusStopping, reader.StatusStopped) {
			close(r.readChan)
		}
		if err == nil {
			log.Infof("Runner[%v] %v successfully finished", r.meta.RunnerName, r.Name())
		}
	}()

	now := time.Now().Add(-r.magicLagDur)
	r.database = goMagic(r.rawDatabase, now)
	r.table = goMagic(r.rawTable, now)

	var connectStr string
	switch r.dbtype {
	case reader.ModeMySQL:
		connectStr = getConnectStr(r.datasource, "", r.encoder)
	case reader.ModeMSSQL:
		connectStr = r.datasource + ";database=" + r.database
	case reader.ModePostgreSQL:
		spls := strings.Split(r.datasource, " ")
		contains := false
		for idx, v := range spls {
			if strings.Contains(v, "dbname") {
				contains = true
				spls[idx] = "dbname=" + r.database
			}
		}
		if !contains {
			spls = append(spls, "dbname="+r.database)
		}
		connectStr = strings.Join(spls, " ")
	}
	// 开始work逻辑
	for {
		if atomic.LoadInt32(&r.status) == reader.StatusStopping {
			log.Warnf("Runner[%v] %v stopped from running", r.meta.RunnerName, r.Name())
			return
		}
		err = r.exec(connectStr)
		if err == nil {
			log.Infof("Runner[%v] %v successfully exec", r.meta.RunnerName, r.Name())
			return
		}
		log.Error(err)
		r.setStatsError(err.Error())
		time.Sleep(3 * time.Second)
	}
}

func (r *Reader) getInitScans(length int, rows *sql.Rows, sqltype string) (scanArgs []interface{}, nochoiced []bool) {
	nochoice := make([]interface{}, length)
	nochoiced = make([]bool, length)
	for i := range scanArgs {
		nochoice[i] = new(interface{})
		nochoiced[i] = true
	}
	defer func() {
		if r := recover(); r != nil {
			log.Error("Recovered in f", r)
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
		log.Errorf("Runner[%v] %v getInitScans length is %v not equal to columetypes %v", length, len(tps))
		scanArgs = nochoice
	}
	scanArgs = make([]interface{}, length)
	for i, v := range tps {
		nochoiced[i] = false
		scantype := v.ScanType().Name()
		switch scantype {
		case "int64", "int32", "int16", "int", "int8":
			scanArgs[i] = new(int64)
		case "float32", "float64":
			scanArgs[i] = new(float64)
		case "uint", "uint8", "uint16", "uint32", "uint64":
			scanArgs[i] = new(uint64)
		case "bool":
			scanArgs[i] = new(bool)
		case "[]uint8":
			scanArgs[i] = new([]byte)
		case "string", "RawBytes", "time.Time", "NullTime":
			//时间类型也作为string处理
			scanArgs[i] = new(interface{})
			if _, ok := r.schemas[v.Name()]; !ok {
				r.schemas[v.Name()] = "string"
			}
		case "NullInt64":
			scanArgs[i] = new(interface{})
			if _, ok := r.schemas[v.Name()]; !ok {
				r.schemas[v.Name()] = "long"
			}
		case "NullFloat64":
			scanArgs[i] = new(interface{})
			if _, ok := r.schemas[v.Name()]; !ok {
				r.schemas[v.Name()] = "float"
			}
		default:
			scanArgs[i] = new(interface{})
			nochoiced[i] = true
		}
		log.Infof("Runner[%v] %v Init field %v scan type is %v ", r.meta.RunnerName, r.Name(), v.Name(), scantype)
	}

	return scanArgs, nochoiced
}

func (r *Reader) getOffsetIndex(columns []string) int {
	offsetKeyIndex := -1
	for idx, key := range columns {
		if key == r.offsetKey {
			return idx
		}
	}
	return offsetKeyIndex
}

func (r *Reader) exec(connectStr string) (err error) {
	now := time.Now().Add(-r.magicLagDur)
	db, err := openSql(r.dbtype, connectStr, r.Name())
	if err != nil {
		return err
	}
	defer db.Close()
	if err = db.Ping(); err != nil {
		return err
	}

	// 获取符合条件的数据库
	dbs, err := r.getDBs(db, now)
	if err != nil {
		return err
	}

	log.Infof("Runner[%v] %v get valid databases: %v", r.meta.RunnerName, r.Name(), dbs)

	if r.rawsqls == "" {
		go func() {
			// 获取数据库所有条数
			r.countDB(dbs, now)
			return
		}()
	}

	for _, currentDB := range dbs {
		var recordTablesDone TableRecords
		if !r.omitDoneDBRecords {
			tableRecords := r.doneRecords.GetTableRecords(currentDB)
			recordTablesDone.Set(tableRecords)
		}
		err = r.execReadDB(currentDB, now, recordTablesDone)
		if err != nil {
			log.Errorf("Runner[%v] %v exect read db: %v error: %v", r.meta.RunnerName, r.Name(), currentDB, err)
		}
		if atomic.LoadInt32(&r.status) == reader.StatusStopping || atomic.LoadInt32(&r.status) == reader.StatusStopped {
			log.Warnf("Runner[%v] %v stopped from running", r.meta.RunnerName, r.Name())
			return nil
		}
	}

	return nil
}

func (r *Reader) countDB(dbs []string, now time.Time) {
	for _, curDb := range dbs {
		var recordTablesDone TableRecords
		if !r.omitDoneDBRecords {
			tableRecords := r.doneRecords.GetTableRecords(curDb)
			recordTablesDone.Set(tableRecords)
		}
		err := r.execCountDB(curDb, now, recordTablesDone)
		if err != nil {
			log.Errorf("Runner[%v] %v get current database: %v count error: %v", r.meta.RunnerName, r.Name(), curDb, err)
		}
		if atomic.LoadInt32(&r.status) == reader.StatusStopping || atomic.LoadInt32(&r.status) == reader.StatusStopped {
			log.Warnf("Runner[%v] %v stopped from running", r.meta.RunnerName, r.Name())
			return
		}
	}
}

func (r *Reader) execCountDB(curDb string, now time.Time, recordTablesDone TableRecords) error {
	connectStr := getConnectStr(r.datasource, curDb, r.encoder)
	db, err := openSql(r.dbtype, connectStr, r.Name())
	if err != nil {
		return err
	}
	defer func() {
		db.Close()
	}()
	if err = db.Ping(); err != nil {
		return err
	}
	log.Infof("Runner[%v] %v prepare %v change database success, current database is: %v", r.meta.RunnerName, r.Name(), r.dbtype, curDb)
	r.database = curDb

	//更新sqls
	var tables []string
	var sqls string
	if r.rawsqls == "" {
		// 获取符合条件的数据表，并且将计算表中记录数的query语句赋给 r.rawsqls
		tables, sqls, err = r.getDatas(db, r.rawTable, now, COUNT)
		if err != nil {
			return err
		}

		log.Debugf("Runner[%v] %v default count sqls %v", r.meta.RunnerName, r.Name(), r.rawsqls)

		if r.omitDoneDBRecords == true {
			// 兼容
			recordTablesDone.restoreTableDone(r.meta, r.database, tables)
		}
	}

	if r.rawsqls != "" {
		sqls = r.rawsqls
	}
	sqlsSlice := updateSqls(sqls, now)
	log.Infof("Runner[%v] %v start to work, sqls %v offsets %v", r.meta.RunnerName, r.Name(), sqlsSlice, r.offsets)
	tablesLen := len(tables)

	for idx, rawSql := range sqlsSlice {
		//分sql执行
		if r.rawsqls == "" && idx < tablesLen {
			if recordTablesDone.GetTableInfo(tables[idx]) != (TableInfo{}) {
				continue
			}
		}

		// 每张表的记录数
		var tableSize int64
		tableSize, err = r.execTableCount(db, idx, rawSql)
		if err != nil {
			return err
		}

		// 符合记录的数据库和表的记录总数
		r.addCount(tableSize)

		if atomic.LoadInt32(&r.status) == reader.StatusStopping || atomic.LoadInt32(&r.status) == reader.StatusStopped {
			log.Warnf("Runner[%v] %v stopped from running", r.meta.RunnerName, r.Name())
			return nil
		}
	}

	return nil
}

func (r *Reader) execReadDB(curDb string, now time.Time, recordTablesDone TableRecords) (err error) {
	connectStr := getConnectStr(r.datasource, curDb, r.encoder)
	db, err := openSql(r.dbtype, connectStr, r.Name())
	if err != nil {
		return err
	}
	defer func() {
		db.Close()
	}()
	if err = db.Ping(); err != nil {
		return err
	}
	log.Infof("Runner[%v] %v prepare %v change database success, current database is: %v", r.meta.RunnerName, r.Name(), r.dbtype, curDb)
	r.database = curDb

	//更新sqls
	var tables []string
	var sqls string
	if r.rawsqls == "" {
		// 获取符合条件的数据表，并且将获取表中所有记录的语句赋给 r.rawsqls
		tables, sqls, err = r.getDatas(db, r.rawTable, now, TABLE)
		if err != nil {
			log.Errorf("Runner[%v] %v db %v rawTable: %v get tables and sqls error %v", r.meta.RunnerName, r.Name(), curDb, r.rawTable, r.rawsqls, err)
			if len(tables) == 0 && sqls == "" {
				return err
			}
		}

		log.Infof("Runner[%v] %v default sqls %v", r.meta.RunnerName, r.Name(), r.rawsqls)

		if r.omitDoneDBRecords {
			// 兼容
			recordTablesDone.restoreTableDone(r.meta, r.database, tables)
			r.syncRecords.SetTableRecords(curDb, recordTablesDone)
		}
	}
	log.Infof("Runner[%v] %v get valid tables: %v, recordTablesDone: %v", r.meta.RunnerName, r.Name(), tables, recordTablesDone)

	var sqlsSlice []string
	sqlsSlice = updateSqls(sqls, now)
	if r.rawsqls != "" {
		sqlsSlice = updateSqls(r.rawsqls, now)
		r.updateOffsets(sqlsSlice)
	}
	r.syncSQLs = sqlsSlice
	tablesLen := len(tables)
	log.Infof("Runner[%v] %v start to work, sqls %v offsets %v", r.meta.RunnerName, r.Name(), r.syncSQLs, r.offsets)

	for idx, rawSql := range r.syncSQLs {
		//分sql执行
		var isRawSql bool
		exit := false
		var tableName string
		var readSize int64
		tmpTablesRecords := r.syncRecords.GetTableRecords(curDb)
		for !exit {
			if r.rawsqls == "" && idx < tablesLen {
				tableName = tables[idx]
				if recordTablesDone.GetTableInfo(tableName) != (TableInfo{}) {
					break
				}
			}
			// 执行每条 sql 语句
			exit, isRawSql, readSize = r.execReadSql(db, idx, rawSql, tables)

			if r.rawsqls == "" {
				tmpTablesRecords.SetTableInfo(tableName, TableInfo{size: readSize, offset: -1})
				r.syncRecords.SetTableRecords(curDb, tmpTablesRecords)
				recordTablesDone.SetTableInfo(tableName, TableInfo{size: readSize, offset: -1})
			}

			if atomic.LoadInt32(&r.status) == reader.StatusStopping || atomic.LoadInt32(&r.status) == reader.StatusStopped {
				log.Warnf("Runner[%v] %v stopped from running", r.meta.RunnerName, r.Name())
				return nil
			}

			if isRawSql {
				log.Infof("Runner[%v] %v is raw SQL, exit after exec once...", r.meta.RunnerName, r.Name())
				break
			}
		}
	}

	return nil
}

// WriteRecordsFile 将当前文件写入donefiel中
func WriteRecordsFile(doneFilePath, content string) (err error) {
	var f *os.File
	filename := fmt.Sprintf("%v.%v", reader.DoneFileName, DefaultDoneRecordsFile)
	filePath := filepath.Join(doneFilePath, filename)
	// write to tmp file
	f, err = os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_APPEND, DefaultFilePerm)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.Write([]byte(content))
	if err != nil {
		return err
	}

	return f.Sync()
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
		log.Errorf("sql reader convertString for type %v is not supported", reflect.TypeOf(idv))
	}
	return "", fmt.Errorf("%v type can not convert to string", dv.Kind())
}

func (r *Reader) getSQL(idx int, rawSQL string) (sql string, err error) {
	rawSQL = strings.TrimSuffix(strings.TrimSpace(rawSQL), ";")
	switch r.dbtype {
	case reader.ModeMySQL:
		if len(r.offsetKey) > 0 {
			sql = fmt.Sprintf("%s WHERE %v >= %d AND %v < %d;", rawSQL, r.offsetKey, r.offsets[idx], r.offsetKey, r.offsets[idx]+int64(r.readBatch))
		} else {
			sql = fmt.Sprintf("%s", rawSQL)
		}
	case reader.ModeMSSQL:
		if len(r.offsetKey) > 0 {
			sql = fmt.Sprintf("%s WHERE CAST(%v AS BIGINT) >= %d AND CAST(%v AS BIGINT) < %d;", rawSQL, r.offsetKey, r.offsets[idx], r.offsetKey, r.offsets[idx]+int64(r.readBatch))
		} else {
			err = fmt.Errorf("%v dbtype is not support get SQL without id now", r.dbtype)
		}
	case reader.ModePostgreSQL:
		if len(r.offsetKey) > 0 {
			sql = fmt.Sprintf("%s WHERE %v >= %d AND %v < %d;", rawSQL, r.offsetKey, r.offsets[idx], r.offsetKey, r.offsets[idx]+int64(r.readBatch))
		} else {
			err = fmt.Errorf("%v dbtype is not support get SQL without id now", r.dbtype)
		}
	default:
		err = fmt.Errorf("%v dbtype is not support get SQL now", r.dbtype)
	}

	return sql, err
}

func (r *Reader) checkExit(idx int, db *sql.DB) (bool, int64) {
	if len(r.offsetKey) <= 0 {
		return true, -1
	}
	rawSQL := r.syncSQLs[idx]
	rawSQL = strings.TrimSuffix(strings.TrimSpace(rawSQL), ";")
	var tsql string
	if r.dbtype == reader.ModeMySQL {
		tsql = fmt.Sprintf("%s WHERE %v >= %d order by %v limit 1;", rawSQL, r.offsetKey, r.offsets[idx], r.offsetKey)
	} else {
		ix := strings.Index(rawSQL, "from")
		if ix < 0 {
			return true, -1
		}
		rawSQL = rawSQL[ix:]
		tsql = fmt.Sprintf("select top(1) * %v WHERE CAST(%v AS BIGINT) >= %v order by CAST(%v AS BIGINT);", rawSQL, r.offsetKey, r.offsets[idx], r.offsetKey)
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
		log.Errorf("Runner[%v] %v prepare %v columns error %v", r.meta.RunnerName, r.Name(), r.dbtype, err)
		return true, -1
	}

	scanArgs, _ := r.getInitScans(len(columns), rows, r.dbtype)
	offsetKeyIndex := r.getOffsetIndex(columns)
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
func (r *Reader) SyncMeta() {
	if r.rawsqls == "" {
		now := time.Now().String()
		var all string
		for database, tablesRecord := range r.syncRecords {
			var tablesRecordStr string
			for table, tableInfo := range tablesRecord {
				tablesRecordStr += table + "," +
					strconv.FormatInt(tableInfo.size, 10) + "," +
					strconv.FormatInt(tableInfo.offset, 10) + "," +
					now + "@"
			}
			if tablesRecordStr == "" {
				continue
			}
			all += database + sqlOffsetConnector + tablesRecordStr + "\n"
		}

		if err := WriteRecordsFile(r.meta.DoneFilePath, all); err != nil {
			log.Errorf("Runner[%v] %v SyncMeta error %v", r.meta.RunnerName, r.Name(), err)
			return
		}

		r.syncRecords.Reset()
		return
	}

	encodeSQLs := make([]string, 0)
	for _, sql := range r.syncSQLs {
		encodeSQLs = append(encodeSQLs, strings.Replace(sql, " ", "@", -1))
	}
	for _, offset := range r.offsets {
		encodeSQLs = append(encodeSQLs, strconv.FormatInt(offset, 10))
	}
	all := strings.Join(encodeSQLs, sqlOffsetConnector)
	if err := r.meta.WriteOffset(all, int64(len(r.syncSQLs))); err != nil {
		log.Errorf("Runner[%v] %v SyncMeta error %v", r.meta.RunnerName, r.Name(), err)
	}
	return
}

func (r *Reader) SetMode(mode string, v interface{}) error {
	return errors.New("SqlReader not support readmode")
}

func contains(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}

// 查看时间是否符合
func validTime(str, match string, startIndex, endIndex []int) (valid bool) {
	for idx, record := range startIndex {
		if record == -1 {
			continue
		}

		if len(str) < endIndex[idx] {
			return false
		}

		// 比较大小
		curStr := str[record:endIndex[idx]]
		curInt, err := strconv.Atoi(curStr)
		if err != nil {
			return false
		}
		matchStr := match[record:endIndex[idx]]
		matchInt, err := strconv.Atoi(matchStr)
		if err != nil {
			return false
		}

		// 小于
		if curInt < matchInt {
			return true
		}

		if curInt > matchInt {
			return false
		}

		// 相等
		valid = true
	}

	return true
}

type DataQuery struct {
	validData []string
	sqls      string
}

func (r *Reader) getValidData(db *sql.DB, matchData, matchStr string, startIndex, endIndex, timeIndex []int, queryType int) (validData []string, sqls string, err error) {
	// get all databases and check validate database
	query, err := r.getQuery(queryType)
	if err != nil {
		return validData, sqls, err
	}

	rowsDBs, err := db.Query(query)
	if err != nil {
		log.Errorf("Runner[%v] %v prepare %v <%v> query error %v", r.meta.RunnerName, r.Name(), r.dbtype, query, err)
		return validData, sqls, err
	}
	defer rowsDBs.Close()

	validData = make([]string, 0)
	for rowsDBs.Next() {
		var s string
		err = rowsDBs.Scan(&s)
		if err != nil {
			log.Errorf("Runner[%v] %v scan rows error %v", r.meta.RunnerName, r.Name(), err)
			continue
		}

		// 检查是否已经读过
		if r.checkDoneRecords(queryType, s) {
			continue
		}

		if matchData != "" {
			// 字符匹配
			match := matchRemainStr(s, matchStr, timeIndex)
			log.Debugf("Runner[%v] %v current data: %v, current time data: %v, remain str: %v, timeIndex: %v, isMatch: %v", r.meta.RunnerName, r.Name(), s, matchData, matchStr, timeIndex, match)
			if !match || !validTime(s, matchData, startIndex, endIndex) {
				continue
			}
		}

		rawSql, err := getRawSqls(queryType, s)
		if err != nil {
			return validData, sqls, err
		}
		sqls += rawSql

		validData = append(validData, s)
	}

	return validData, sqls, nil
}

func getConnectStr(datasource, database, encoder string) (connectStr string) {
	connectStr = datasource + "/" + database
	if encoder != "" {
		connectStr += "?charset=" + encoder
	}
	return connectStr
}

func openSql(dbtype, connectStr, name string) (db *sql.DB, err error) {
	db, err = sql.Open(dbtype, connectStr)
	if err != nil {
		err = fmt.Errorf("%v open %v failed: %v", name, dbtype, err)
		return nil, err
	}
	return db, nil
}

func matchRemainStr(origin, match string, timeIndex []int) bool {
	if len(timeIndex) > 0 && len(origin) < timeIndex[len(timeIndex)-1] {
		return false
	}

	remainStr := getRemainStr(origin, timeIndex)
	if len(remainStr) < len(match) ||
		remainStr[:len(match)] != match {
		return false
	}

	return true
}

func getRemainStr(origin string, timeIndex []int) (remainStr string) {
	if len(timeIndex)%2 != 0 {
		return origin
	}

	for idx := 0; idx < len(timeIndex); {
		remainStr += origin[timeIndex[idx]:timeIndex[idx+1]]
		idx = idx + 2
	}

	return remainStr
}

func (r *Reader) Lag() (rl *LagInfo, err error) {
	rl = &LagInfo{SizeUnit: "records"}
	if r.rawsqls == "" {
		count := r.getCount()
		rl.Size = count - r.CurrentCount
		rl.Total = count
	}

	return rl, nil
}

func getDefaultSql(database, dbtype string) (defaultSql string, err error) {
	switch dbtype {
	case reader.ModeMySQL:
		defaultSql = strings.Replace(DefaultMySQLTable, "DATABASE_NAME", database, -1)
	case reader.ModePostgreSQL:
		defaultSql = DefaultPGSQLTable
	case reader.ModeMSSQL:
		defaultSql = strings.Replace(DefaultMsSQLTable, "DATABASE_NAME", database, -1)
	default:
		return "", fmt.Errorf("not support reader type: %v", dbtype)
	}
	return defaultSql, nil
}

// 根据queryType获取符合要求的数据和需要执行的原始sql语句mr.rawsqls
// queryType 可以为TABLE DATABASE COUNT
func (r *Reader) getDatas(db *sql.DB, rawData string, now time.Time, queryType int) (datas []string, rawsqls string, err error) {
	var startIndex, endIndex, timeIndex []int
	var matchData string

	// 是否导入历史数据
	checkHistory, err := r.getCheckHistory(queryType)
	if err != nil {
		return datas, rawsqls, err
	}
	if !checkHistory {
		// 非导入历史数据，正常情况下获得数据
		datas, rawsqls, err = r.getGeneralDatas(db, queryType)
		if err != nil {
			return datas, rawsqls, err
		}
		return datas, rawsqls, nil
	}

	// 获取符合条件的历史数据
	// 先获取 转换magic后的值
	// 再获取除时间以外剩余的字符串，进行字符串匹配，如果匹配，继续判断时间不符合，不匹配跳过
	// 最后判断时间是否符合，时间小于等于，则匹配，加入返回数组，不匹配跳过
	matchData, startIndex, endIndex, timeIndex, err = goMagicIndex(rawData, now)
	if err != nil {
		return datas, rawsqls, err
	}

	datas = make([]string, 0)
	if matchData == rawData && !strings.Contains(rawData, Wildcards) {
		datas = append(datas, matchData)
		return datas, rawsqls, nil
	}

	matchStr := getRemainStr(matchData, timeIndex)
	datas, rawsqls, err = r.getValidData(db, matchData, matchStr, startIndex, endIndex, timeIndex, queryType)
	if err != nil {
		return datas, rawsqls, err
	}

	return datas, rawsqls, nil
}

// 是否拿历史数据
func (r *Reader) getCheckHistory(queryType int) (checkHistory bool, err error) {
	switch queryType {
	case TABLE, COUNT:
		checkHistory = r.historyAll && r.rawTable != "*"
	case DATABASE:
		checkHistory = r.historyAll && r.rawDatabase != "*"
	default:
		return false, fmt.Errorf("%v queryType is not support get sql now", queryType)
	}

	return checkHistory, nil
}

// 根据 queryType 获取表中所有记录或者表中所有数据的条数的sql语句
func getRawSqls(queryType int, table string) (sqls string, err error) {
	switch queryType {
	case TABLE:
		sqls += "Select * From `" + table + "`;"
	case COUNT:
		sqls += "Select Count(*) From `" + table + "`;"
	case DATABASE:
	default:
		return "", fmt.Errorf("%v queryType is not support get sql now", queryType)
	}

	return sqls, nil
}

// 根据 queryType 获取query语句
func (r *Reader) getQuery(queryType int) (query string, err error) {
	switch queryType {
	case TABLE, COUNT:
		query, err = getDefaultSql(r.database, r.dbtype)
	case DATABASE:
		query = DefaultMySQLDatabase
	default:
		return "", fmt.Errorf("%v queryType is not support get sql now", queryType)
	}

	return query, nil
}

// 计算每个table的记录条数
func (r *Reader) execTableCount(db *sql.DB, idx int, rawSql string) (tableSize int64, err error) {
	execSQL, err := r.getSQL(idx, rawSql)
	if err != nil {
		log.Errorf("Runner[%v] get SQL error %v, use raw SQL", r.meta.RunnerName, err)
		execSQL = rawSql
	}
	log.Infof("Runner[%v] reader <%v> exec sql <%v>", r.meta.RunnerName, r.Name(), execSQL)
	rows, err := db.Query(execSQL)
	if err != nil {
		log.Errorf("Runner[%v] %v prepare %v <%v> query error %v", r.meta.RunnerName, r.Name(), r.dbtype, execSQL, err)
		return 0, err
	}
	defer rows.Close()

	// Fetch rows
	for rows.Next() {
		var s string
		err = rows.Scan(&s)
		if err != nil {
			log.Errorf("Runner[%v] %v scan rows error %v", r.meta.RunnerName, r.Name(), err)
			return 0, err
		}

		tableSize, err = strconv.ParseInt(s, 10, 64)
		if err != nil {
			log.Errorf("Runner[%v] %v convert string to int64 error %v", r.meta.RunnerName, r.Name(), err)
			return 0, err
		}
	}

	return tableSize, nil
}

// 执行每条 sql 语句
func (r *Reader) execReadSql(db *sql.DB, idx int, rawSql string, tables []string) (exit bool, isRawSql bool, readSize int64) {
	exit = true

	execSQL, err := r.getSQL(idx, r.syncSQLs[idx])
	if err != nil {
		log.Errorf("Runner[%v] get SQL error %v, use raw SQL", r.meta.RunnerName, err)
		execSQL = rawSql
	}

	if execSQL == rawSql {
		isRawSql = true
	}

	log.Infof("Runner[%v] reader <%v> exec sql <%v>", r.meta.RunnerName, r.Name(), execSQL)
	rows, err := db.Query(execSQL)
	if err != nil {
		log.Errorf("Runner[%v] %v prepare %v <%v> query error %v", r.meta.RunnerName, r.Name(), r.dbtype, execSQL, err)
		return exit, isRawSql, readSize
	}
	defer rows.Close()
	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		log.Errorf("Runner[%v] %v prepare %v <%v> columns error %v", r.meta.RunnerName, r.Name(), r.dbtype, execSQL, err)
		return exit, isRawSql, readSize
	}
	log.Infof("Runner[%v] SQL ：<%v>, schemas: <%v>", r.meta.RunnerName, execSQL, strings.Join(columns, ", "))
	scanArgs, nochiced := r.getInitScans(len(columns), rows, r.dbtype)
	var offsetKeyIndex int
	if r.rawsqls != "" {
		offsetKeyIndex = r.getOffsetIndex(columns)
	}

	// Fetch rows
	var maxOffset int64 = -1
	for rows.Next() {
		exit = false
		// get RawBytes from data
		err = rows.Scan(scanArgs...)
		if err != nil {
			log.Errorf("Runner[%v] %v scan rows error %v", r.meta.RunnerName, r.Name(), err)
			continue
		}

		var totalBytes int64
		data := make(Data, len(scanArgs))
		for i := 0; i < len(scanArgs); i++ {
			var bytes int64
			vtype, ok := r.schemas[columns[i]]
			if !ok {
				vtype = "unknown"
			}
			switch vtype {
			case "long":
				val, serr := convertLong(scanArgs[i])
				if serr != nil {
					log.Errorf("Runner[%v] %v convertLong for %v (%v) error %v, ignore this key...", r.meta.RunnerName, r.Name(), columns[i], scanArgs[i], serr)
				} else {
					data[columns[i]] = val
					bytes = 8
				}
			case "float":
				val, serr := convertFloat(scanArgs[i])
				if serr != nil {
					log.Errorf("Runner[%v] %v convertFloat for %v (%v) error %v, ignore this key...", r.meta.RunnerName, r.Name(), columns[i], scanArgs[i], serr)
				} else {
					data[columns[i]] = val
					bytes = 8
				}
			case "string":
				val, serr := convertString(scanArgs[i])
				if serr != nil {
					log.Errorf("Runner[%v] %v convertString for %v (%v) error %v, ignore this key...", r.meta.RunnerName, r.Name(), columns[i], scanArgs[i], serr)
				} else {
					data[columns[i]] = val
					bytes = int64(len(val))
				}
			default:
				dealed := false
				if !nochiced[i] {
					dealed = true
					switch d := scanArgs[i].(type) {
					case *string:
						data[columns[i]] = *d
						bytes = int64(len(*d))
					case *[]byte:
						data[columns[i]] = string(*d)
						bytes = int64(len(*d))
					case *bool:
						data[columns[i]] = *d
						bytes = 1
					case int64:
						data[columns[i]] = d
						bytes = 8
					case *int64:
						data[columns[i]] = *d
						bytes = 8
					case float64:
						data[columns[i]] = d
						bytes = 8
					case *float64:
						data[columns[i]] = *d
						bytes = 8
					case uint64:
						data[columns[i]] = d
						bytes = 8
					case *uint64:
						data[columns[i]] = *d
						bytes = 8
					case *interface{}:
						dealed = false
					default:
						dealed = false
					}
				}
				if !dealed {
					val, serr := convertString(scanArgs[i])
					if serr != nil {
						log.Errorf("Runner[%v] %v convertString for %v (%v) error %v, ignore this key...", r.meta.RunnerName, r.Name(), columns[i], scanArgs[i], serr)
					} else {
						data[columns[i]] = val
						bytes = int64(len(val))
					}
				}
			}

			totalBytes += bytes
		}
		if atomic.LoadInt32(&r.status) == reader.StatusStopping || atomic.LoadInt32(&r.status) == reader.StatusStopped {
			log.Warnf("Runner[%v] %v stopped from running", r.meta.RunnerName, r.Name())
			return exit, isRawSql, readSize
		}
		r.readChan <- readInfo{data, totalBytes}
		r.CurrentCount++
		readSize++

		if r.historyAll || r.rawsqls == "" {
			continue
		}

		maxOffset = r.updateOffset(idx, offsetKeyIndex, maxOffset, scanArgs)
	}

	if maxOffset > 0 {
		r.offsets[idx] = maxOffset + 1
	}
	if exit {
		var newOffsetIdx int64
		exit, newOffsetIdx = r.checkExit(idx, db)
		if !exit {
			r.offsets[idx] += int64(r.readBatch)
			if newOffsetIdx > r.offsets[idx] {
				r.offsets[idx] = newOffsetIdx
			}
		} else {
			log.Infof("Runner[%v] %v no data any more, exit...", r.meta.RunnerName, r.Name())
		}
	}

	return exit, isRawSql, readSize
}

func (r *Reader) getGeneralDatas(db *sql.DB, queryType int) (datas []string, sqls string, err error) {
	// 拿到数据库中所有表及对应的sql语句
	datas, sqls, err = r.getValidData(db, "", "", []int{}, []int{}, []int{}, queryType)
	if err != nil {
		return datas, sqls, err
	}

	return datas, sqls, nil
}

func (r *Reader) updateOffset(idx, offsetKeyIndex int, maxOffset int64, scanArgs []interface{}) int64 {
	if offsetKeyIndex >= 0 {
		var tmpOffsetIndex int64
		tmpOffsetIndex, err := convertLong(scanArgs[offsetKeyIndex])
		if err != nil {
			log.Errorf("Runner[%v] %v offset key value parse error %v, offset was not recorded", r.meta.RunnerName, r.Name(), err)
		} else if tmpOffsetIndex > maxOffset {
			maxOffset = tmpOffsetIndex
		}
	} else {
		r.offsets[idx]++
	}

	return maxOffset
}

func (r *Reader) addCount(current int64) {
	r.countLock.Lock()
	defer r.countLock.Unlock()
	r.count += current
}

func (r *Reader) getCount() int64 {
	r.countLock.RLock()
	defer r.countLock.RUnlock()
	return r.count
}

func (r *Reader) checkDoneRecords(queryType int, target string) bool {
	if queryType != TABLE {
		return false
	}

	tableDoneRecords := r.doneRecords.GetTableRecords(r.database)
	if tableDoneRecords == nil {
		return false
	}

	tableInfo := tableDoneRecords.GetTableInfo(target)
	if tableInfo == (TableInfo{}) {
		return false
	}

	return true
}

func (r *Reader) getDBs(db *sql.DB, now time.Time) ([]string, error) {
	dbsAll, _, err := r.getDatas(db, r.rawDatabase, now, DATABASE)
	if err != nil {
		return dbsAll, err
	}

	dbs := make([]string, 0)
	for _, db := range dbsAll {
		if contains(MysqlSystemDB, strings.ToLower(db)) {
			continue
		}
		dbs = append(dbs, db)
	}

	return dbs, nil
}
