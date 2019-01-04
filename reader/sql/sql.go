package sql

import (
	"database/sql"
	"errors"
	"fmt"
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
	. "github.com/qiniu/logkit/reader/config"
	"github.com/qiniu/logkit/utils/magic"
	. "github.com/qiniu/logkit/utils/models"
)

const (
	sqlOffsetConnector   = "##"
	sqlSpliter           = ";"
	DefaultMySQLTable    = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA='DATABASE_NAME';"
	DefaultMySQLDatabase = "SHOW DATABASES;"
	DefaultPGSQLTable    = "SELECT TABLENAME FROM PG_TABLES WHERE SCHEMANAME='SCHEMA_NAME';"
	DefaultMSSQLTable    = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_CATALOG='DATABASE_NAME' AND TABLE_SCHEMA='SCHEMA_NAME';"

	SupportReminder = "history all magic only support @(YYYY) @(YY) @(MM) @(DD) @(hh) @(mm) @(ss)"
	Wildcards       = "*"

	DefaultDoneRecordsFile = "sql.records"
	TimestampRecordsFile   = "timestamp.records"
	CacheMapFile           = "cachemap.records"
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

const (
	// 获取数据条数的函数
	COUNTFUNC = iota
	// 获取读取数据的函数
	READFUNC
)

var (
	_ reader.DaemonReader = &Reader{}
	_ reader.StatsReader  = &Reader{}
	_ reader.DataReader   = &Reader{}
	_ reader.Reader       = &Reader{}
)

var MysqlSystemDB = []string{"information_schema", "performance_schema", "mysql", "sys"}

func init() {
	reader.RegisterConstructor(ModeMySQL, NewReader)
	reader.RegisterConstructor(ModeMSSQL, NewReader)
	reader.RegisterConstructor(ModePostgreSQL, NewReader)
}

type readInfo struct {
	data  Data
	bytes int64
	json  string //排序去重时使用，其他时候无用
}

type Reader struct {
	meta *reader.Meta
	// Note: 原子操作，用于表示 reader 整体的运行状态
	status int32
	/*
		Note: 原子操作，用于表示获取数据的线程运行状态

		- StatusInit: 当前没有任务在执行
		- StatusRunning: 当前有任务正在执行
		- StatusStopping: 数据管道已经由上层关闭，执行中的任务完成时直接退出无需再处理
	*/
	routineStatus int32

	stopChan chan struct{}
	readChan chan readInfo
	errChan  chan error

	stats     StatsInfo
	statsLock sync.RWMutex

	dbtype      string //数据库类型
	datasource  string //数据源
	database    string //数据库名称
	rawDatabase string // 记录原始数据库
	rawSQLs     string // 原始sql执行列表
	historyAll  bool   // 是否导入历史数据
	rawTable    string // 记录原始数据库表名
	table       string // 数据库表名

	isLoop          bool
	loopDuration    time.Duration
	cronSchedule    bool //是否为定时任务
	execOnStart     bool
	Cron            *cron.Cron //定时任务
	readBatch       int        // 每次读取的数据量
	offsetKey       string
	timestampKey    string
	timestampKeyInt bool
	timestampMux    sync.RWMutex
	startTime       time.Time
	startTimeInt    int64
	timeCacheMap    map[string]string
	batchDuration   time.Duration
	batchDurInt     int

	encoder           string  // 解码方式
	offsets           []int64 // 当前处理文件的sql的offset
	muxOffsets        sync.RWMutex
	syncSQLs          []string      // 当前在查询的sqls
	syncRecords       SyncDBRecords // 将要append的记录
	doneRecords       SyncDBRecords // 已经读过的记录
	lastDatabase      string        // 读过的最后一条记录的数据库
	lastTable         string        // 读过的最后一条记录的数据表
	omitDoneDBRecords bool
	schemas           map[string]string
	dbSchema          string
	magicLagDur       time.Duration
	count             int64
	CurrentCount      int64
	countLock         sync.RWMutex

	firstPrinted bool
}

func NewReader(meta *reader.Meta, conf conf.MapConf) (reader.Reader, error) {
	var readBatch int
	var dbtype, dataSource, rawDatabase, rawSQLs, cronSchedule, offsetKey, encoder, table, dbSchema string
	var execOnStart, historyAll bool
	var (
		timestampkey   string
		startTime      time.Time
		batchDuration  time.Duration
		startTimeInt   int64
		batchDurInt    int
		pgtimestampInt bool
	)
	dbtype, _ = conf.GetStringOr(KeyMode, ModeMySQL)
	logpath, _ := conf.GetStringOr(KeyLogPath, "")

	var err error
	switch dbtype {
	case ModeMySQL:
		readBatch, _ = conf.GetIntOr(KeyMysqlReadBatch, 100)
		offsetKey, _ = conf.GetStringOr(KeyMysqlOffsetKey, "")
		if logpath == "" {
			dataSource, err = conf.GetPasswordEnvString(KeyMysqlDataSource)
		} else {
			dataSource, err = conf.GetPasswordEnvStringOr(KeyMysqlDataSource, logpath)
		}
		if err != nil {
			return nil, err
		}
		rawDatabase, _ = conf.GetStringOr(KeyMysqlDataBase, "")
		rawSQLs, _ = conf.GetStringOr(KeyMysqlSQL, "")
		cronSchedule, _ = conf.GetStringOr(KeyMysqlCron, "")
		execOnStart, _ = conf.GetBoolOr(KeyMysqlExecOnStart, true)
		encoder, _ = conf.GetStringOr(KeyMysqlEncoding, "utf8")
		if strings.Contains(encoder, "-") {
			encoder = strings.Replace(strings.ToLower(encoder), "-", "", -1)
		}
		historyAll, _ = conf.GetBoolOr(KeyMysqlHistoryAll, false)
		table, _ = conf.GetStringOr(KyeMysqlTable, "")
	case ModeMSSQL:
		readBatch, _ = conf.GetIntOr(KeyMssqlReadBatch, 100)
		offsetKey, _ = conf.GetStringOr(KeyMssqlOffsetKey, "")
		if logpath == "" {
			dataSource, err = conf.GetPasswordEnvString(KeyMssqlDataSource)
		} else {
			dataSource, err = conf.GetPasswordEnvStringOr(KeyMssqlDataSource, logpath)
		}
		if err != nil {
			return nil, err
		}
		rawDatabase, err = conf.GetString(KeyMssqlDataBase)
		if err != nil {
			return nil, err
		}
		dbSchema, _ = conf.GetStringOr(KeyMssqlSchema, "dbo")
		rawSQLs, _ = conf.GetStringOr(KeyMssqlSQL, "")
		cronSchedule, _ = conf.GetStringOr(KeyMssqlCron, "")
		execOnStart, _ = conf.GetBoolOr(KeyMssqlExecOnStart, true)
	case ModePostgreSQL:
		readBatch, _ = conf.GetIntOr(KeyPGsqlReadBatch, 100)
		offsetKey, _ = conf.GetStringOr(KeyPGsqlOffsetKey, "")
		if logpath == "" {
			dataSource, err = conf.GetPasswordEnvString(KeyPGsqlDataSource)
		} else {
			dataSource, err = conf.GetPasswordEnvStringOr(KeyPGsqlDataSource, logpath)
		}
		if err != nil {
			return nil, err
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
		timestampkey, _ = conf.GetStringOr(KeyPGtimestampKey, "")
		pgtimestampInt, _ = conf.GetBoolOr(KeyPGtimestampInt, false)
		if !pgtimestampInt {
			startTimeStr, _ := conf.GetStringOr(KeyPGStartTime, "")
			if startTimeStr != "" {
				startTime, err = parsePostgresDatetime(startTimeStr)
				if err != nil {
					return nil, fmt.Errorf("parse starttime %s error %v", startTimeStr, err)
				}
			} else {
				startTime = time.Now()
			}
			timestampDurationStr, _ := conf.GetStringOr(KeyPGBatchDuration, "1m")
			batchDuration, err = time.ParseDuration(timestampDurationStr)
			if err != nil {
				return nil, err
			}
		} else {
			startTimeInt, _ = conf.GetInt64Or(KeyPGStartTime, 0)
			batchDurInt, _ = conf.GetIntOr(KeyPGBatchDuration, 1000)
		}

		rawDatabase, err = conf.GetString(KeyPGsqlDataBase)
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
		dbSchema, _ = conf.GetStringOr(KeyPGsqlSchema, "public")
		rawSQLs, _ = conf.GetStringOr(KeyPGsqlSQL, "")
		cronSchedule, _ = conf.GetStringOr(KeyPGsqlCron, "")
		execOnStart, _ = conf.GetBoolOr(KeyPGsqlExecOnStart, true)
	default:
		err = fmt.Errorf("%v mode not support in sql reader", dbtype)
		return nil, err
	}
	rawSchemas, _ := conf.GetStringListOr(KeySQLSchema, []string{})
	magicLagDur, _ := conf.GetStringOr(KeyMagicLagDuration, "")
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
	if rawSQLs != "" {
		offsets, sqls, omitMeta = restoreMeta(meta, rawSQLs, mgld)
	}

	r := &Reader{
		meta:            meta,
		status:          StatusInit,
		routineStatus:   StatusInit,
		stopChan:        make(chan struct{}),
		readChan:        make(chan readInfo),
		errChan:         make(chan error),
		datasource:      dataSource,
		database:        rawDatabase,
		rawDatabase:     rawDatabase,
		rawSQLs:         rawSQLs,
		Cron:            cron.New(),
		readBatch:       readBatch,
		offsetKey:       offsetKey,
		timestampKey:    timestampkey,
		timestampKeyInt: pgtimestampInt,
		startTime:       startTime,
		startTimeInt:    startTimeInt,
		batchDuration:   batchDuration,
		batchDurInt:     batchDurInt,
		syncSQLs:        sqls,
		dbtype:          dbtype,
		execOnStart:     execOnStart,
		historyAll:      historyAll,
		rawTable:        table,
		table:           table,
		magicLagDur:     mgld,
		schemas:         schemas,
		encoder:         encoder,
		dbSchema:        dbSchema,
	}

	if r.rawDatabase == "" {
		r.rawDatabase = "*"
	}
	if r.rawTable == "" {
		r.rawTable = "*"
	}
	if r.timestampKey != "" {
		if r.timestampKeyInt {
			tm, cache, err := RestoreTimestampIntOffset(r.meta.DoneFilePath)
			if err == nil {
				r.startTimeInt = tm
				r.timestampMux.Lock()
				r.timeCacheMap = cache
				r.timestampMux.Unlock()
			} else {
				log.Errorf("RestoreTimestampIntOffset err %v", err)
			}
		} else {
			tm, cache, err := RestoreTimestampOffset(r.meta.DoneFilePath)
			if err == nil {
				r.startTime = tm
				r.timestampMux.Lock()
				r.timeCacheMap = cache
				r.timestampMux.Unlock()
			} else {
				log.Errorf("RestoreTimestampOffset err %v", err)
			}
		}
	}

	if r.rawSQLs == "" {
		valid := checkMagic(r.database) && checkMagic(r.table)
		if !valid {
			err = fmt.Errorf(SupportReminder)
			return nil, err
		}

		r.lastDatabase, r.lastTable, r.omitDoneDBRecords = r.doneRecords.restoreRecordsFile(r.meta)
	}

	// 如果meta初始信息损坏
	if !omitMeta {
		r.offsets = offsets
	} else {
		r.offsets = make([]int64, len(r.syncSQLs))
	}

	// 定时任务配置串
	if len(cronSchedule) > 0 {
		cronSchedule = strings.ToLower(cronSchedule)
		if strings.HasPrefix(cronSchedule, Loop) {
			r.isLoop = true
			r.loopDuration, err = reader.ParseLoopDuration(cronSchedule)
			if err != nil {
				log.Errorf("Runner[%v] %v %v", r.meta.RunnerName, r.Name(), err)
			}
			if r.loopDuration.Nanoseconds() <= 0 {
				r.loopDuration = 1 * time.Second
			}
		} else {
			r.cronSchedule = true
			err = r.Cron.AddFunc(cronSchedule, r.run)
			if err != nil {
				return nil, err
			}
			log.Infof("Runner[%v] %v Cron job added with schedule <%v>", r.meta.RunnerName, r.Name(), cronSchedule)
		}
	}
	return r, nil
}

func (r *Reader) isStopping() bool {
	return atomic.LoadInt32(&r.status) == StatusStopping
}

func (r *Reader) hasStopped() bool {
	return atomic.LoadInt32(&r.status) == StatusStopped
}

func (r *Reader) Name() string {
	return strings.ToUpper(r.dbtype) + "_Reader:" + r.rawDatabase + "_" + Hash(r.rawSQLs)
}

func (r *Reader) SetMode(mode string, v interface{}) error {
	return errors.New("SQL reader does not support read mode")
}

func (r *Reader) setStatsError(err string) {
	r.statsLock.Lock()
	defer r.statsLock.Unlock()
	r.stats.LastError = err
}

func (r *Reader) sendError(err error) {
	if err == nil {
		return
	}
	defer func() {
		if rec := recover(); rec != nil {
			log.Errorf("Reader %q was panicked and recovered from %v", r.Name(), rec)
		}
	}()
	r.errChan <- err
}

func (r *Reader) Start() error {
	if r.isStopping() || r.hasStopped() {
		return errors.New("reader is stopping or has stopped")
	} else if !atomic.CompareAndSwapInt32(&r.status, StatusInit, StatusRunning) {
		log.Warnf("Runner[%v] %q daemon has already started and is running", r.meta.RunnerName, r.Name())
		return nil
	}

	if r.isLoop {
		go func() {
			ticker := time.NewTicker(r.loopDuration)
			defer ticker.Stop()
			for {
				r.run()

				select {
				case <-r.stopChan:
					atomic.StoreInt32(&r.status, StatusStopped)
					log.Infof("Runner[%v] %q daemon has stopped from running", r.meta.RunnerName, r.Name())
					return
				case <-ticker.C:
				}
			}
		}()

	} else {
		if r.execOnStart {
			go r.run()
		}
		r.Cron.Start()
	}
	log.Infof("Runner[%v] %q daemon has started", r.meta.RunnerName, r.Name())
	return nil
}

func (r *Reader) Source() string {
	// 不能把 DataSource 弄出去，包含密码
	return r.dbtype + "_" + r.database
}

func (r *Reader) ReadLine() (string, error) {
	return "", errors.New("method ReadLine is not supported, please use ReadData")
}

func (r *Reader) ReadData() (Data, int64, error) {
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	select {
	case info := <-r.readChan:
		return info.data, info.bytes, nil
	case err := <-r.errChan:
		return nil, 0, err
	case <-timer.C:
	}

	return nil, 0, nil
}

func (r *Reader) Status() StatsInfo {
	r.statsLock.RLock()
	defer r.statsLock.RUnlock()
	return r.stats
}

// SyncMeta 从队列取数据时同步队列，作用在于保证数据不重复
func (r *Reader) SyncMeta() {
	if r.rawSQLs == "" {
		now := time.Now().String()
		var all string
		dbRecords := r.syncRecords.GetDBRecords()

		for database, tablesRecord := range dbRecords {
			for table, tableInfo := range tablesRecord.GetTable() {
				all += database + sqlOffsetConnector + table + "," +
					strconv.FormatInt(tableInfo.size, 10) + "," +
					strconv.FormatInt(tableInfo.offset, 10) + "," +
					now + "@" + "\n"
			}
		}

		if len(all) <= 0 {
			r.syncRecords.Reset()
			return
		}
		if err := WriteRecordsFile(r.meta.DoneFilePath, all); err != nil {
			log.Errorf("Runner[%v] %v SyncMeta error %v", r.meta.RunnerName, r.Name(), err)
		}
		r.syncRecords.Reset()
		return
	}
	if r.timestampKey != "" {
		r.timestampMux.RLock()
		var content string
		if r.timestampKeyInt {
			content = strconv.FormatInt(r.startTimeInt, 10)
		} else {
			content = r.startTime.Format(time.RFC3339Nano)
		}
		if err := WriteTimestmapOffset(r.meta.DoneFilePath, content); err != nil {
			log.Errorf("Runner[%v] %v SyncMeta WriteTimestmapOffset error %v", r.meta.RunnerName, r.Name(), err)
		}
		if err := WriteCacheMap(r.meta.DoneFilePath, r.timeCacheMap); err != nil {
			log.Errorf("Runner[%v] %v SyncMeta WriteCacheMap error %v", r.meta.RunnerName, r.Name(), err)
		}
		r.timestampMux.RUnlock()
	}
	encodeSQLs := make([]string, 0)
	for _, sqlStr := range r.syncSQLs {
		encodeSQLs = append(encodeSQLs, strings.Replace(sqlStr, " ", "@", -1))
	}
	r.muxOffsets.RLock()
	defer r.muxOffsets.RUnlock()
	for _, offset := range r.offsets {
		encodeSQLs = append(encodeSQLs, strconv.FormatInt(offset, 10))
	}
	all := strings.Join(encodeSQLs, sqlOffsetConnector)
	if err := r.meta.WriteOffset(all, int64(len(r.syncSQLs))); err != nil {
		log.Errorf("Runner[%v] %v SyncMeta error %v", r.meta.RunnerName, r.Name(), err)
	}
	return
}

func (r *Reader) Close() error {
	if !atomic.CompareAndSwapInt32(&r.status, StatusRunning, StatusStopping) {
		log.Warnf("Runner[%v] reader %q is not running, close operation ignored", r.meta.RunnerName, r.Name())
		return nil
	}
	log.Debugf("Runner[%v] %q daemon is stopping", r.meta.RunnerName, r.Name())
	close(r.stopChan)

	r.Cron.Stop()

	// 如果此时没有 routine 正在运行，则在此处关闭数据管道，否则由 routine 在退出时负责关闭
	if atomic.CompareAndSwapInt32(&r.routineStatus, StatusInit, StatusStopping) {
		close(r.readChan)
		close(r.errChan)
	}
	return nil
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

func updateSqls(rawsqls string, now time.Time) []string {
	encodedSQLs := strings.Split(rawsqls, sqlSpliter)
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

//check if syncSQLs is out of date
func (r *Reader) updateOffsets(sqls []string) {
	r.muxOffsets.Lock()
	defer r.muxOffsets.Unlock()
	for idx, sqlStr := range sqls {
		if idx >= len(r.offsets) {
			r.offsets = append(r.offsets, 0)
			continue
		}
		if idx >= len(r.syncSQLs) {
			continue
		}
		if r.syncSQLs[idx] != sqlStr {
			r.offsets[idx] = 0
		}
	}

	return
}

func (r *Reader) run() {
	// 未在准备状态（StatusInit）时无法执行此次任务
	if !atomic.CompareAndSwapInt32(&r.routineStatus, StatusInit, StatusRunning) {
		if r.isStopping() || r.hasStopped() {
			log.Warnf("Runner[%v] %q daemon has stopped, this task does not need to be executed and is skipped this time", r.meta.RunnerName, r.Name())
		} else {
			errMsg := fmt.Sprintf("Runner[%v] %q daemon is still working on last task, this task will not be executed and is skipped this time", r.meta.RunnerName, r.Name())
			log.Error(errMsg)
			if !r.isLoop {
				// 通知上层 Cron 执行间隔可能过短或任务执行时间过长
				r.sendError(errors.New(errMsg))
			}
		}
		return
	}
	defer func() {
		// 如果 reader 在 routine 运行时关闭，则需要此 routine 负责关闭数据管道
		if r.isStopping() || r.hasStopped() {
			if atomic.CompareAndSwapInt32(&r.routineStatus, StatusRunning, StatusStopping) {
				close(r.readChan)
				close(r.errChan)
			}
			return
		}
		atomic.StoreInt32(&r.routineStatus, StatusInit)
	}()

	now := time.Now().Add(-r.magicLagDur)
	r.table = magic.GoMagic(r.rawTable, now)

	connectStr, err := r.getConnectStr("", now)
	if err != nil {
		log.Error(err)
		return
	}

	// 如果执行失败，最多重试 10 次
	for i := 1; i <= 10; i++ {
		// 判断上层是否已经关闭，先判断 routineStatus 再判断 status 可以保证同时只有一个 r.run 会运行到此处
		if r.isStopping() || r.hasStopped() {
			log.Warnf("Runner[%v] %q daemon has stopped, task is interrupted", r.meta.RunnerName, r.Name())
			return
		}

		err = r.exec(connectStr)
		if err == nil {
			log.Infof("Runner[%v] %q task has been successfully executed", r.meta.RunnerName, r.Name())
			return
		}

		log.Error(err)
		r.setStatsError(err.Error())
		r.sendError(err)

		if r.isLoop {
			return // 循环执行的任务上层逻辑已经等同重试
		}
		time.Sleep(3 * time.Second)
	}
	log.Errorf("Runner[%v] %q task execution failed and gave up after 10 tries", r.meta.RunnerName, r.Name())
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
			log.Error("Recovered in getInitScans", r)
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
		log.Errorf("Runner[%s] %s getInitScans length is %d not equal to columetypes %d", r.meta.RunnerName, r.Name(), length, len(tps))
		scanArgs = nochoice
	}
	scanArgs = make([]interface{}, length)
	for i, v := range tps {
		nochoiced[i] = false
		scantype := v.ScanType().String()
		dataBaseType := v.DatabaseTypeName()
		switch scantype {
		case "int64", "int32", "int16", "int", "int8":
			scanArgs[i] = new(interface{})
			if _, ok := r.schemas[v.Name()]; !ok {
				r.schemas[v.Name()] = "long"
			}
		case "float32", "float64":
			scanArgs[i] = new(float64)
		case "uint", "uint8", "uint16", "uint32", "uint64":
			scanArgs[i] = new(uint64)
		case "bool":
			scanArgs[i] = new(interface{})
			if _, ok := r.schemas[v.Name()]; !ok {
				r.schemas[v.Name()] = "bool"
			}
		case "[]uint8":
			scanArgs[i] = new([]byte)
		case "string", "RawBytes", "NullTime":
			scanArgs[i] = new(interface{})
			if _, ok := r.schemas[v.Name()]; !ok {
				r.schemas[v.Name()] = "string"
			}
		case "time.Time":
			scanArgs[i] = new(interface{})
			if _, ok := r.schemas[v.Name()]; !ok {
				r.schemas[v.Name()] = "date"
			}
		case "sql.NullInt64":
			scanArgs[i] = new(interface{})
			if _, ok := r.schemas[v.Name()]; !ok {
				r.schemas[v.Name()] = "long"
			}
		case "sql.NullFloat64":
			scanArgs[i] = new(interface{})
			if _, ok := r.schemas[v.Name()]; !ok {
				r.schemas[v.Name()] = "float"
			}
		default:
			scanArgs[i] = new(interface{})
			//Postgres Float的ScanType为interface,使用dataBaseType进一步判断
			if strings.Contains(dataBaseType, "FLOAT") {
				if _, ok := r.schemas[v.Name()]; !ok {
					r.schemas[v.Name()] = "float"
				}
			} else {
				nochoiced[i] = true
			}
		}
		if !r.firstPrinted {
			log.Infof("Runner[%v] %v Init field %v scan type is %v ", r.meta.RunnerName, r.Name(), v.Name(), scantype)
		}
	}
	r.firstPrinted = true
	return scanArgs, nochoiced
}

func (r *Reader) getOffsetIndex(columns []string) int {
	offsetKeyIndex := -1
	for idx, key := range columns {
		if len(r.offsetKey) > 0 && key == r.offsetKey {
			return idx
		}
		if len(r.timestampKey) > 0 && key == r.timestampKey {
			return idx
		}
	}
	return offsetKeyIndex
}

// mysql 中 若原始sql语句为空，则根据用户填写的database, table, historyAll获取数据，并且统计数据的条数
// 1. 若 database 和 table 都为空，则默认使用 *, 即获取所有的数据库和数据表
// 2. historyAll 为 true 时，获取所有小于渲染结果的数据
// 3. historyAll 为 false 时，若非定时任务，即只执行一次，获取与渲染结果相匹配的数据，若为定时任务，获取小于等于渲染结果的数据
func (r *Reader) exec(connectStr string) (err error) {
	now := time.Now().Add(-r.magicLagDur)
	// 获取符合条件的数据库
	dbs := make([]string, 0)
	switch r.dbtype {
	case ModeMySQL:
		if r.rawSQLs != "" {
			dbs = append(dbs, magic.GoMagic(r.rawDatabase, now))
		} else {
			var err error
			dbs, err = r.getDBs(connectStr, now)
			if err != nil {
				return err
			}

			log.Infof("Runner[%v] %v get valid databases: %v", r.meta.RunnerName, r.Name(), dbs)

			go func() {
				// 获取数据库所有条数
				r.execDB(dbs, now, COUNTFUNC)
				return
			}()
		}
	case ModeMSSQL, ModePostgreSQL:
		dbs = append(dbs, r.database)
	}

	err = r.execDB(dbs, now, READFUNC)
	if err != nil {
		return err
	}

	return nil
}

func (r *Reader) execDB(dbs []string, now time.Time, handlerFunc int) error {
	for _, currentDB := range dbs {
		var recordTablesDone TableRecords
		tableRecords := r.doneRecords.GetTableRecords(currentDB)
		recordTablesDone.Set(tableRecords)

		switch handlerFunc {
		case COUNTFUNC:
			err := r.execCountDB(currentDB, now, recordTablesDone)
			if err != nil {
				log.Errorf("Runner[%v] %v get current database: %v count error: %v", r.meta.RunnerName, r.Name(), currentDB, err)
			}
		case READFUNC:
			err := r.execReadDB(currentDB, now, recordTablesDone)
			if err != nil {
				log.Errorf("Runner[%v] %v exect read db: %v error: %v,will retry read it", r.meta.RunnerName, r.Name(), currentDB, err)
				return err
			}
		}

		if r.isStopping() || r.hasStopped() {
			log.Warnf("Runner[%v] %v stopped from running", r.meta.RunnerName, currentDB)
			return nil
		}
	}
	return nil
}

func (r *Reader) execCountDB(curDB string, now time.Time, recordTablesDone TableRecords) error {
	connectStr, err := r.getConnectStr(curDB, now)
	if err != nil {
		return err
	}

	log.Infof("Runner[%v] prepare %v change database success, current database is: %v", r.meta.RunnerName, r.dbtype, curDB)

	//更新sqls
	var tables []string
	var sqls string
	if r.rawSQLs == "" {
		// 获取符合条件的数据表和count语句
		tables, sqls, err = r.getValidData(connectStr, curDB, r.rawTable, now, COUNT)
		if err != nil {
			return err
		}

		log.Debugf("Runner[%v] %v default count sqls %v", r.meta.RunnerName, curDB, r.rawSQLs)

		if r.omitDoneDBRecords {
			// 兼容
			recordTablesDone.restoreTableDone(r.meta, curDB, tables)
		}
	}

	if r.rawSQLs != "" {
		sqls = r.rawSQLs
	}
	sqlsSlice := updateSqls(sqls, now)
	log.Infof("Runner[%v] %v start to work, sqls %v offsets %v", r.meta.RunnerName, curDB, sqlsSlice, r.offsets)
	tablesLen := len(tables)

	for idx, rawSql := range sqlsSlice {
		//分sql执行
		if r.rawSQLs == "" && idx < tablesLen {
			if recordTablesDone.GetTableInfo(tables[idx]) != (TableInfo{}) {
				continue
			}
		}

		// 每张表的记录数
		var tableSize int64
		tableSize, err = r.execTableCount(connectStr, idx, curDB, rawSql)
		if err != nil {
			return err
		}

		// 符合记录的数据库和表的记录总数
		r.addCount(tableSize)

		if r.isStopping() || r.hasStopped() {
			log.Warnf("Runner[%v] %v stopped from running", r.meta.RunnerName, curDB)
			return nil
		}
	}

	return nil
}

func (r *Reader) execReadDB(curDB string, now time.Time, recordTablesDone TableRecords) (err error) {
	connectStr, err := r.getConnectStr(curDB, now)
	if err != nil {
		return err
	}

	log.Debugf("Runner[%v] %v prepare %v change database success", r.meta.RunnerName, curDB, r.dbtype)
	r.database = curDB

	//更新sqls
	var tables []string
	var sqls string
	if r.rawSQLs == "" {
		// 获取符合条件的数据表和获取所有数据的语句
		tables, sqls, err = r.getValidData(connectStr, curDB, r.rawTable, now, TABLE)
		if err != nil {
			log.Errorf("Runner[%s] %s rawTable: %v rawSQLs: %v get tables and sqls error %v", r.meta.RunnerName, r.Name(), r.rawTable, r.rawSQLs, err)
			if len(tables) == 0 && sqls == "" {
				return err
			}
		}

		log.Infof("Runner[%s] %s default sqls %v", r.meta.RunnerName, r.Name(), sqls)

		if r.omitDoneDBRecords && !recordTablesDone.restoreTableDone(r.meta, curDB, tables) {
			// 兼容
			r.syncRecords.SetTableRecords(curDB, recordTablesDone)
			r.doneRecords.SetTableRecords(curDB, recordTablesDone)
		}
	}
	log.Debugf("Runner[%s] %s get valid tables: %v, recordTablesDone: %v", r.meta.RunnerName, r.Name(), tables, recordTablesDone)

	var sqlsSlice []string
	if r.rawSQLs != "" {
		sqlsSlice = updateSqls(r.rawSQLs, now)
		r.updateOffsets(sqlsSlice)
	} else {
		sqlsSlice = updateSqls(sqls, now)
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
		for !exit {
			if r.rawSQLs == "" && idx < tablesLen {
				tableName = tables[idx]
				if recordTablesDone.GetTableInfo(tableName) != (TableInfo{}) {
					break
				}
			}
			// 执行每条 sql 语句
			exit, isRawSql, readSize, err = r.execReadSql(connectStr, curDB, idx, rawSql, tables)
			if err != nil {
				return err
			}

			if r.rawSQLs == "" {
				r.syncRecords.SetTableInfo(curDB, tableName, TableInfo{size: readSize, offset: -1})
				r.doneRecords.SetTableInfo(curDB, tableName, TableInfo{size: readSize, offset: -1})
				recordTablesDone.SetTableInfo(tableName, TableInfo{size: readSize, offset: -1})
			}

			if r.isStopping() || r.hasStopped() {
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

func (r *Reader) getSQL(idx int, rawSQL string) (sql string, err error) {
	r.muxOffsets.RLock()
	defer r.muxOffsets.RUnlock()
	rawSQL = strings.TrimSuffix(strings.TrimSpace(rawSQL), ";")
	switch r.dbtype {
	case ModeMySQL:
		if len(r.offsetKey) > 0 && len(r.offsets) > idx {
			sql = fmt.Sprintf("%s WHERE %v >= %d AND %v < %d;", rawSQL, r.offsetKey, r.offsets[idx], r.offsetKey, r.offsets[idx]+int64(r.readBatch))
		} else {
			sql = fmt.Sprintf("%s", rawSQL)
		}
	case ModeMSSQL:
		if len(r.offsetKey) > 0 && len(r.offsets) > idx {
			sql = fmt.Sprintf("%s WHERE CAST(%v AS BIGINT) >= %d AND CAST(%v AS BIGINT) < %d;", rawSQL, r.offsetKey, r.offsets[idx], r.offsetKey, r.offsets[idx]+int64(r.readBatch))
		} else {
			err = fmt.Errorf("%v dbtype is not support get SQL without id now", r.dbtype)
		}
	case ModePostgreSQL:
		if len(r.timestampKey) > 0 {
			if r.timestampKeyInt {
				sql = fmt.Sprintf("%s WHERE %s >= %v and %s < %v;", rawSQL, r.timestampKey, r.startTimeInt, r.timestampKey, r.startTimeInt+int64(r.batchDurInt))
			} else {
				sql = fmt.Sprintf("%s WHERE %s >= '%s' and %s < '%s';", rawSQL, r.timestampKey, r.startTime.Format(pgtimeFormat), r.timestampKey, r.startTime.Add(r.batchDuration).Format(pgtimeFormat))
			}
		} else if len(r.offsetKey) > 0 && len(r.offsets) > idx {
			sql = fmt.Sprintf("%s WHERE %v >= %d AND %v < %d;", rawSQL, r.offsetKey, r.offsets[idx], r.offsetKey, r.offsets[idx]+int64(r.readBatch))
		} else {
			err = fmt.Errorf("%v dbtype is not support get SQL without id now", r.dbtype)
		}
	default:
		err = fmt.Errorf("%v dbtype is not support get SQL now", r.dbtype)
	}

	return sql, err
}

//这个query只有一行
func queryNumber(tsql string, db *sql.DB) (int64, error) {
	rows, err := db.Query(tsql)
	if err != nil {
		log.Error(err)
		return 0, err
	}
	defer rows.Close()
	var scanArgs = []interface{}{new(interface{})}
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			log.Error(err)
			return 0, err
		}
		if len(scanArgs) < 1 {
			return 0, errors.New("no data found")
		}

		number, err := convertLong(scanArgs[0])
		if err != nil {
			log.Error(err)
		}
		return number, nil
	}
	return 0, errors.New("no data found")
}

func (r *Reader) checkExit(idx int, db *sql.DB) (bool, int64) {
	if len(r.offsetKey) <= 0 && len(r.timestampKey) <= 0 {
		return true, -1
	}
	rawSQL := r.syncSQLs[idx]
	rawSQL = strings.TrimSuffix(strings.TrimSpace(rawSQL), ";")
	var tsql string
	if r.dbtype == ModeMySQL {
		tsql = fmt.Sprintf("%s WHERE %v >= %d order by %v limit 1;", rawSQL, r.offsetKey, r.offsets[idx], r.offsetKey)
	} else if r.dbtype == ModePostgreSQL {
		if len(r.timestampKey) > 0 {
			ix := strings.Index(rawSQL, "from")
			if ix < 0 {
				return true, -1
			}
			/* 是否要更新时间，取决于两点
			1. 原来那一刻到现在为止是否有新增数据
			2. 原来那一刻是否有新数据
			如果第一点满足，就不能退出
			如果第二点满足，就不能移动时间
			否则要移动时间，不然就死循环了
			*/
			rawSQL = rawSQL[ix:]

			//获得最新时间戳到当前时间的数据量
			if r.timestampKeyInt {
				tsql = fmt.Sprintf("select COUNT(*) as countnum %v WHERE %v >= %v;", rawSQL, r.timestampKey, r.startTimeInt)
			} else {
				tsql = fmt.Sprintf("select COUNT(*) as countnum %v WHERE %v >= '%s';", rawSQL, r.timestampKey, r.startTime.Format(pgtimeFormat))
			}
			largerAmount, err := queryNumber(tsql, db)
			if err != nil || largerAmount <= int64(len(r.timeCacheMap)) {
				//查询失败或者数据量不变本轮就先退出了
				return true, -1
			}
			//-- 比较有没有比之前的数量大，如果没有变大就退出
			//如果变大，继续判断当前这个重复的时间有没有数据
			if r.timestampKeyInt {
				tsql = fmt.Sprintf("select COUNT(*) as countnum %v WHERE %v = %v;", rawSQL, r.timestampKey, r.startTimeInt)
			} else {
				tsql = fmt.Sprintf("select COUNT(*) as countnum %v WHERE %v = '%s';", rawSQL, r.timestampKey, r.startTime.Format(pgtimeFormat))
			}
			equalAmount, err := queryNumber(tsql, db)
			if err == nil && equalAmount > int64(len(r.timeCacheMap)) {
				//说明还有新数据在原来的时间点，不能退出，且还要再查
				return false, -1
			}
			//此处如果发现同样的时间戳数据没有变，那么说明是新的时间产生的数据，时间戳要更新了
			//获得最小的时间戳
			if r.timestampKeyInt {
				tsql = fmt.Sprintf("select MIN(%s) as %s %v WHERE %v > %v;", r.timestampKey, r.timestampKey, rawSQL, r.timestampKey, r.startTimeInt)
			} else {
				tsql = fmt.Sprintf("select MIN(%s) as %s %v WHERE %v > '%s';", r.timestampKey, r.timestampKey, rawSQL, r.timestampKey, r.startTime.Format(pgtimeFormat))
			}
		} else {
			ix := strings.Index(rawSQL, "from")
			if ix < 0 {
				return true, -1
			}
			rawSQL = rawSQL[ix:]
			tsql = fmt.Sprintf("select MIN(%s) as %s %v WHERE %v >= %v;", r.offsetKey, r.offsetKey, rawSQL, r.offsetKey, r.offsets[idx])
		}
	} else {
		ix := strings.Index(rawSQL, "from")
		if ix < 0 {
			return true, -1
		}
		rawSQL = rawSQL[ix:]
		tsql = fmt.Sprintf("select top(1) * %v WHERE CAST(%v AS BIGINT) >= %v order by CAST(%v AS BIGINT);", rawSQL, r.offsetKey, r.offsets[idx], r.offsetKey)
	}
	log.Info("query <", tsql, "> to check exit")
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
			if len(r.timestampKey) > 0 {
				updated := r.updateStartTime(offsetKeyIndex, scanArgs)
				return !updated, -1
			}
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

func contains(slice []string, str string) bool {
	for _, s := range slice {
		if s == str {
			return true
		}
	}
	return false
}

// 查看时间是否符合, min为true则取出来为小于等于，min为false则取出来大于等于
func compareTime(target, match string, timeStart, timeEnd []int, min bool) (valid bool) {
	for idx, record := range timeStart {
		if record == -1 {
			continue
		}

		if len(target) < timeEnd[idx] || len(match) < timeEnd[idx] {
			return false
		}

		// 比较大小
		curStr := target[record:timeEnd[idx]]
		curInt, err := strconv.Atoi(curStr)
		if err != nil {
			return false
		}
		matchStr := match[record:timeEnd[idx]]
		matchInt, err := strconv.Atoi(matchStr)
		if err != nil {
			return false
		}

		// 小于
		if curInt < matchInt {
			return min
		}

		if curInt > matchInt {
			return !min
		}

		// 相等
		valid = true
	}

	return true
}

// 查看时间是否相等
func equalTime(target, magicRet string, timeStart, timeEnd []int) (valid bool) {
	for idx, record := range timeStart {
		if record == -1 {
			continue
		}

		if len(target) < timeEnd[idx] {
			return false
		}

		// 比较大小
		curStr := target[record:timeEnd[idx]]
		curInt, err := strconv.Atoi(curStr)
		if err != nil {
			return false
		}
		matchStr := magicRet[record:timeEnd[idx]]
		matchInt, err := strconv.Atoi(matchStr)
		if err != nil {
			return false
		}

		// 等于
		if curInt != matchInt {
			return false
		}

		valid = true
	}

	return true
}

// 获取有效数据
func (r *Reader) getValidData(connectStr, curDB, rawData string, now time.Time, queryType int) (validData []string, sqls string, err error) {
	// 是否导入所有数据
	getAll, err := r.getAll(queryType)
	if err != nil {
		return nil, "", err
	}

	// get all databases and check validate database
	query, err := r.getQuery(queryType, curDB)
	if err != nil {
		return nil, "", err
	}

	db, err := openSql(r.dbtype, connectStr)
	if err != nil {
		return nil, "", err
	}
	defer func() {
		db.Close()
	}()
	if err = db.Ping(); err != nil {
		return nil, "", err
	}

	rowsDBs, err := db.Query(query)
	if err != nil {
		log.Errorf("Runner[%v] %v prepare %v <%v> query error %v", r.meta.RunnerName, curDB, r.dbtype, query, err)
		return nil, "", err
	}
	defer rowsDBs.Close()

	validData = make([]string, 0)
	for rowsDBs.Next() {
		var s string
		err = rowsDBs.Scan(&s)
		if err != nil {
			log.Errorf("Runner[%v] %v scan rows error %v", r.meta.RunnerName, curDB, err)
			continue
		}

		// queryType == TABLE时，检查是否已经读过，DATABASE 和 COUNT 不需要check
		if queryType == TABLE && r.checkDoneRecords(s, curDB) {
			continue
		}

		// 不导入所有数据时，需要进行匹配
		if !getAll {
			magicRes, err := goMagicIndex(rawData, now)
			if err != nil {
				return nil, "", err
			}

			magicRemainStr := getRemainStr(magicRes.ret, magicRes.remainIndex)
			if !r.compareData(queryType, curDB, s, magicRemainStr, &magicRes) {
				log.Debugf("Runner[%v] %v current data: %v, current time data: %v, remain str: %v, timeIndex: %v", r.meta.RunnerName, curDB, s, magicRes.ret, magicRemainStr, magicRes.remainIndex)
				continue
			}
		}

		rawSql, err := r.getRawSqls(queryType, s)
		if err != nil {
			return validData, sqls, err
		}
		sqls += rawSql

		validData = append(validData, s)
	}

	return validData, sqls, nil
}

func (r *Reader) getConnectStr(database string, now time.Time) (connectStr string, err error) {
	switch r.dbtype {
	case ModeMySQL:
		connectStr = r.datasource + "/" + database
		if r.encoder != "" {
			connectStr += "?charset=" + r.encoder
		}
	case ModeMSSQL:
		r.database = magic.GoMagic(r.rawDatabase, now)
		connectStr = r.datasource + ";database=" + r.database
	case ModePostgreSQL:
		r.database = magic.GoMagic(r.rawDatabase, now)
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
	default:
		return "", fmt.Errorf("reader type unsupported: %v", r.dbtype)
	}
	return connectStr, nil
}

func openSql(dbtype, connectStr string) (db *sql.DB, err error) {
	db, err = sql.Open(dbtype, connectStr)
	if err != nil {
		return nil, fmt.Errorf("open %v failed: %v", dbtype, err)
	}
	return db, nil
}

func compareRemainStr(target, magicRemainStr, magicRet string, magicRemainIndex []int) bool {
	if len(magicRemainIndex) > 0 && len(target) < magicRemainIndex[len(magicRemainIndex)-1] {
		return false
	}
	targetRemainStr := getRemainStr(target, magicRemainIndex)
	if len(targetRemainStr) < len(magicRemainStr) {
		return false
	}

	// magicRet 中如果含有通配符，则只需剩余字符匹配即可
	if targetRemainStr[:len(magicRemainStr)] != magicRemainStr {
		return false
	}

	// matchData中有通配符
	if strings.HasSuffix(magicRet, Wildcards) {
		return true
	}

	if len(target) > len(magicRet) {
		targetRemainStr += target[len(magicRet):]
		if targetRemainStr != magicRemainStr {
			return false
		}
	}

	return true
}

func getRemainStr(origin string, magicRemainIndex []int) (remainStr string) {
	if len(magicRemainIndex)%2 != 0 {
		return origin
	}

	for idx := 0; idx < len(magicRemainIndex); {
		remainStr += origin[magicRemainIndex[idx]:magicRemainIndex[idx+1]]
		idx = idx + 2
	}

	return remainStr
}

func (r *Reader) Lag() (rl *LagInfo, err error) {
	rl = &LagInfo{SizeUnit: "records"}
	if r.rawSQLs == "" {
		count := r.getCount()
		rl.Size = count - r.CurrentCount
		if rl.Size < 0 {
			rl.Size = 0
		}
		rl.Total = count
	}

	return rl, nil
}

func (r *Reader) getDefaultSql(database string) (defaultSql string, err error) {
	switch r.dbtype {
	case ModeMySQL:
		return strings.Replace(DefaultMySQLTable, "DATABASE_NAME", database, -1), nil
	case ModePostgreSQL:
		return strings.Replace(DefaultPGSQLTable, "SCHEMA_NAME", r.dbSchema, -1), nil
	case ModeMSSQL:
		sqlStr := strings.Replace(DefaultMSSQLTable, "DATABASE_NAME", database, -1)
		sqlStr = strings.Replace(sqlStr, "SCHEMA_NAME", r.dbSchema, -1)
		return sqlStr, nil
	default:
		return "", fmt.Errorf("reader type unsupported: %v", r.dbtype)
	}
}

func (r *Reader) checkCron() bool {
	return r.isLoop || r.cronSchedule
}

// 是否获取所有数据
func (r *Reader) getAll(queryType int) (getAll bool, err error) {
	switch queryType {
	case TABLE, COUNT:
		return r.rawTable == "*", nil
	case DATABASE:
		return r.rawDatabase == "*", nil
	default:
		return false, fmt.Errorf("%v queryType is not support get sql now", queryType)
	}

	return true, nil
}

//根据数据库类型返回表名
func (r *Reader) getWrappedTableName(table string) (tableName string, err error) {
	switch r.dbtype {
	case ModeMySQL:
		tableName = "`" + table + "`"
	case ModeMSSQL:
		tableName = fmt.Sprintf("\"%s\".\"%s\"", r.dbSchema, table)
	case ModePostgreSQL:
		tableName = fmt.Sprintf("\"%s\".\"%s\"", r.dbSchema, table)
	default:
		err = fmt.Errorf("%v mode not support in sql reader", r.dbtype)
	}
	return tableName, nil
}

// 根据 queryType 获取 table 中所有记录或者表中所有数据的条数的sql语句
func (r *Reader) getRawSqls(queryType int, table string) (sqls string, err error) {
	switch queryType {
	case TABLE:
		tableName, err := r.getWrappedTableName(table)
		if err != nil {
			return "", err
		}
		sqls += "Select * From " + tableName + ";"
	case COUNT:
		tableName, err := r.getWrappedTableName(table)
		if err != nil {
			return "", err
		}
		sqls += "Select Count(*) From " + tableName + ";"
	case DATABASE:
	default:
		return "", fmt.Errorf("%v queryType is not support get sql now", queryType)
	}

	return sqls, nil
}

// 根据 queryType 获取query语句
func (r *Reader) getQuery(queryType int, curDB string) (query string, err error) {
	switch queryType {
	case TABLE, COUNT:
		return r.getDefaultSql(curDB)
	case DATABASE:
		return DefaultMySQLDatabase, nil
	default:
		return "", fmt.Errorf("%v queryType is not support get sql now", queryType)
	}
}

// 计算每个table的记录条数
func (r *Reader) execTableCount(connectStr string, idx int, curDB, rawSql string) (tableSize int64, err error) {
	execSQL, err := r.getSQL(idx, rawSql)
	if err != nil {
		log.Errorf("Runner[%v] get SQL error %v, use raw SQL", r.meta.RunnerName, err)
		execSQL = rawSql
	}
	log.Infof("Runner[%v] reader <%v> exec sql <%v>", r.meta.RunnerName, curDB, execSQL)

	db, err := openSql(r.dbtype, connectStr)
	if err != nil {
		return 0, err
	}
	defer func() {
		db.Close()
	}()
	if err = db.Ping(); err != nil {
		return 0, err
	}

	rows, err := db.Query(execSQL)
	if err != nil {
		log.Errorf("Runner[%v] %v prepare %v <%v> query error %v", r.meta.RunnerName, curDB, r.dbtype, execSQL, err)
		return 0, err
	}
	defer rows.Close()

	// Fetch rows
	for rows.Next() {
		var s string
		err = rows.Scan(&s)
		if err != nil {
			log.Errorf("Runner[%v] %v scan rows error %v", r.meta.RunnerName, curDB, err)
			return 0, err
		}

		tableSize, err = strconv.ParseInt(s, 10, 64)
		if err != nil {
			log.Errorf("Runner[%v] %v convert string to int64 error %v", r.meta.RunnerName, curDB, err)
			return 0, err
		}
	}

	return tableSize, nil
}

func (r *Reader) getAllDatas(rows *sql.Rows, scanArgs []interface{}, columns []string, nochiced []bool) ([]readInfo, bool) {
	datas := make([]readInfo, 0)
	for rows.Next() {
		data, totalBytes := r.getData(rows, scanArgs, columns, nochiced)
		if len(data) <= 0 {
			continue
		}
		if r.isStopping() || r.hasStopped() {
			log.Warnf("Runner[%v] %v stopped from running", r.meta.RunnerName, r.Name())
			return nil, true
		}
		datas = append(datas, readInfo{data: data, bytes: totalBytes})
	}
	return datas, false
}

// 执行每条 sql 语句
func (r *Reader) execReadSql(connectStr, curDB string, idx int, rawSql string, tables []string) (exit bool, isRawSql bool, readSize int64, err error) {
	exit = true

	execSQL, err := r.getSQL(idx, r.syncSQLs[idx])
	if err != nil {
		log.Warnf("Runner[%v] get SQL error %v, use raw SQL", r.meta.RunnerName, err)
		execSQL = rawSql
	}

	if execSQL == rawSql {
		isRawSql = true
	}

	db, err := openSql(r.dbtype, connectStr)
	if err != nil {
		return exit, isRawSql, 0, err
	}
	defer func() {
		db.Close()
	}()
	if err = db.Ping(); err != nil {
		return exit, isRawSql, 0, err
	}

	log.Debugf("Runner[%v] reader <%v> start to exec sql <%v>", r.meta.RunnerName, r.Name(), execSQL)
	rows, err := db.Query(execSQL)
	if err != nil {
		err = fmt.Errorf("runner[%v] %v prepare %v <%v> query error %v", r.meta.RunnerName, r.Name(), r.dbtype, execSQL, err)
		log.Error(err)
		r.sendError(err)
		return exit, isRawSql, readSize, err
	}
	defer rows.Close()
	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		err = fmt.Errorf("runner[%v] %v prepare %v <%v> columns error %v", r.meta.RunnerName, r.Name(), r.dbtype, execSQL, err)
		log.Error(err)
		r.sendError(err)
		return exit, isRawSql, readSize, err
	}
	log.Debugf("Runner[%v] SQL ：<%v>, got schemas: <%v>", r.meta.RunnerName, execSQL, strings.Join(columns, ", "))
	scanArgs, nochiced := r.getInitScans(len(columns), rows, r.dbtype)
	var offsetKeyIndex int
	if r.rawSQLs != "" {
		offsetKeyIndex = r.getOffsetIndex(columns)
	}

	alldatas, closed := r.getAllDatas(rows, scanArgs, columns, nochiced)
	if closed {
		return exit, isRawSql, readSize, nil
	}
	total := len(alldatas)
	alldatas = r.trimeExistData(alldatas)

	// Fetch rows
	var maxOffset int64 = -1
	for _, v := range alldatas {
		exit = false
		if len(r.timestampKey) > 0 {
			r.updateTimeCntFromData(v)
		}
		r.readChan <- v
		r.CurrentCount++
		readSize++

		if r.historyAll || r.rawSQLs == "" {
			continue
		}
		if len(r.timestampKey) <= 0 {
			maxOffset = r.updateOffset(idx, offsetKeyIndex, maxOffset, scanArgs)
		}
	}
	var startTimePrint string
	if r.timestampKeyInt {
		startTimePrint = strconv.FormatInt(r.startTimeInt, 10)
	} else {
		startTimePrint = r.startTime.String()
	}
	log.Infof("Runner[%v] SQL: <%v> find total %d data, after trim duplicated, left data is: %d, "+
		"now we have total got %v data, and start time is %v ",
		r.meta.RunnerName, execSQL, total, len(alldatas), len(r.timeCacheMap), startTimePrint)
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
	return exit, isRawSql, readSize, rows.Err()
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
		r.muxOffsets.Lock()
		r.offsets[idx]++
		r.muxOffsets.Unlock()
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

func (r *Reader) checkDoneRecords(target, curDB string) bool {
	var tableInfo TableInfo
	tableDoneRecords := r.doneRecords.GetTableRecords(curDB)
	if tableDoneRecords.GetTable() == nil {
		return false
	}

	tableInfo = tableDoneRecords.GetTableInfo(target)
	if tableInfo == (TableInfo{}) {
		return false
	}

	return true
}

// 取大于等于 最后一条记录 的数据，结果为 true 为小于或者不符合, false 为大于等于
func (r *Reader) greaterThanLastRecord(queryType int, target, magicRemainStr string, magicRes *MagicRes) bool {
	log.Debugf("Runner[%v] current data: %v, last database record: %v, last table record: %v", r.meta.RunnerName, target, r.lastDatabase, r.lastTable)
	if magicRes == nil {
		return true
	}
	var rawData string
	switch queryType {
	case DATABASE:
		rawData = r.lastDatabase
	case TABLE, COUNT:
		rawData = r.lastTable
	default:
		return false
	}

	if len(rawData) == 0 {
		return true
	}
	log.Infof("Runner[%v] last %v is: %v, target: %v, magicRes: %v", r.meta.RunnerName, queryType, rawData, target, magicRes)

	match := compareRemainStr(rawData, magicRemainStr, magicRes.ret, magicRes.remainIndex)
	if !match {
		return false
	}

	return compareTime(target, rawData, magicRes.timeStart, magicRes.timeEnd, false)
}

func (r *Reader) getDBs(connectStr string, now time.Time) ([]string, error) {
	db, err := openSql(r.dbtype, connectStr)
	if err != nil {
		return nil, err
	}
	defer db.Close()
	if err = db.Ping(); err != nil {
		return nil, err
	}
	// 获取所有符合条件的数据库
	dbsAll, _, err := r.getValidData(connectStr, "", r.rawDatabase, now, DATABASE)
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

// 不为 * 时，进行匹配
func (r *Reader) compareData(queryType int, curDB, target, magicRemainStr string, magicRes *MagicRes) bool {
	if magicRes == nil {
		return true
	}

	match := compareRemainStr(target, magicRemainStr, magicRes.ret, magicRes.remainIndex)
	log.Debugf("Runner[%v] %v current data: %v, current time data: %v, remain str: %v, magicRemainIndex: %v, isMatch: %v", r.meta.RunnerName, curDB, target, magicRes.ret, magicRemainStr, magicRes.remainIndex, match)
	if !match {
		return false
	}

	if r.historyAll {
		// 取大于等于上一条的和小于等于现有的
		if compareTime(target, magicRes.ret, magicRes.timeStart, magicRes.timeEnd, true) &&
			r.greaterThanLastRecord(queryType, target, magicRemainStr, magicRes) {
			return true
		}
	} else {
		// 执行一次，应符合渲染结果
		if !r.checkCron() {
			return equalTime(target, magicRes.ret, magicRes.timeStart, magicRes.timeEnd)
		}

		// 取大于等于上一条的和小于等于现有的
		if compareTime(target, magicRes.ret, magicRes.timeStart, magicRes.timeEnd, true) &&
			r.greaterThanLastRecord(queryType, target, magicRemainStr, magicRes) {
			return true
		}
	}

	return false
}
