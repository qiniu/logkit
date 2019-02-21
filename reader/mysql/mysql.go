package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql" //mysql 驱动
	"github.com/robfig/cron"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/reader/config"
	. "github.com/qiniu/logkit/reader/sql"
	"github.com/qiniu/logkit/utils/magic"
	"github.com/qiniu/logkit/utils/models"
)

var (
	_ reader.DaemonReader = &MysqlReader{}
	_ reader.StatsReader  = &MysqlReader{}
	_ reader.DataReader   = &MysqlReader{}
	_ reader.Reader       = &MysqlReader{}
)

var MysqlSystemDB = []string{"information_schema", "performance_schema", "mysql", "sys"}

func init() {
	reader.RegisterConstructor(ModeMySQL, NewMysqlReader)
}

type readInfo struct {
	data  models.Data
	bytes int64
}

type MysqlReader struct {
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

	stats     models.StatsInfo
	statsLock sync.RWMutex

	datasource  string //数据源
	database    string //数据库名称
	rawDatabase string // 记录原始数据库
	rawSQLs     string // 原始sql执行列表
	historyAll  bool   // 是否导入历史数据
	rawTable    string // 记录原始数据库表名
	table       string // 数据库表名

	isLoop       bool
	loopDuration time.Duration
	cronSchedule bool //是否为定时任务
	execOnStart  bool
	Cron         *cron.Cron //定时任务
	readBatch    int        // 每次读取的数据量
	offsetKey    string

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
}

func NewMysqlReader(meta *reader.Meta, conf conf.MapConf) (reader.Reader, error) {
	var (
		dataSource, rawDatabase, rawSQLs, cronSchedule, offsetKey, encoder, table, dbSchema string

		execOnStart, historyAll bool
		readBatch               int
		err                     error
		mgld                    time.Duration
	)

	logpath, _ := conf.GetStringOr(KeyLogPath, "")
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
	rawDatabase, err = conf.GetString(KeyMysqlDataBase)
	if err != nil {
		return nil, err
	}
	rawSQLs, _ = conf.GetStringOr(KeyMysqlSQL, "")
	cronSchedule, _ = conf.GetStringOr(KeyMysqlCron, "")
	execOnStart, _ = conf.GetBoolOr(KeyMysqlExecOnStart, true)
	encoder, _ = conf.GetStringOr(KeyMysqlEncoding, "utf8")
	if strings.Contains(encoder, "-") {
		encoder = strings.Replace(strings.ToLower(encoder), "-", "", -1)
	}
	historyAll, _ = conf.GetBoolOr(KeyMysqlHistoryAll, false)
	table, _ = conf.GetStringOr(KyeMysqlTable, "")
	rawSchemas, _ := conf.GetStringListOr(KeySQLSchema, []string{})
	magicLagDur, _ := conf.GetStringOr(KeyMagicLagDuration, "")
	if magicLagDur != "" {
		mgld, err = time.ParseDuration(magicLagDur)
		if err != nil {
			return nil, err
		}
	}
	schemas, err := SchemaCheck(rawSchemas)
	if err != nil {
		return nil, err
	}

	var (
		sqls     []string
		omitMeta = true
		offsets  []int64
	)
	if rawSQLs != "" {
		offsets, sqls, omitMeta = RestoreMeta(meta, rawSQLs, mgld)
	}

	r := &MysqlReader{
		meta:          meta,
		status:        StatusInit,
		routineStatus: StatusInit,
		stopChan:      make(chan struct{}),
		readChan:      make(chan readInfo),
		errChan:       make(chan error),
		datasource:    dataSource,
		database:      rawDatabase,
		rawDatabase:   rawDatabase,
		rawSQLs:       rawSQLs,
		Cron:          cron.New(),
		readBatch:     readBatch,
		offsetKey:     offsetKey,
		syncSQLs:      sqls,
		execOnStart:   execOnStart,
		historyAll:    historyAll,
		rawTable:      table,
		table:         table,
		magicLagDur:   mgld,
		schemas:       schemas,
		encoder:       encoder,
		dbSchema:      dbSchema,
	}

	if r.rawDatabase == "" {
		r.rawDatabase = "*"
	}
	if r.rawTable == "" {
		r.rawTable = "*"
	}

	if r.rawSQLs == "" {
		valid := CheckMagic(r.database) && CheckMagic(r.table)
		if !valid {
			err = fmt.Errorf(SupportReminder)
			return nil, err
		}

		r.lastDatabase, r.lastTable, r.omitDoneDBRecords = r.doneRecords.RestoreRecordsFile(r.meta)
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

func (r *MysqlReader) isStopping() bool {
	return atomic.LoadInt32(&r.status) == StatusStopping
}

func (r *MysqlReader) hasStopped() bool {
	return atomic.LoadInt32(&r.status) == StatusStopped
}

func (r *MysqlReader) Name() string {
	return "MYSQL_Reader:" + r.rawDatabase + "_" + models.Hash(r.rawSQLs)
}

func (r *MysqlReader) SetMode(mode string, v interface{}) error {
	return errors.New("MYSQL reader does not support read mode")
}

func (r *MysqlReader) setStatsError(err string) {
	r.statsLock.Lock()
	r.stats.LastError = err
	r.statsLock.Unlock()
}

func (r *MysqlReader) sendError(err error) {
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

func (r *MysqlReader) Start() error {
	if r.isStopping() || r.hasStopped() {
		return errors.New("reader is stopping or has stopped")
	}
	if !atomic.CompareAndSwapInt32(&r.status, StatusInit, StatusRunning) {
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

func (r *MysqlReader) Source() string {
	// 不能把 DataSource 弄出去，包含密码
	return "MYSQL_" + r.database
}

func (r *MysqlReader) ReadLine() (string, error) {
	return "", errors.New("method ReadLine is not supported, please use ReadData")
}

func (r *MysqlReader) ReadData() (models.Data, int64, error) {
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

func (r *MysqlReader) Status() models.StatsInfo {
	r.statsLock.RLock()
	defer r.statsLock.RUnlock()
	return r.stats
}

// SyncMeta 从队列取数据时同步队列，作用在于保证数据不重复
func (r *MysqlReader) SyncMeta() {
	if r.rawSQLs == "" {
		now := time.Now().String()
		var all string
		dbRecords := r.syncRecords.GetDBRecords()

		for database, tablesRecord := range dbRecords {
			for table, tableInfo := range tablesRecord.GetTable() {
				all += database + SqlOffsetConnector + table + "," +
					strconv.FormatInt(tableInfo.Size, 10) + "," +
					strconv.FormatInt(tableInfo.Offset, 10) + "," +
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
	encodeSQLs := make([]string, 0)
	for _, sqlStr := range r.syncSQLs {
		encodeSQLs = append(encodeSQLs, strings.Replace(sqlStr, " ", "@", -1))
	}
	r.muxOffsets.RLock()
	defer r.muxOffsets.RUnlock()
	for _, offset := range r.offsets {
		encodeSQLs = append(encodeSQLs, strconv.FormatInt(offset, 10))
	}
	all := strings.Join(encodeSQLs, SqlOffsetConnector)
	if err := r.meta.WriteOffset(all, int64(len(r.syncSQLs))); err != nil {
		log.Errorf("Runner[%v] %v SyncMeta error %v", r.meta.RunnerName, r.Name(), err)
	}
	return
}

func (r *MysqlReader) Close() error {
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

//check if syncSQLs is out of date
func (r *MysqlReader) updateOffsets(sqls []string) {
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

func (r *MysqlReader) run() {
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
	connectStr := r.getConnectStr("", now)

	// 如果执行失败，最多重试 10 次
	for i := 1; i <= 10; i++ {
		// 判断上层是否已经关闭，先判断 routineStatus 再判断 status 可以保证同时只有一个 r.run 会运行到此处
		if r.isStopping() || r.hasStopped() {
			log.Warnf("Runner[%v] %q daemon has stopped, task is interrupted", r.meta.RunnerName, r.Name())
			return
		}

		err := r.exec(connectStr)
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

// mysql 中 若原始sql语句为空，则根据用户填写的database, table, historyAll获取数据，并且统计数据的条数
// 1. 若 database 和 table 都为空，则默认使用 *, 即获取所有的数据库和数据表
// 2. historyAll 为 true 时，获取所有小于渲染结果的数据
// 3. historyAll 为 false 时，若非定时任务，即只执行一次，获取与渲染结果相匹配的数据，若为定时任务，获取小于等于渲染结果的数据
func (r *MysqlReader) exec(connectStr string) (err error) {
	var (
		now = time.Now().Add(-r.magicLagDur)
		// 获取符合条件的数据库
		dbs = make([]string, 0, 100)
	)

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

	err = r.execDB(dbs, now, READFUNC)
	if err != nil {
		return err
	}

	return nil
}

func (r *MysqlReader) execDB(dbs []string, now time.Time, handlerFunc int) error {
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

func (r *MysqlReader) execCountDB(curDB string, now time.Time, recordTablesDone TableRecords) error {
	connectStr := r.getConnectStr(curDB, now)
	db, err := OpenSql(ModeMySQL, connectStr)
	if err != nil {
		return err
	}
	defer db.Close()

	log.Infof("Runner[%v] prepare MYSQL change database success, current database is: %v", r.meta.RunnerName, curDB)

	//更新sqls
	var tables []string
	var sqls string
	if r.rawSQLs == "" {
		// 获取符合条件的数据表和count语句
		tables, sqls, err = r.getValidData(curDB, r.rawTable, now, COUNT, db)
		if err != nil {
			return err
		}

		log.Debugf("Runner[%v] %v default count sqls %v", r.meta.RunnerName, curDB, r.rawSQLs)

		if r.omitDoneDBRecords {
			// 兼容
			recordTablesDone.RestoreTableDone(r.meta, curDB, tables)
		}
	}

	if r.rawSQLs != "" {
		sqls = r.rawSQLs
	}
	sqlsSlice := UpdateSqls(sqls, now)
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

func (r *MysqlReader) execReadDB(curDB string, now time.Time, recordTablesDone TableRecords) (err error) {
	connectStr := r.getConnectStr(curDB, now)
	db, err := OpenSql(ModeMySQL, connectStr)
	if err != nil {
		return err
	}
	defer db.Close()

	log.Debugf("Runner[%v] %v prepare MYSQL change database success", r.meta.RunnerName, curDB)
	r.database = curDB

	//更新sqls
	var tables []string
	var sqls string
	if r.rawSQLs == "" {
		// 获取符合条件的数据表和获取所有数据的语句
		tables, sqls, err = r.getValidData(curDB, r.rawTable, now, TABLE, db)
		if err != nil {
			log.Errorf("Runner[%s] %s rawTable: %v rawSQLs: %v get tables and sqls error %v", r.meta.RunnerName, r.Name(), r.rawTable, r.rawSQLs, err)
			if len(tables) == 0 && sqls == "" {
				return err
			}
		}

		log.Infof("Runner[%s] %s default sqls %v", r.meta.RunnerName, r.Name(), sqls)

		if r.omitDoneDBRecords && !recordTablesDone.RestoreTableDone(r.meta, curDB, tables) {
			// 兼容
			r.syncRecords.SetTableRecords(curDB, recordTablesDone)
			r.doneRecords.SetTableRecords(curDB, recordTablesDone)
		}
	}
	log.Debugf("Runner[%s] %s get valid tables: %v, recordTablesDone: %v", r.meta.RunnerName, r.Name(), tables, recordTablesDone)

	var sqlsSlice []string
	if r.rawSQLs != "" {
		sqlsSlice = UpdateSqls(r.rawSQLs, now)
		r.updateOffsets(sqlsSlice)
	} else {
		sqlsSlice = UpdateSqls(sqls, now)
	}

	r.syncSQLs = sqlsSlice
	tablesLen := len(tables)
	log.Infof("Runner[%v] %v start to work, sqls %v offsets %v", r.meta.RunnerName, r.Name(), r.syncSQLs, r.offsets)

	for idx, rawSql := range r.syncSQLs {
		//分sql执行
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
			execSQL := r.getSQL(idx, r.syncSQLs[idx])
			exit, readSize, err = r.execReadSql(curDB, idx, execSQL, db)
			if err != nil {
				return err
			}

			if r.rawSQLs == "" {
				r.syncRecords.SetTableInfo(curDB, tableName, TableInfo{Size: readSize, Offset: -1})
				r.doneRecords.SetTableInfo(curDB, tableName, TableInfo{Size: readSize, Offset: -1})
				recordTablesDone.SetTableInfo(tableName, TableInfo{Size: readSize, Offset: -1})
			}

			if r.isStopping() || r.hasStopped() {
				log.Warnf("Runner[%v] %v stopped from running", r.meta.RunnerName, r.Name())
				return nil
			}

			if execSQL == rawSql {
				log.Infof("Runner[%v] %v is raw SQL, exit after exec once...", r.meta.RunnerName, r.Name())
				break
			}
		}
	}
	return nil
}

func (r *MysqlReader) getSQL(idx int, rawSQL string) string {
	r.muxOffsets.RLock()
	defer r.muxOffsets.RUnlock()
	rawSQL = strings.TrimSuffix(strings.TrimSpace(rawSQL), ";")
	if len(r.offsetKey) > 0 && len(r.offsets) > idx {
		return fmt.Sprintf("%s WHERE %v >= %d AND %v < %d;", rawSQL, r.offsetKey, r.offsets[idx], r.offsetKey, r.offsets[idx]+int64(r.readBatch))
	}
	return rawSQL
}

func (r *MysqlReader) checkExit(idx int, db *sql.DB) (bool, int64) {
	if len(r.offsetKey) <= 0 {
		return true, -1
	}
	rawSQL := r.syncSQLs[idx]
	rawSQL = strings.TrimSuffix(strings.TrimSpace(rawSQL), ";")
	tsql := fmt.Sprintf("%s WHERE %v >= %d order by %v limit 1;", rawSQL, r.offsetKey, r.offsets[idx], r.offsetKey)
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
		log.Errorf("Runner[%v] %v prepare columns error %v", r.meta.RunnerName, r.Name(), err)
		return true, -1
	}
	scanArgs, _ := GetInitScans(len(columns), rows, r.schemas, r.meta.RunnerName, r.Name())
	offsetKeyIndex := GetOffsetIndex(r.offsetKey, columns)
	for rows.Next() {
		err = rows.Scan(scanArgs...)
		if err != nil {
			return false, -1
		}
		if offsetKeyIndex >= 0 {
			offsetIdx, err := ConvertLong(scanArgs[offsetKeyIndex])
			if err != nil {
				return false, -1
			}
			return false, offsetIdx
		}
		return false, -1
	}
	return true, -1
}

// 获取有效数据
func (r *MysqlReader) getValidData(curDB, rawData string, now time.Time, queryType int, db *sql.DB) (validData []string, sqls string, err error) {
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

	rowsDBs, err := db.Query(query)
	if err != nil {
		log.Errorf("Runner[%v] %v prepare MYSQL <%v> query error %v", r.meta.RunnerName, curDB, query, err)
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
		if queryType == TABLE && r.doneRecords.CheckDoneRecords(s, curDB) {
			continue
		}

		// 不导入所有数据时，需要进行匹配
		if !getAll {
			magicRes, err := GoMagicIndex(rawData, now)
			if err != nil {
				return nil, "", err
			}

			magicRemainStr := GetRemainStr(magicRes.Ret, magicRes.RemainIndex)
			if !r.compareData(queryType, curDB, s, magicRemainStr, &magicRes) {
				log.Debugf("Runner[%v] %v current data: %v, current time data: %v, remain str: %v, timeIndex: %v", r.meta.RunnerName, curDB, s, magicRes.Ret, magicRemainStr, magicRes.RemainIndex)
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

func (r *MysqlReader) getConnectStr(database string, now time.Time) string {
	connectStr := r.datasource + "/" + database
	if r.encoder != "" {
		connectStr += "?charset=" + r.encoder
	}
	return connectStr
}

func (r *MysqlReader) Lag() (rl *models.LagInfo, err error) {
	rl = &models.LagInfo{SizeUnit: "records"}
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

func (r *MysqlReader) checkCron() bool {
	return r.isLoop || r.cronSchedule
}

// 是否获取所有数据
func (r *MysqlReader) getAll(queryType int) (getAll bool, err error) {
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

// 根据 queryType 获取 table 中所有记录或者表中所有数据的条数的sql语句
func (r *MysqlReader) getRawSqls(queryType int, table string) (sqls string, err error) {
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
func (r *MysqlReader) getQuery(queryType int, curDB string) (query string, err error) {
	switch queryType {
	case TABLE, COUNT:
		return strings.Replace(DefaultMySQLTable, "DATABASE_NAME", curDB, -1), nil
	case DATABASE:
		return DefaultMySQLDatabase, nil
	default:
		return "", fmt.Errorf("%v queryType is not support get sql now", queryType)
	}
}

// 计算每个table的记录条数
func (r *MysqlReader) execTableCount(connectStr string, idx int, curDB, rawSql string) (tableSize int64, err error) {
	execSQL := r.getSQL(idx, rawSql)
	log.Infof("Runner[%v] reader <%v> exec sql <%v>", r.meta.RunnerName, curDB, execSQL)

	db, err := OpenSql(ModeMySQL, connectStr)
	if err != nil {
		return 0, err
	}
	defer db.Close()

	rows, err := db.Query(execSQL)
	if err != nil {
		log.Errorf("Runner[%v] %v prepare MYSQL <%v> query error %v", r.meta.RunnerName, curDB, execSQL, err)
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

func (r *MysqlReader) getAllDatas(rows *sql.Rows, scanArgs []interface{}, columns []string, nochiced []bool) ([]readInfo, bool) {
	datas := make([]readInfo, 0)
	for rows.Next() {
		// get RawBytes from data
		err := rows.Scan(scanArgs...)
		if err != nil {
			err = fmt.Errorf("runner[%v] %v scan rows error %v", r.meta.RunnerName, r.Name(), err)
			log.Error(err)
			r.sendError(err)
			continue
		}

		var (
			totalBytes int64
			data       = make(models.Data, len(scanArgs))
		)
		for i := 0; i < len(scanArgs); i++ {
			bytes, err := ConvertScanArgs(data, scanArgs[i], columns[i], r.meta.RunnerName, r.Name(), nochiced[i], r.schemas)
			if err != nil {
				r.sendError(err)
			}

			totalBytes += bytes
		}
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
func (r *MysqlReader) execReadSql(curDB string, idx int, execSQL string, db *sql.DB) (exit bool, readSize int64, err error) {
	exit = true

	log.Debugf("Runner[%v] reader <%v> start to exec sql <%v>", r.meta.RunnerName, r.Name(), execSQL)
	rows, err := db.Query(execSQL)
	if err != nil {
		err = fmt.Errorf("runner[%v] %v prepare <%v> query error %v", r.meta.RunnerName, r.Name(), execSQL, err)
		log.Error(err)
		r.sendError(err)
		return exit, readSize, err
	}
	defer rows.Close()
	// Get column names
	columns, err := rows.Columns()
	if err != nil {
		err = fmt.Errorf("runner[%v] %v prepare <%v> columns error %v", r.meta.RunnerName, r.Name(), execSQL, err)
		log.Error(err)
		r.sendError(err)
		return exit, readSize, err
	}
	log.Debugf("Runner[%v] SQL ：<%v>, got schemas: <%v>", r.meta.RunnerName, execSQL, strings.Join(columns, ", "))
	scanArgs, nochiced := GetInitScans(len(columns), rows, r.schemas, r.meta.RunnerName, r.Name())
	var offsetKeyIndex int
	if r.rawSQLs != "" {
		offsetKeyIndex = GetOffsetIndex(r.offsetKey, columns)
	}

	alldatas, closed := r.getAllDatas(rows, scanArgs, columns, nochiced)
	if closed {
		return exit, readSize, nil
	}

	// Fetch rows
	var maxOffset int64 = -1
	for _, v := range alldatas {
		exit = false
		r.readChan <- v
		r.CurrentCount++
		readSize++

		if r.historyAll || r.rawSQLs == "" {
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
	return exit, readSize, rows.Err()
}

func (r *MysqlReader) updateOffset(idx, offsetKeyIndex int, maxOffset int64, scanArgs []interface{}) int64 {
	if offsetKeyIndex >= 0 {
		tmpOffsetIndex, err := ConvertLong(scanArgs[offsetKeyIndex])
		if err != nil {
			log.Errorf("Runner[%v] %v offset key value parse error %v, offset was not recorded", r.meta.RunnerName, r.Name(), err)
			return maxOffset
		}
		if tmpOffsetIndex > maxOffset {
			return tmpOffsetIndex
		}
		return maxOffset
	}

	r.muxOffsets.Lock()
	r.offsets[idx]++
	r.muxOffsets.Unlock()

	return maxOffset
}

func (r *MysqlReader) addCount(current int64) {
	r.countLock.Lock()
	defer r.countLock.Unlock()
	r.count += current
}

func (r *MysqlReader) getCount() int64 {
	r.countLock.RLock()
	defer r.countLock.RUnlock()
	return r.count
}

func (r *MysqlReader) getDBs(connectStr string, now time.Time) ([]string, error) {
	db, err := OpenSql(ModeMySQL, connectStr)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	// 获取所有符合条件的数据库
	dbsAll, _, err := r.getValidData("", r.rawDatabase, now, DATABASE, db)
	if err != nil {
		return dbsAll, err
	}

	dbs := make([]string, 0, len(dbsAll))
	for _, db := range dbsAll {
		if Contains(MysqlSystemDB, strings.ToLower(db)) {
			continue
		}
		dbs = append(dbs, db)
	}

	return dbs, nil
}

// 不为 * 时，进行匹配
func (r *MysqlReader) compareData(queryType int, curDB, target, magicRemainStr string, magicRes *MagicRes) bool {
	if magicRes == nil {
		return true
	}

	match := CompareRemainStr(target, magicRemainStr, magicRes.Ret, magicRes.RemainIndex)
	log.Debugf("Runner[%v] %v current data: %v, current time data: %v, remain str: %v, magicRemainIndex: %v, isMatch: %v", r.meta.RunnerName, curDB, target, magicRes.Ret, magicRemainStr, magicRes.RemainIndex, match)
	if !match {
		return false
	}

	if r.historyAll {
		// 取大于等于上一条的和小于等于现有的
		if CompareTime(target, magicRes.Ret, magicRes.TimeStart, magicRes.TimeEnd, true) &&
			r.greaterThanLastRecord(queryType, target, magicRemainStr, magicRes) {
			return true
		}
	} else {
		// 执行一次，应符合渲染结果
		if !r.checkCron() {
			return EqualTime(target, magicRes.Ret, magicRes.TimeStart, magicRes.TimeEnd)
		}

		// 取大于等于上一条的和小于等于现有的
		if CompareTime(target, magicRes.Ret, magicRes.TimeStart, magicRes.TimeEnd, true) &&
			r.greaterThanLastRecord(queryType, target, magicRemainStr, magicRes) {
			return true
		}
	}

	return false
}

// 取大于等于 最后一条记录 的数据，结果为 true 为小于或者不符合, false 为大于等于
func (r *MysqlReader) greaterThanLastRecord(queryType int, target, magicRemainStr string, magicRes *MagicRes) bool {
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

	match := CompareRemainStr(rawData, magicRemainStr, magicRes.Ret, magicRes.RemainIndex)
	if !match {
		return false
	}

	return CompareTime(target, rawData, magicRes.TimeStart, magicRes.TimeEnd, false)
}
