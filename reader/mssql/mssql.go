package mssql

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
	_ reader.DaemonReader = &MssqlReader{}
	_ reader.StatsReader  = &MssqlReader{}
	_ reader.DataReader   = &MssqlReader{}
	_ reader.Reader       = &MssqlReader{}
)

func init() {
	reader.RegisterConstructor(ModeMSSQL, NewMssqlReader)
}

type MssqlReader struct {
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

	isLoop       bool
	loopDuration time.Duration
	cronSchedule bool //是否为定时任务
	execOnStart  bool
	Cron         *cron.Cron //定时任务
	readBatch    int        // 每次读取的数据量
	offsetKey    string

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
}

type readInfo struct {
	data  models.Data
	bytes int64
}

func NewMssqlReader(meta *reader.Meta, conf conf.MapConf) (reader.Reader, error) {
	var readBatch int
	var dataSource, rawDatabase, rawSQLs, cronSchedule, offsetKey, dbSchema string
	var execOnStart bool
	logpath, _ := conf.GetStringOr(KeyLogPath, "")

	var err error

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
	rawSchemas, _ := conf.GetStringListOr(KeySQLSchema, []string{})
	magicLagDur, _ := conf.GetStringOr(KeyMagicLagDuration, "")
	var mgld time.Duration
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

	var sqls []string
	omitMeta := true
	var offsets []int64
	if rawSQLs != "" {
		offsets, sqls, omitMeta = RestoreMeta(meta, rawSQLs, mgld)
	}

	r := &MssqlReader{
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
		magicLagDur:   mgld,
		schemas:       schemas,
		dbSchema:      dbSchema,
	}

	if r.rawDatabase == "" {
		r.rawDatabase = "*"
	}

	if r.rawSQLs == "" {
		valid := CheckMagic(r.database)
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

func (r *MssqlReader) run() {
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

	var (
		now              = time.Now().Add(-r.magicLagDur)
		recordTablesDone TableRecords
		tableRecords     = r.doneRecords.GetTableRecords(r.database)
	)
	r.database = magic.GoMagic(r.rawDatabase, now)
	recordTablesDone.Set(tableRecords)

	// 如果执行失败，最多重试 10 次
	for i := 1; i <= 10; i++ {
		// 判断上层是否已经关闭，先判断 routineStatus 再判断 status 可以保证同时只有一个 r.run 会运行到此处
		if r.isStopping() || r.hasStopped() {
			log.Warnf("Runner[%v] %q daemon has stopped, task is interrupted", r.meta.RunnerName, r.Name())
			return
		}

		err := r.execReadDB(r.database, now, recordTablesDone)
		if err == nil {
			log.Infof("Runner[%v] %q task has been successfully executed", r.meta.RunnerName, r.Name())
			return
		}

		log.Errorf("Runner[%v] %v exec read db: %v error: %v,will retry read it", r.meta.RunnerName, r.Name(), r.database, err)
		r.setStatsError(err.Error())
		r.sendError(err)

		if r.isLoop {
			return // 循环执行的任务上层逻辑已经等同重试
		}
		time.Sleep(3 * time.Second)
	}
	log.Errorf("Runner[%v] %q task execution failed and gave up after 10 tries", r.meta.RunnerName, r.Name())
}

func (r *MssqlReader) execReadDB(curDB string, now time.Time, recordTablesDone TableRecords) (err error) {
	connectStr := r.datasource + ";database=" + r.database
	db, err := OpenSql(ModeMSSQL, connectStr)
	if err != nil {
		return err
	}
	defer db.Close()

	//更新sqls
	var tables []string
	var sqls string
	if r.rawSQLs == "" {
		// 获取符合条件的数据表和获取所有数据的语句
		tables, sqls, err = r.getValidData(curDB, db)
		if err != nil {
			log.Errorf("Runner[%s] %s rawTable: %v rawSQLs: %v get tables and sqls error %v", r.meta.RunnerName, r.Name(), "", r.rawSQLs, err)
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
		var (
			exit      = false
			tableName string
			readSize  int64
		)

		for !exit {
			if r.rawSQLs == "" && idx < tablesLen {
				tableName = tables[idx]
				if recordTablesDone.GetTableInfo(tableName) != (TableInfo{}) {
					break
				}
			}

			execSQL := r.getSQL(rawSql, idx)
			// 执行每条 sql 语句
			exit, readSize, err = r.execReadSql(curDB, execSQL, idx, tables, db)
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

// 获取有效数据
func (r *MssqlReader) getValidData(curDB string, db *sql.DB) (validData []string, sqls string, err error) {
	// get all databases and check validate database
	query := strings.Replace(DefaultMSSQLTable, "DATABASE_NAME", curDB, -1)
	query = strings.Replace(query, "SCHEMA_NAME", r.dbSchema, -1)
	rows, err := db.Query(query)
	if err != nil {
		log.Errorf("Runner[%v] %v prepare MSSQL <%v> query error %v", r.meta.RunnerName, curDB, query, err)
		return nil, "", err
	}
	defer rows.Close()

	validData = make([]string, 0)
	for rows.Next() {
		var s string
		err = rows.Scan(&s)
		if err != nil {
			log.Errorf("Runner[%v] %v scan rows error %v", r.meta.RunnerName, curDB, err)
			continue
		}

		if r.doneRecords.CheckDoneRecords(s, curDB) {
			continue
		}

		tableName := fmt.Sprintf("\"%s\".\"%s\"", r.dbSchema, s)
		sqls += "Select * From " + tableName + ";"

		validData = append(validData, s)
	}

	return validData, sqls, nil
}

func (r *MssqlReader) Name() string {
	return "MSSQL_Reader:" + r.rawDatabase + "_" + models.Hash(r.rawSQLs)
}

func (r *MssqlReader) SetMode(mode string, v interface{}) error {
	return errors.New("MSSQL reader does not support read mode")
}

// 执行每条 sql 语句
func (r *MssqlReader) execReadSql(curDB, execSQL string, idx int, tables []string, db *sql.DB) (exit bool, readSize int64, err error) {
	exit = true
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
		readSize++

		if r.rawSQLs == "" {
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

func (r *MssqlReader) setStatsError(err string) {
	r.statsLock.Lock()
	defer r.statsLock.Unlock()
	r.stats.LastError = err
}

func (r *MssqlReader) sendError(err error) {
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

func (r *MssqlReader) checkExit(idx int, db *sql.DB) (bool, int64) {
	if idx >= len(r.offsets) || len(r.offsetKey) <= 0 {
		return true, -1
	}
	rawSQL := strings.TrimSuffix(strings.TrimSpace(r.syncSQLs[idx]), ";")
	rawSQLIdx := strings.Index(rawSQL, "from")
	if rawSQLIdx < 0 {
		return true, -1
	}
	rawSQL = rawSQL[rawSQLIdx:]
	tsql := fmt.Sprintf("select top(1) * %v WHERE CAST(%v AS BIGINT) >= %v order by CAST(%v AS BIGINT);", rawSQL, r.offsetKey, r.offsets[idx], r.offsetKey)

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

func (r *MssqlReader) getAllDatas(rows *sql.Rows, scanArgs []interface{}, columns []string, nochiced []bool) ([]readInfo, bool) {
	datas := make([]readInfo, 0, 100)
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

func (r *MssqlReader) Start() error {
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

func (r *MssqlReader) Source() string {
	// 不能把 DataSource 弄出去，包含密码
	return "MSSQL_" + r.database
}

func (r *MssqlReader) isStopping() bool {
	return atomic.LoadInt32(&r.status) == StatusStopping
}

func (r *MssqlReader) hasStopped() bool {
	return atomic.LoadInt32(&r.status) == StatusStopped
}

func (r *MssqlReader) Status() models.StatsInfo {
	r.statsLock.RLock()
	defer r.statsLock.RUnlock()
	return r.stats
}

func (r *MssqlReader) ReadLine() (string, error) {
	return "", errors.New("method ReadLine is not supported, please use ReadData")
}

func (r *MssqlReader) ReadData() (models.Data, int64, error) {
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

// SyncMeta 从队列取数据时同步队列，作用在于保证数据不重复
func (r *MssqlReader) SyncMeta() {
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

func (r *MssqlReader) Close() error {
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

	r.SyncMeta()
	return nil
}

//check if syncSQLs is out of date
func (r *MssqlReader) updateOffsets(sqls []string) {
	r.muxOffsets.Lock()
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
	r.muxOffsets.Unlock()
}

func (r *MssqlReader) updateOffset(idx, offsetKeyIndex int, maxOffset int64, scanArgs []interface{}) int64 {
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

func (r *MssqlReader) getSQL(rawSql string, idx int) string {
	var rawSQL = strings.TrimSuffix(strings.TrimSpace(rawSql), ";")
	r.muxOffsets.RLock()
	defer r.muxOffsets.RUnlock()
	if len(r.offsetKey) > 0 && len(r.offsets) > idx {
		return fmt.Sprintf("%s WHERE CAST(%v AS BIGINT) >= %d AND CAST(%v AS BIGINT) < %d;", rawSQL, r.offsetKey, r.offsets[idx], r.offsetKey, r.offsets[idx]+int64(r.readBatch))
	}

	log.Warn("get SQL error: MSSQL not support get SQL without id now, use raw SQL")
	return rawSql
}
