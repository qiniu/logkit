package reader

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/utils"
	"github.com/robfig/cron"

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

	for idx := range mr.syncSQLs {
		//分sql执行
		exit := false
		for !exit {
			exit = true
			execSQL := mr.getSQL(idx)
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
			log.Infof("SQL ：<%v>, schemas: <%v>", execSQL, strings.Join(columns, ", "))
			values := make([]sql.RawBytes, len(columns))
			scanArgs := make([]interface{}, len(values))
			for i := range values {
				scanArgs[i] = &values[i]
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
				ret := utils.TuoEncode(values)
				if atomic.LoadInt32(&mr.status) == StatusStoping {
					log.Warnf("%v stopped from running", mr.Name())
					return nil
				}
				mr.readChan <- ret

				if offsetKeyIndex >= 0 {
					mr.offsets[idx], err = strconv.ParseInt(string(values[offsetKeyIndex]), 10, 64)
					if err != nil {
						log.Errorf("%v offset key value parse error %v, offset was not recorded", mr.Name(), err)
						err = nil
					}
				}
				mr.offsets[idx]++
			}
		}
	}
	return nil
}

func (mr *SqlReader) getSQL(idx int) string {
	if len(mr.offsetKey) > 0 {
		return fmt.Sprintf("%s WHERE %v >= %d AND %v < %d;", mr.syncSQLs[idx], mr.offsetKey, mr.offsets[idx], mr.offsetKey, mr.offsets[idx]+int64(mr.readBatch))
	}
	return fmt.Sprintf("%s LIMIT %d,%d;", mr.syncSQLs[idx], mr.offsets[idx], mr.offsets[idx]+int64(mr.readBatch))
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
