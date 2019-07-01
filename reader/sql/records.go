package sql

import (
	"strconv"
	"strings"
	"sync"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/reader"
	"github.com/qiniu/logkit/utils/models"
)

type DBRecords map[string]TableRecords

type TableInfo struct {
	Size   int64
	Offset int64
}

type TableRecords struct {
	Table map[string]TableInfo
	Mutex sync.RWMutex
}

func (tableRecords *TableRecords) Set(value TableRecords) {
	if tableRecords.GetTable() == nil {
		*tableRecords = TableRecords{
			Table: make(map[string]TableInfo),
			Mutex: sync.RWMutex{},
		}
	}

	valueTable := value.GetTable()
	value.Mutex.RLock()
	if valueTable != nil {
		for key, tableInfo := range valueTable {
			tableRecords.SetTableInfo(key, tableInfo)
		}
	}
	value.Mutex.RUnlock()
}

func (tableRecords *TableRecords) SetTableInfo(table string, tableInfo TableInfo) {
	if tableRecords.GetTable() == nil {
		*tableRecords = TableRecords{
			Table: make(map[string]TableInfo),
			Mutex: sync.RWMutex{},
		}
	}
	tableRecords.Mutex.Lock()
	tableRecords.Table[table] = tableInfo
	tableRecords.Mutex.Unlock()
}

func (tableRecords *TableRecords) GetTableInfo(table string) TableInfo {
	tableRecords.Mutex.RLock()
	defer tableRecords.Mutex.RUnlock()
	if tableRecords.Table != nil {
		return tableRecords.Table[table]
	}
	return TableInfo{}
}

func (tableRecords *TableRecords) GetTable() map[string]TableInfo {
	tableRecords.Mutex.RLock()
	defer tableRecords.Mutex.RUnlock()
	if tableRecords.Table != nil {
		return tableRecords.Table
	}
	return nil
}

func (tableRecords *TableRecords) Reset() {
	*tableRecords = TableRecords{}
}

func (tableRecords *TableRecords) RestoreTableDone(meta *reader.Meta, database string, tables []string) bool {
	omitTableRecords := true
	tablesDoneRecord, err := meta.ReadDBDoneFile(database)
	if err != nil {
		log.Errorf("Runner[%v] %v -table done data is corrupted err:%v, omit table done data", meta.RunnerName, meta.DoneFilePath, err)
		return omitTableRecords
	}

	if len(tablesDoneRecord) == 0 {
		return omitTableRecords
	}
	omitTableRecords = false
	for _, table := range tables {
		if Contains(tablesDoneRecord, table) {
			tableRecords.SetTableInfo(table, TableInfo{
				Size:   -1,
				Offset: -1,
			})
		}
	}
	return omitTableRecords
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

func (dbRecords *DBRecords) SetTableInfo(db, table string, tableInfo TableInfo) {
	if *dbRecords == nil {
		*dbRecords = make(DBRecords)
	}
	tableRecords := (*dbRecords)[db]
	tableRecords.SetTableInfo(table, tableInfo)
	(*dbRecords)[db] = tableRecords
}

func (dbRecords *DBRecords) GetTableRecords(db string) TableRecords {
	if *dbRecords != nil {
		return (*dbRecords)[db]
	}
	return TableRecords{}
}

func (dbRecords *DBRecords) Reset() {
	*dbRecords = make(DBRecords)
}

type SyncDBRecords struct {
	Records DBRecords
	Mutex   sync.RWMutex
}

func (syncDBRecords *SyncDBRecords) SetTableRecords(db string, tableRecords TableRecords) {
	syncDBRecords.Mutex.Lock()
	syncDBRecords.Records.SetTableRecords(db, tableRecords)
	syncDBRecords.Mutex.Unlock()
}

func (syncDBRecords *SyncDBRecords) SetTableInfo(db, table string, tableInfo TableInfo) {
	syncDBRecords.Mutex.Lock()
	syncDBRecords.Records.SetTableInfo(db, table, tableInfo)
	syncDBRecords.Mutex.Unlock()
}

func (syncDBRecords *SyncDBRecords) GetTableRecords(db string) TableRecords {
	syncDBRecords.Mutex.RLock()
	tableRecords := syncDBRecords.Records.GetTableRecords(db)
	syncDBRecords.Mutex.RUnlock()
	return tableRecords
}

func (syncDBRecords *SyncDBRecords) CheckDoneRecords(target, curDB string) bool {
	var tableInfo TableInfo
	tableDoneRecords := syncDBRecords.GetTableRecords(curDB)
	if tableDoneRecords.GetTable() == nil {
		return false
	}

	tableInfo = tableDoneRecords.GetTableInfo(target)
	if tableInfo == (TableInfo{}) {
		return false
	}

	return true
}

func (syncDBRecords *SyncDBRecords) GetDBRecords() DBRecords {
	syncDBRecords.Mutex.RLock()
	dbRecords := syncDBRecords.Records
	syncDBRecords.Mutex.RUnlock()
	return dbRecords
}

func (syncDBRecords *SyncDBRecords) Reset() {
	syncDBRecords.Mutex.Lock()
	syncDBRecords.Records.Reset()
	syncDBRecords.Mutex.Unlock()
}

func (dbRecords *SyncDBRecords) RestoreRecordsFile(meta *reader.Meta) (lastDB, lastTable string, omitDoneDBRecords bool) {
	recordsDone, err := meta.ReadRecordsFile(DefaultDoneRecordsFile)
	if err != nil {
		log.Errorf("Runner[%v] %v -table done data is corrupted err:%v, omit table done data", meta.RunnerName, meta.DoneFilePath, err)
		return lastDB, lastTable, true
	}

	omitDoneDBRecords = true
	recordsDoneLength := len(recordsDone)
	if recordsDoneLength <= 0 {
		return lastDB, lastTable, true
	}

	for idx, record := range recordsDone {
		tmpDBRecords := models.TrimeList(strings.Split(record, SqlOffsetConnector))
		if int64(len(tmpDBRecords)) != 2 {
			log.Errorf("Runner[%v] %v -meta Records done file is invalid sql Records done file %v， omit meta data", meta.RunnerName, meta.MetaFile(), record)
			continue
		}

		database := tmpDBRecords[0]
		var tableRecords TableRecords
		tmpTableRecords := dbRecords.GetTableRecords(database)
		if tmpTableRecords.GetTable() != nil {
			tableRecords.Set(tmpTableRecords)
		}

		tmpTablesRecords := models.TrimeList(strings.Split(tmpDBRecords[1], "@"))
		if int64(len(tmpTablesRecords)) < 1 {
			log.Errorf("Runner[%v] %v -meta Records done file is invalid sql Records done file %v， omit meta data", meta.RunnerName, meta.MetaFile(), tmpDBRecords)
			continue
		}

		for idx, tableRecord := range tmpTablesRecords {
			tableRecordArr := strings.Split(tableRecord, ",")
			if int64(len(tableRecordArr)) != 4 {
				log.Errorf("Runner[%v] %v -meta Records done file is invalid sql Records done file %v， omit meta data", meta.RunnerName, meta.MetaFile(), tableRecord)
				continue
			}

			omitDoneDBRecords = false
			Size, err := strconv.ParseInt(tableRecordArr[1], 10, 64)
			if err != nil {
				log.Errorf("Runner[%v] %v -meta file sql is out of date %v or parse Size err %v， omit this Offset", meta.RunnerName, meta.MetaFile(), tableRecordArr[1], err)
				Size = -1
			}

			Offset, err := strconv.ParseInt(tableRecordArr[1], 10, 64)
			if err != nil {
				log.Errorf("Runner[%v] %v -meta file sql is out of date %v or parse Offset err %v， omit this Offset", meta.RunnerName, meta.MetaFile(), tableRecordArr[1], err)
				Offset = -1
			}

			tableInfo := TableInfo{
				Size:   Size,
				Offset: Offset,
			}
			tableRecords.SetTableInfo(tableRecordArr[0], tableInfo)
			if idx == len(tmpTablesRecords)-1 {
				lastTable = tableRecordArr[0]
			}
		}

		if len(tableRecords.GetTable()) != 0 {
			dbRecords.SetTableRecords(database, tableRecords)
		}
		if idx == recordsDoneLength-1 {
			lastDB = database
		}
	}

	return lastDB, lastTable, omitDoneDBRecords
}
