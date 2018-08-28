package sql

import (
	"strconv"
	"strings"
	"sync"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/reader"
	. "github.com/qiniu/logkit/utils/models"
)

type DBRecords map[string]TableRecords

type TableInfo struct {
	size   int64
	offset int64
}

type TableRecords struct {
	Table map[string]TableInfo
	mutex sync.RWMutex
}

func (tableRecords *TableRecords) Set(value TableRecords) {
	if tableRecords.GetTable() == nil {
		*tableRecords = TableRecords{
			Table: make(map[string]TableInfo),
			mutex: sync.RWMutex{},
		}
	}

	tableRecords.mutex.Lock()

	valueTable := value.GetTable()
	value.mutex.RLock()
	if valueTable != nil {
		for key, value := range valueTable {
			tableRecords.Table[key] = value
		}
	}
	value.mutex.RUnlock()
	tableRecords.mutex.Unlock()
}

func (tableRecords *TableRecords) SetTableInfo(table string, tableInfo TableInfo) {
	if tableRecords.GetTable() == nil {
		*tableRecords = TableRecords{
			Table: make(map[string]TableInfo),
			mutex: sync.RWMutex{},
		}
	}
	tableRecords.mutex.Lock()
	tableRecords.Table[table] = tableInfo
	tableRecords.mutex.Unlock()
}

func (tableRecords *TableRecords) GetTableInfo(table string) TableInfo {
	tableRecords.mutex.RLock()
	defer tableRecords.mutex.RUnlock()
	if tableRecords.Table != nil {
		return tableRecords.Table[table]
	}
	return TableInfo{}
}

func (tableRecords *TableRecords) GetTable() map[string]TableInfo {
	tableRecords.mutex.RLock()
	defer tableRecords.mutex.RUnlock()
	if tableRecords.Table != nil {
		return tableRecords.Table
	}
	return nil
}

func (tableRecords *TableRecords) Reset() {
	*tableRecords = TableRecords{}
}

func (tableRecords *TableRecords) restoreTableDone(meta *reader.Meta, database string, tables []string) bool {
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
		if contains(tablesDoneRecord, table) {
			tableRecords.SetTableInfo(table, TableInfo{
				size:   -1,
				offset: -1,
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
	if tableRecords.GetTable() == nil {
		tableRecords = TableRecords{
			Table: make(map[string]TableInfo),
			mutex: sync.RWMutex{},
		}
	}
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
	records DBRecords
	mutex   sync.RWMutex
}

func (syncDBRecords *SyncDBRecords) SetTableRecords(db string, tableRecords TableRecords) {
	syncDBRecords.mutex.Lock()
	syncDBRecords.records.SetTableRecords(db, tableRecords)
	syncDBRecords.mutex.Unlock()
}

func (syncDBRecords *SyncDBRecords) SetTableInfo(db, table string, tableInfo TableInfo) {
	syncDBRecords.mutex.Lock()
	syncDBRecords.records.SetTableInfo(db, table, tableInfo)
	syncDBRecords.mutex.Unlock()
}

func (syncDBRecords *SyncDBRecords) GetTableRecords(db string) TableRecords {
	syncDBRecords.mutex.RLock()
	tableRecords := syncDBRecords.records.GetTableRecords(db)
	syncDBRecords.mutex.RUnlock()
	return tableRecords
}

func (syncDBRecords *SyncDBRecords) GetDBRecords() DBRecords {
	syncDBRecords.mutex.RLock()
	dbRecords := syncDBRecords.records
	syncDBRecords.mutex.RUnlock()
	return dbRecords
}

func (syncDBRecords *SyncDBRecords) Reset() {
	syncDBRecords.mutex.Lock()
	syncDBRecords.records.Reset()
	syncDBRecords.mutex.Unlock()
}

func (dbRecords *SyncDBRecords) restoreRecordsFile(meta *reader.Meta) (lastDB, lastTable string, omitDoneDBRecords bool) {
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
		tmpDBRecords := TrimeList(strings.Split(record, sqlOffsetConnector))
		if int64(len(tmpDBRecords)) != 2 {
			log.Errorf("Runner[%v] %v -meta records done file is not invalid sql records done file %v， omit meta data", meta.RunnerName, meta.MetaFile(), record)
			continue
		}

		database := tmpDBRecords[0]
		var tableRecords TableRecords
		tmpTableRecords := dbRecords.GetTableRecords(database)
		if tmpTableRecords.GetTable() != nil {
			tableRecords.Set(tmpTableRecords)
		}

		tmpTablesRecords := TrimeList(strings.Split(tmpDBRecords[1], "@"))
		if int64(len(tmpTablesRecords)) < 1 {
			log.Errorf("Runner[%v] %v -meta records done file is not invalid sql records done file %v， omit meta data", meta.RunnerName, meta.MetaFile(), tmpDBRecords)
			continue
		}

		for idx, tableRecord := range tmpTablesRecords {
			tableRecordArr := strings.Split(tableRecord, ",")
			if int64(len(tableRecordArr)) != 4 {
				log.Errorf("Runner[%v] %v -meta records done file is not invalid sql records done file %v， omit meta data", meta.RunnerName, meta.MetaFile(), tableRecord)
				continue
			}

			omitDoneDBRecords = false
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
