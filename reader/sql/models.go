package sql

const (
	SqlOffsetConnector   = "##"
	SqlSpliter           = ";"
	DefaultMySQLTable    = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_SCHEMA='DATABASE_NAME';"
	DefaultMySQLDatabase = "SHOW DATABASES;"
	DefaultPGSQLTable    = "SELECT TABLENAME FROM PG_TABLES WHERE SCHEMANAME='SCHEMA_NAME';"
	DefaultMSSQLTable    = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE='BASE TABLE' AND TABLE_CATALOG='DATABASE_NAME' AND TABLE_SCHEMA='SCHEMA_NAME';"

	SupportReminder = "magic only support @(YYYY) @(YY) @(MM) @(DD) @(hh) @(mm) @(ss)"
	Wildcards       = "*"

	DefaultDoneRecordsFile = "sql.records"
	TimestampRecordsFile   = "timestamp.records"
	CacheMapFile           = "cachemap.records"
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

const (
	YEAR = iota
	MONTH
	DAY
	HOUR
	MINUTE
	SECOND
)
