package mysqllog

const (
	attributeTypeFloat = iota
	attributeTypeInt
	attributeTypeString
	attributeTypeBool
)

var attributeTypes = map[string]int{
	"Thread_id":             attributeTypeInt,
	"Schema":                attributeTypeString,
	"Last_errno":            attributeTypeInt,
	"Killed":                attributeTypeInt,
	"Query_time":            attributeTypeFloat,
	"Lock_time":             attributeTypeFloat,
	"Rows_sent":             attributeTypeInt,
	"Rows_examined":         attributeTypeInt,
	"Rows_affected":         attributeTypeInt,
	"Rows_read":             attributeTypeInt,
	"Bytes_sent":            attributeTypeInt,
	"Tmp_tables":            attributeTypeInt,
	"Tmp_disk_tables":       attributeTypeInt,
	"Tmp_table_sizes":       attributeTypeInt,
	"InnoDB_trx_id":         attributeTypeString,
	"QC_Hit":                attributeTypeBool,
	"Full_scan":             attributeTypeBool,
	"Full_join":             attributeTypeBool,
	"Tmp_table":             attributeTypeBool,
	"Tmp_table_on_disk":     attributeTypeBool,
	"Filesort":              attributeTypeBool,
	"Filesort_on_disk":      attributeTypeBool,
	"Merge_passes":          attributeTypeInt,
	"InnoDB_IO_r_ops":       attributeTypeInt,
	"InnoDB_IO_r_bytes":     attributeTypeInt,
	"InnoDB_IO_r_wait":      attributeTypeFloat,
	"InnoDB_rec_lock_wait":  attributeTypeFloat,
	"InnoDB_queue_wait":     attributeTypeFloat,
	"InnoDB_pages_distinct": attributeTypeInt,
}
