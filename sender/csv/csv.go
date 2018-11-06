package csv

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"time"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/sender"
	. "github.com/qiniu/logkit/sender/config"
	"github.com/qiniu/logkit/utils/models"
	"github.com/qiniu/logkit/utils/ratelimit"
)

const (
	defaultRotateSize = 10 * 1024 * 1024 // 10MB
	defaultPathPrefix = "./sync.csv"
)

func init() {
	sender.RegisterConstructor(TypeCSV, NewSender)
}

type writer struct {
	w        *csv.Writer
	file     *os.File
	apprSize int64

	fields     []string
	delimeter  string
	rotateSize int64
	pathPrefix string
}

func (w *writer) Write(rawdata []models.Data) error {
	if len(rawdata) == 0 {
		return nil
	}

	if w.NeedRotate() {
		if err := w.RotateNow(); err != nil {
			return err
		}
	}

	var bytes int64
	records := make([][]string, 0, len(rawdata))
	for _, d := range rawdata {
		record := make([]string, 0, len(d))
		for _, field := range w.fields {
			value := asString(d[field])
			bytes += int64(len(value))
			record = append(record, value)
		}
		records = append(records, record)
	}
	return w.WriteAll(records, bytes)
}

func asString(i interface{}) string {
	switch v := i.(type) {
	case string:
		return v
	case []byte:
		return string(v)
	}
	rv := reflect.ValueOf(i)
	switch rv.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(rv.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return strconv.FormatUint(rv.Uint(), 10)
	case reflect.Float64:
		return strconv.FormatFloat(rv.Float(), 'f', -1, 64)
	case reflect.Float32:
		return strconv.FormatFloat(rv.Float(), 'f', -1, 32)
	case reflect.Bool:
		return strconv.FormatBool(rv.Bool())
	case reflect.Map, reflect.Slice, reflect.Array:
		data, _ := json.Marshal(rv.Interface())
		return string(data)
	}
	return fmt.Sprintf("%v", i)
}

func (w *writer) WriteAll(records [][]string, bytes int64) error {
	if w.w != nil {
		if err := w.w.WriteAll(records); err != nil {
			return err
		}
	}
	w.apprSize += bytes
	w.apprSize += int64(len(w.fields) * len(records))
	return nil
}

func (w *writer) NeedRotate() bool {
	if w.w == nil {
		return true
	}
	return w.apprSize >= w.rotateSize
}

func (w *writer) RotateNow() (err error) {
	if err = w.Close(); err != nil {
		return
	}
	w.file, err = w.CreateFile()
	if err != nil {
		return
	}
	w.w = csv.NewWriter(w.file)
	w.apprSize = 0
	return nil
}

func (w *writer) CreateFile() (*os.File, error) {
	now := time.Now()
	pathname := fmt.Sprintf("%s-%.2d%.2d%.2d%.2d%.2d", w.pathPrefix,
		now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second())
	return os.Create(pathname)
}

func (w *writer) Close() error {
	if w.w != nil {
		w.w.Flush()
		w.w = nil
	}
	if w.file != nil {
		if err := w.file.Close(); err != nil {
			return err
		}
	}
	return nil
}

type Sender struct {
	w    *writer
	name string

	limiter ratelimit.Limiter
}

func NewSender(conf conf.MapConf) (s sender.Sender, err error) {
	fields, err := conf.GetStringList(KeyCSVFields)
	if err != nil {
		return
	}
	delimeter, err := conf.GetStringOr(KeyCSVDelimiter, ",")
	if err != nil {
		return
	}
	rotateSize, err := conf.GetInt64Or(KeyCSVRotateSize, defaultRotateSize)
	if err != nil {
		return
	}
	pathPrefix, _ := conf.GetStringOr(KeyCSVPathPrefix, defaultPathPrefix)

	w := &writer{
		fields:     fields,
		delimeter:  delimeter,
		rotateSize: rotateSize,
		pathPrefix: pathPrefix,
	}
	name, _ := conf.GetStringOr(KeyName, fmt.Sprintf("sqlfile(path_prefix:%s)", pathPrefix))
	rate, _ := conf.GetInt64Or(KeyMaxSendRate, -1)
	return &Sender{
		w:       w,
		name:    name,
		limiter: ratelimit.NewLimiter(rate),
	}, nil
}

func (s *Sender) Send(records []models.Data) error {
	s.limiter.Limit(uint64(len(records)))
	return s.w.Write(records)
}

func (s *Sender) Name() string {
	return s.name
}

func (s *Sender) Close() error {
	return s.w.Close()
}
