package sqlfile

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/sender"
	"github.com/qiniu/logkit/utils/models"
	"github.com/qiniu/logkit/utils/ratelimit"
)

const (
	defaultRotateSize = 10 * 1024 * 1024 // 10MB
	defaultPathPrefix = "./sync.sql"
)

var bufPool = sync.Pool{
	New: func() interface{} { return new(bytes.Buffer) },
}

func init() {
	sender.RegisterConstructor(sender.TypeSQLFile, NewSender)
}

type writer struct {
	sqlfile  *os.File
	fileSize int64

	inited    bool
	columns   []string
	sqlPrefix string

	table      string
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
	w.InitColumns(rawdata[0])

	buf := bufPool.Get().(*bytes.Buffer)
	buf.Reset()
	defer bufPool.Put(buf)
	for _, d := range rawdata {
		buf.WriteString(w.sqlPrefix)
		buf.WriteByte('(')
		for _, col := range w.columns {
			buf.WriteString(asString(d[col]))
			buf.WriteByte(',')
		}
		buf.Truncate(buf.Len() - 1)
		buf.WriteString(");\n")
	}
	n, err := w.sqlfile.WriteString(buf.String())
	w.fileSize += int64(n)
	return err
}

func asString(i interface{}) string {
	switch v := i.(type) {
	case string:
		return strconv.Quote(v)
	case []byte:
		return strconv.Quote(string(v))
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
		return strconv.Quote(string(data))
	}
	return fmt.Sprintf("%v", i)
}

func (w *writer) InitColumns(firstData models.Data) {
	if w.inited || len(firstData) == 0 {
		return
	}

	for col := range firstData {
		w.columns = append(w.columns, col)
	}
	sort.Strings(w.columns)

	w.sqlPrefix = fmt.Sprintf("INSERT INTO %s(%s) VALUES ",
		w.table, strings.Join(w.columns, ","))
	w.inited = true
}

func (w *writer) NeedRotate() bool {
	if w.sqlfile == nil {
		return true
	}
	return w.fileSize >= w.rotateSize
}

func (w *writer) RotateNow() (err error) {
	if err = w.Close(); err != nil {
		return
	}
	w.sqlfile, err = w.CreateFile()
	if err != nil {
		return
	}
	w.fileSize = 0
	return nil
}

func (w *writer) CreateFile() (*os.File, error) {
	now := time.Now()
	pathname := fmt.Sprintf("%s-%.2d%.2d%.2d%.2d%.2d", w.pathPrefix,
		now.Month(), now.Day(), now.Hour(), now.Minute(), now.Second())
	return os.Create(pathname)
}

func (w *writer) Close() error {
	if w.sqlfile != nil {
		if err := w.sqlfile.Close(); err != nil {
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
	rotateSize, err := conf.GetInt64Or(sender.KeySQLFileRotateSize, defaultRotateSize)
	if err != nil {
		return
	}
	table, err := conf.GetString(sender.KeySQLFileTable)
	if err != nil {
		return
	}
	pathPrefix, _ := conf.GetStringOr(sender.KeySQLFilePathPrefix, defaultPathPrefix)

	w := &writer{
		rotateSize: rotateSize,
		table:      table,
		pathPrefix: pathPrefix,
	}
	name, _ := conf.GetStringOr(sender.KeyName, fmt.Sprintf("sqlfile(table:%s)", table))
	rate, _ := conf.GetInt64Or(sender.KeyMaxSendRate, -1)
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
