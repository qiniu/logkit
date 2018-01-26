package sender

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/conf"
	. "github.com/qiniu/logkit/utils/models"
	"github.com/qiniu/pandora-go-sdk/base/reqerr"
)

// InfluxdbSender write datas into influxdb
type InfluxdbSender struct {
	name        string
	host        string
	db          string
	autoCreate  bool
	retention   string
	duration    string
	measurement string
	tags        map[string]string // key为tag的列名,value为alias名
	fields      map[string]string // key为field的列名，value为alias名
	timestamp   string            // 时间戳列名
	timePrec    int64
}

// Influxdb sender 的可配置字段
const (
	KeyInfluxdbHost               = "influxdb_host"
	KeyInfluxdbDB                 = "influxdb_db"
	KeyInfluxdbAutoCreate         = "influxdb_autoCreate"
	KeyInfluxdbRetetion           = "influxdb_retention"
	KeyInfluxdbRetetionDuration   = "influxdb_retention_duration"
	KeyInfluxdbMeasurement        = "influxdb_measurement"
	KeyInfluxdbTags               = "influxdb_tags"
	KeyInfluxdbFields             = "influxdb_fields"              // influxdb
	KeyInfluxdbTimestamp          = "influxdb_timestamp"           // 可选 nano时间戳字段
	KeyInfluxdbTimestampPrecision = "influxdb_timestamp_precision" // 时间戳字段的精度，代表时间戳1个单位代表多少纳秒
)

// NewInfluxdbSender 创建Influxdb 的sender
func NewInfluxdbSender(c conf.MapConf) (s Sender, err error) {
	host, err := c.GetString(KeyInfluxdbHost)
	if err != nil {
		return
	}
	db, err := c.GetString(KeyInfluxdbDB)
	if err != nil {
		return
	}
	autoCreate, _ := c.GetBoolOr(KeyInfluxdbAutoCreate, true)
	measurement, err := c.GetString(KeyInfluxdbMeasurement)
	if err != nil {
		return
	}
	fields, err := c.GetAliasMap(KeyInfluxdbFields)
	if err != nil {
		return
	}
	tags, _ := c.GetAliasMapOr(KeyInfluxdbTags, make(map[string]string))
	retention, _ := c.GetStringOr(KeyInfluxdbRetetion, "")
	duration, _ := c.GetStringOr(KeyInfluxdbRetetionDuration, "")
	timestamp, _ := c.GetStringOr(KeyInfluxdbTimestamp, "")
	prec, _ := c.GetIntOr(KeyInfluxdbTimestampPrecision, 1)
	name, _ := c.GetStringOr(KeyName, fmt.Sprintf("influxdbSender:(%v,db:%v,measurement:%v", host, db, measurement))

	s = &InfluxdbSender{
		name:        name,
		host:        host,
		db:          db,
		autoCreate:  autoCreate,
		retention:   retention,
		duration:    duration,
		measurement: measurement,
		tags:        tags,
		fields:      fields,
		timestamp:   timestamp,
		timePrec:    int64(prec),
	}
	if autoCreate {
		if err = CreateInfluxdbDatabase(host, db, name); err != nil {
			return
		}
		if retention != "" {
			if err = CreateInfluxdbRetention(host, db, retention, duration, name); err != nil {
				return
			}
		}
	}
	return
}

func (s *InfluxdbSender) Name() string {
	return s.name
}

func (s *InfluxdbSender) Close() error {
	return nil
}

func (s *InfluxdbSender) Send(datas []Data) error {
	ps := Points{}
	for _, d := range datas {
		p, err := s.makePoint(d)
		if err != nil {
			log.Warnf("%s make point format err : %v", s.Name(), err)
			continue
		}
		ps = append(ps, p)
	}
	err := s.sendPoints(ps)
	if err != nil {
		return reqerr.NewSendError(s.Name()+" Cannot write data into influxdb, error is "+err.Error(), ConvertDatasBack(datas), reqerr.TypeDefault)
	}
	return nil
}

func postForm(host string, influxdbSql string, sender string) (err error) {
	data := url.Values{}
	data.Set("q", influxdbSql)

	resp, err := http.DefaultClient.PostForm(host+"/query", data)
	if resp != nil {
		defer func() {
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
		}()
	}
	if err != nil {
		return fmt.Errorf("%s request influxdb error: %v", sender, err)
	}
	if resp.StatusCode != http.StatusOK {
		buf, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			return fmt.Errorf("%s read resp body error: %v", sender, err)
		}
		return fmt.Errorf(strings.Replace(string(buf), "\\", "", -1))
	}
	return
}

func CreateInfluxdbDatabase(host, database, sender string) (err error) {
	influxdbSql := fmt.Sprintf("CREATE DATABASE %s", database)
	log.Infof("%s create database by %q", sender, influxdbSql)
	return postForm(host, influxdbSql, sender)
}

func CreateInfluxdbRetention(host, database, retention, duration, sender string) (err error) {
	influxdbSql := fmt.Sprintf("CREATE RETENTION POLICY %s ON %s DURATION %s REPLICATION 1 DEFAULT", retention, database, duration)
	log.Infof("%s create retention by %q", sender, influxdbSql)
	return postForm(host, influxdbSql, sender)
}

func (s *InfluxdbSender) sendPoints(ps Points) (err error) {
	host := s.host
	if !strings.HasPrefix(host, "http://") {
		host = "http://" + host
	}
	db := s.db
	u := host + "/write?db=" + db
	if s.retention != "" {
		u = u + "&rp=" + s.retention
	}
	req, err := http.NewRequest("POST", u, bytes.NewReader(ps.Buffer()))
	if err != nil {
		log.Errorf("%s writePoints NewRequest error: %v", s.Name(), err)
		return
	}
	req.Header.Set("Content-Type", "text/plain")

	resp, err := http.DefaultClient.Do(req)
	if resp != nil {
		defer func() {
			io.Copy(ioutil.Discard, resp.Body)
			resp.Body.Close()
		}()
	}
	if err != nil {
		log.Errorf("%s request influxdb error: %v", s.Name(), err)
		return
	}

	var b []byte
	if b, err = ioutil.ReadAll(resp.Body); err != nil {
		log.Errorf("%s read resp body error: %v", s.Name(), err)
		return
	}
	if resp.StatusCode != 204 {
		err = fmt.Errorf(strings.Replace(string(b), "\\", "", -1))
		return
	}
	return
}

func (s *InfluxdbSender) makePoint(d Data) (p Point, err error) {
	p.Measurement = s.measurement
	tags := map[string]string{}
	for k, t := range s.tags {
		v, exist := d[k]
		if !exist {
			log.Debugf("%s tag %s not exist", s.Name(), t)
			continue
		}
		tags[t] = fmt.Sprintf("%v", v)
	}
	p.Tags = tags
	fields := map[string]interface{}{}
	for k, f := range s.fields {
		v, exist := d[k]
		if !exist {
			log.Debugf("%s field %s not exist", s.Name(), f)
			continue
		}
		fields[f] = v
	}
	if len(fields) <= 0 {
		return p, errors.New("must contain at least 1 field ")
	}
	p.Fields = fields

	t, exist := d[s.timestamp]
	t1, succ := t.(int64)
	if exist && succ {
		p.Time = t1 * s.timePrec
	}

	return
}

//write points related

type Point struct {
	Measurement string
	Tags        map[string]string
	Fields      map[string]interface{}
	Time        int64
}

type Points []Point

func (ps Points) Buffer() []byte {
	var buf bytes.Buffer
	for _, p := range ps {
		buf.WriteString(p.String())
		buf.WriteByte('\n')
	}
	if len(ps) > 0 {
		buf.Truncate(buf.Len() - 1)
	}
	return buf.Bytes()
}

type Tags map[string]string

// HashKey hashes all of a tag's keys.
func (t Tags) HashKey() []byte {
	// Empty maps marshal to empty bytes.
	if len(t) == 0 {
		return nil
	}

	escaped := Tags{}
	for k, v := range t {
		ek := escapeTag([]byte(k))
		ev := escapeTag([]byte(v))

		if len(ev) > 0 {
			escaped[string(ek)] = string(ev)
		}
	}

	// Extract keys and determine final size.
	sz := len(escaped) + (len(escaped) * 2) // separators
	keys := make([]string, len(escaped)+1)
	i := 0
	for k, v := range escaped {
		keys[i] = k
		i++
		sz += len(k) + len(v)
	}
	keys = keys[:i]
	sort.Strings(keys)
	// Generate marshaled bytes.
	b := make([]byte, sz)
	buf := b
	idx := 0
	for _, k := range keys {
		buf[idx] = ','
		idx++
		copy(buf[idx:idx+len(k)], k)
		idx += len(k)
		buf[idx] = '='
		idx++
		v := escaped[k]
		copy(buf[idx:idx+len(v)], v)
		idx += len(v)
	}
	return b[:idx]
}

func (p *Point) String() string {
	if p.Time == 0 {
		return fmt.Sprintf("%s %s", string(p.Key()), string(p.GetFields()))
	}
	return fmt.Sprintf("%s %s %s", string(p.Key()), string(p.GetFields()), strconv.FormatInt(int64(p.Time), 10))

}

func (p *Point) Key() []byte {
	return MakeKey([]byte(p.Measurement), p.Tags)
}

func (p *Point) GetFields() []byte {
	b := []byte{}
	keys := make([]string, len(p.Fields))
	i := 0
	for k := range p.Fields {
		keys[i] = k
		i++
	}
	sort.Strings(keys)

	for _, k := range keys {
		v := p.Fields[k]
		b = append(b, []byte(String(k))...)
		b = append(b, '=')
		switch t := v.(type) {
		case int:
			b = append(b, []byte(strconv.FormatInt(int64(t), 10))...)
			b = append(b, 'i')
		case int8:
			b = append(b, []byte(strconv.FormatInt(int64(t), 10))...)
			b = append(b, 'i')
		case int16:
			b = append(b, []byte(strconv.FormatInt(int64(t), 10))...)
			b = append(b, 'i')
		case int32:
			b = append(b, []byte(strconv.FormatInt(int64(t), 10))...)
			b = append(b, 'i')
		case int64:
			b = append(b, []byte(strconv.FormatInt(t, 10))...)
			b = append(b, 'i')
		case uint:
			b = append(b, []byte(strconv.FormatInt(int64(t), 10))...)
			b = append(b, 'i')
		case uint8:
			b = append(b, []byte(strconv.FormatInt(int64(t), 10))...)
			b = append(b, 'i')
		case uint16:
			b = append(b, []byte(strconv.FormatInt(int64(t), 10))...)
			b = append(b, 'i')
		case uint32:
			b = append(b, []byte(strconv.FormatInt(int64(t), 10))...)
			b = append(b, 'i')
		case uint64:
			val := []byte(strconv.FormatFloat(float64(t), 'f', -1, 64))
			b = append(b, val...)
		case float32:
			val := []byte(strconv.FormatFloat(float64(t), 'f', -1, 32))
			b = append(b, val...)
		case float64:
			val := []byte(strconv.FormatFloat(t, 'f', -1, 64))
			b = append(b, val...)
		case bool:
			b = append(b, []byte(strconv.FormatBool(t))...)
		case []byte:
			b = append(b, t...)
		case string:
			b = append(b, '"')
			b = append(b, []byte(escapeStringField(t))...)
			b = append(b, '"')
		case nil:
			// skip
		default:
			// Can't determine the type, so convert to string
			b = append(b, '"')
			b = append(b, []byte(escapeStringField(fmt.Sprintf("%v", v)))...)
			b = append(b, '"')

		}
		b = append(b, ',')
	}
	if len(b) > 0 {
		return b[0 : len(b)-1]
	}
	return b
}
func escapeStringField(in string) string {
	var out []byte
	i := 0
	for {
		if i >= len(in) {
			break
		}
		// escape double-quotes
		if in[i] == '\\' {
			out = append(out, '\\')
			out = append(out, '\\')
			i++
			continue
		}
		// escape double-quotes
		if in[i] == '"' {
			out = append(out, '\\')
			out = append(out, '"')
			i++
			continue
		}
		out = append(out, in[i])
		i++

	}
	return string(out)
}

func escapeMeasurement(in []byte) []byte {
	for b, esc := range measurementEscapeCodes {
		in = bytes.Replace(in, []byte{b}, esc, -1)
	}
	return in
}
func unescapeMeasurement(in []byte) []byte {
	for b, esc := range measurementEscapeCodes {
		in = bytes.Replace(in, esc, []byte{b}, -1)
	}
	return in
}

// MakeKey creates a key for a set of tags.
func MakeKey(name []byte, tags Tags) []byte {
	// unescape the name and then re-escape it to avoid double escaping.
	// The key should always be stored in escaped form.
	return append(escapeMeasurement(unescapeMeasurement(name)), tags.HashKey()...)
}

func escapeTag(in []byte) []byte {
	for b, esc := range tagEscapeCodes {
		if bytes.Contains(in, []byte{b}) {
			in = bytes.Replace(in, []byte{b}, esc, -1)
		}
	}
	return in
}

var (
	measurementEscapeCodes = map[byte][]byte{
		',': []byte(`\,`),
		' ': []byte(`\ `),
	}

	tagEscapeCodes = map[byte][]byte{
		',': []byte(`\,`),
		' ': []byte(`\ `),
		'=': []byte(`\=`),
	}
)

func u32tob(v uint32) []byte {
	b := make([]byte, 4)
	binary.BigEndian.PutUint32(b, v)
	return b
}

func u64Tob(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

var (
	Codes = map[byte][]byte{
		',': []byte(`\,`),
		'"': []byte(`\"`),
		' ': []byte(`\ `),
		'=': []byte(`\=`),
	}

	codesStr = map[string]string{}
)

func init() {
	for k, v := range Codes {
		codesStr[string(k)] = string(v)
	}
}

func UnescapeString(in string) string {
	for b, esc := range codesStr {
		in = strings.Replace(in, esc, b, -1)
	}
	return in
}

func String(in string) string {
	for b, esc := range codesStr {
		in = strings.Replace(in, b, esc, -1)
	}
	return in
}
