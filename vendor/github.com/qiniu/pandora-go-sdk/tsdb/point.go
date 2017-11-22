package tsdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"strconv"
	"strings"
)

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
		return string(p.Key()) + " " + string(p.GetFields())
	}
	return string(p.Key()) + " " + string(p.GetFields()) + " " + strconv.FormatInt(int64(p.Time), 10)

}

func (p *Point) Key() []byte {
	return MakeKey([]byte(p.SeriesName), p.Tags)
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
