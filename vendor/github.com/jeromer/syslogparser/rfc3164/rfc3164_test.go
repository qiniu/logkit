package rfc3164

import (
	"bytes"
	"testing"
	"time"

	"github.com/jeromer/syslogparser"
	"github.com/stretchr/testify/assert"
)

const lastTriedTimestampLen = 15

func TestParser_Valid(t *testing.T) {
	buff := []byte("<34>Oct 11 22:14:15 mymachine very.large.syslog.message.tag: 'su root' failed for lonvick on /dev/pts/8")

	p := NewParser(buff)
	expectedP := &Parser{
		buff:     buff,
		cursor:   0,
		buffLen:  len(buff),
		location: time.UTC,
	}
	assert.Equal(t, p, expectedP)
	now := time.Now()
	expected := syslogparser.LogParts{
		"timestamp": time.Date(now.Year(), time.October, 11, 22, 14, 15, 0, time.UTC),
		"hostname":  "mymachine",
		"tag":       "very.large.syslog.message.tag",
		"content":   "'su root' failed for lonvick on /dev/pts/8",
		"priority":  34,
		"facility":  4,
		"severity":  2,
	}
	err := p.Parse()
	assert.NoError(t, err)

	obtained := p.Dump()

	assert.Equal(t, expected, obtained)
}

func TestParser_ValidNoTag(t *testing.T) {
	buff := []byte("<34>Oct 11 22:14:15 mymachine singleword")
	p := NewParser(buff)
	err := p.Parse()
	assert.NoError(t, err)
	now := time.Now()
	obtained := p.Dump()
	expected := syslogparser.LogParts{
		"timestamp": time.Date(now.Year(), time.October, 11, 22, 14, 15, 0, time.UTC),
		"hostname":  "mymachine",
		"tag":       "",
		"content":   "singleword",
		"priority":  34,
		"facility":  4,
		"severity":  2,
	}
	assert.Equal(t, expected, obtained)
}

// RFC 3164 section 4.3.2
func TestParser_NoTimstamp(t *testing.T) {
	buff := []byte("<14>INFO     leaving (1) step postscripts")
	p := NewParser(buff)
	err := p.Parse()
	assert.NoError(t, err)

	now := time.Now()
	obtained := p.Dump()
	obtained["timestamp"] = now // XXX: Need to mock out time to test this fully
	expected := syslogparser.LogParts{
		"timestamp": now,
		"hostname":  "",
		"tag":       "",
		"content":   "INFO     leaving (1) step postscripts",
		"priority":  14,
		"facility":  1,
		"severity":  6,
	}
	assert.Equal(t, expected, obtained)
}

func TestParserHeader_Valid(t *testing.T) {
	buff := []byte("Oct 11 22:14:15 mymachine ")
	now := time.Now()
	hdr := header{
		timestamp: time.Date(now.Year(), time.October, 11, 22, 14, 15, 0, time.UTC),
		hostname:  "mymachine",
	}
	assertRfc3164Header(t, hdr, buff, 25, nil)
}

func TestParserHeader_InvalidTimestamp(t *testing.T) {
	buff := []byte("Oct 34 32:72:82 mymachine ")
	hdr := header{}
	assertRfc3164Header(t, hdr, buff, lastTriedTimestampLen+1, syslogparser.ErrTimestampUnknownFormat)
}

func TestParsemessage_Valid(t *testing.T) {
	content := "foo bar baz blah quux"
	buff := []byte("sometag[123]: " + content)
	hdr := rfc3164message{
		tag:     "sometag",
		content: content,
	}
	assertRfc3164message(t, hdr, buff, len(buff), syslogparser.ErrEOL)
}

func TestParseTimestamp_Invalid(t *testing.T) {
	buff := []byte("Oct 34 32:72:82")
	ts := new(time.Time)
	assertTimestamp(t, *ts, buff, lastTriedTimestampLen, syslogparser.ErrTimestampUnknownFormat)
}

func TestParseTimestamp_TrailingSpace(t *testing.T) {
	// XXX : no year specified. Assumed current year
	// XXX : no timezone specified. Assume UTC
	buff := []byte("Oct 11 22:14:15 ")
	now := time.Now()
	ts := time.Date(now.Year(), time.October, 11, 22, 14, 15, 0, time.UTC)
	assertTimestamp(t, ts, buff, len(buff), nil)
}

func TestParseTimestamp_NeDigitForMonths(t *testing.T) {
	// XXX : no year specified. Assumed current year
	// XXX : no timezone specified. Assume UTC
	buff := []byte("Oct  1 22:14:15")

	now := time.Now()
	ts := time.Date(now.Year(), time.October, 1, 22, 14, 15, 0, time.UTC)

	assertTimestamp(t, ts, buff, len(buff), nil)
}

func TestParseTimestamp_Valid(t *testing.T) {
	// XXX : no year specified. Assumed current year
	// XXX : no timezone specified. Assume UTC
	buff := []byte("Oct 11 22:14:15")

	now := time.Now()
	ts := time.Date(now.Year(), time.October, 11, 22, 14, 15, 0, time.UTC)

	assertTimestamp(t, ts, buff, len(buff), nil)
}

func TestParseTag_Pid(t *testing.T) {
	buff := []byte("apache2[10]:")
	tag := "apache2"

	assertTag(t, tag, buff, len(buff), nil)
}

func TestParseTag_NoPid(t *testing.T) {
	buff := []byte("apache2:")
	tag := "apache2"

	assertTag(t, tag, buff, len(buff), nil)
}

func TestParseTag_TrailingSpace(t *testing.T) {
	buff := []byte("apache2: ")
	tag := "apache2"

	assertTag(t, tag, buff, len(buff), nil)
}

func TestParseTag_NoTag(t *testing.T) {
	buff := []byte("apache2")
	tag := ""

	assertTag(t, tag, buff, 0, nil)
}

func TestParseContent_Valid(t *testing.T) {
	buff := []byte(" foo bar baz quux ")
	content := string(bytes.Trim(buff, " "))

	p := NewParser(buff)
	obtained, err := p.parseContent()
	assert.Equal(t, syslogparser.ErrEOL, err)
	assert.Equal(t, content, obtained)
	assert.Equal(t, len(content), p.cursor)
}

func BenchmarkParse_Timestamp(b *testing.B) {
	buff := []byte("Oct 11 22:14:15")
	p := NewParser(buff)
	for i := 0; i < b.N; i++ {
		_, err := p.parseTimestamp()
		if err != nil {
			panic(err)
		}
		p.cursor = 0
	}
}

func BenchmarkParse_Hostname(b *testing.B) {
	buff := []byte("gimli.local")
	p := NewParser(buff)
	for i := 0; i < b.N; i++ {
		_, err := p.parseHostname()
		if err != nil {
			panic(err)
		}
		p.cursor = 0
	}
}

func BenchmarkParse_Tag(b *testing.B) {
	buff := []byte("apache2[10]:")

	p := NewParser(buff)

	for i := 0; i < b.N; i++ {
		_, err := p.parseTag()
		if err != nil {
			panic(err)
		}

		p.cursor = 0
	}
}

func BenchmarkParse_Header(b *testing.B) {
	buff := []byte("Oct 11 22:14:15 mymachine ")

	p := NewParser(buff)

	for i := 0; i < b.N; i++ {
		_, err := p.parseHeader()
		if err != nil {
			panic(err)
		}

		p.cursor = 0
	}
}

func BenchmarkParse_Message(b *testing.B) {
	buff := []byte("sometag[123]: foo bar baz blah quux")

	p := NewParser(buff)

	for i := 0; i < b.N; i++ {
		_, err := p.parsemessage()
		if err != syslogparser.ErrEOL {
			panic(err)
		}

		p.cursor = 0
	}
}

func assertTimestamp(t *testing.T, ts time.Time, b []byte, expC int, e error) {
	p := NewParser(b)
	obtained, err := p.parseTimestamp()
	assert.Equal(t, e, err)
	assert.Equal(t, ts, obtained)
	assert.Equal(t, expC, p.cursor)
}

func assertTag(t *testing.T, ts string, b []byte, expC int, e error) {
	p := NewParser(b)
	obtained, err := p.parseTag()
	assert.Equal(t, e, err)
	assert.Equal(t, ts, obtained)
	assert.Equal(t, expC, p.cursor)
}

func assertRfc3164Header(t *testing.T, hdr header, b []byte, expC int, e error) {
	p := NewParser(b)
	obtained, err := p.parseHeader()
	assert.Equal(t, e, err)
	assert.Equal(t, hdr, obtained)
	assert.Equal(t, expC, p.cursor)
}

func assertRfc3164message(t *testing.T, msg rfc3164message, b []byte, expC int, e error) {
	p := NewParser(b)
	obtained, err := p.parsemessage()
	assert.Equal(t, e, err)
	assert.Equal(t, msg, obtained)
	assert.Equal(t, expC, p.cursor)
}
