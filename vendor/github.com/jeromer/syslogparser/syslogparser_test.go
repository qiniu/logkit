package syslogparser

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParsePriority_Empty(t *testing.T) {
	pri := newPriority(0)
	buff := []byte("")
	start := 0

	assertPriority(t, pri, buff, start, start, ErrPriorityEmpty)
}

func TestParsePriority_NoStart(t *testing.T) {
	pri := newPriority(0)
	buff := []byte("7>")
	start := 0

	assertPriority(t, pri, buff, start, start, ErrPriorityNoStart)
}

func TestParsePriority_NoEnd(t *testing.T) {
	pri := newPriority(0)
	buff := []byte("<77")
	start := 0

	assertPriority(t, pri, buff, start, start, ErrPriorityNoEnd)
}

func TestParsePriority_TooShort(t *testing.T) {
	pri := newPriority(0)
	buff := []byte("<>")
	start := 0

	assertPriority(t, pri, buff, start, start, ErrPriorityTooShort)
}

func TestParsePriority_TooLong(t *testing.T) {
	pri := newPriority(0)
	buff := []byte("<1233>")
	start := 0

	assertPriority(t, pri, buff, start, start, ErrPriorityTooLong)
}

func TestParsePriority_NoDigits(t *testing.T) {
	pri := newPriority(0)
	buff := []byte("<7a8>")
	start := 0

	assertPriority(t, pri, buff, start, start, ErrPriorityNonDigit)
}

func TestParsePriority_Ok(t *testing.T) {
	pri := newPriority(190)
	buff := []byte("<190>")
	start := 0

	assertPriority(t, pri, buff, start, start+5, nil)
}

func TestNewPriority(t *testing.T) {
	obtained := newPriority(165)

	expected := Priority{
		P: 165,
		F: Facility{Value: 20},
		S: Severity{Value: 5},
	}

	assert.Equal(t, expected, obtained)
}

func TestParseVersion_NotFound(t *testing.T) {
	buff := []byte("<123>")
	start := 5

	assertVersion(t, NO_VERSION, buff, start, start, ErrVersionNotFound)
}

func TestParseVersion_NonDigit(t *testing.T) {
	buff := []byte("<123>a")
	start := 5

	assertVersion(t, NO_VERSION, buff, start, start+1, nil)
}

func TestParseVersion_Ok(t *testing.T) {
	buff := []byte("<123>1")
	start := 5

	assertVersion(t, 1, buff, start, start+1, nil)
}

func TestParseHostname_Invalid(t *testing.T) {
	// XXX : no year specified. Assumed current year
	// XXX : no timezone specified. Assume UTC
	buff := []byte("foo name")
	start := 0
	hostname := "foo"

	assertHostname(t, hostname, buff, start, 3, nil)
}

func TestParseHostname_Valid(t *testing.T) {
	// XXX : no year specified. Assumed current year
	// XXX : no timezone specified. Assume UTC
	hostname := "ubuntu11.somehost.com"
	buff := []byte(hostname + " ")
	start := 0

	assertHostname(t, hostname, buff, start, len(hostname), nil)
}

func BenchmarkParsePriority(b *testing.B) {
	buff := []byte("<190>")
	var start int
	l := len(buff)

	for i := 0; i < b.N; i++ {
		start = 0
		_, err := ParsePriority(buff, &start, l)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkParseVersion(b *testing.B) {
	buff := []byte("<123>1")
	start := 5
	l := len(buff)

	for i := 0; i < b.N; i++ {
		start = 0
		_, err := ParseVersion(buff, &start, l)
		if err != nil {
			panic(err)
		}
	}
}

func assertPriority(t *testing.T, p Priority, b []byte, cursor int, expC int, e error) {
	obtained, err := ParsePriority(b, &cursor, len(b))
	assert.Equal(t, e, err)
	assert.Equal(t, p, obtained)
	assert.Equal(t, expC, cursor)
}

func assertVersion(t *testing.T, version int, b []byte, cursor int, expC int, e error) {
	obtained, err := ParseVersion(b, &cursor, len(b))
	assert.Equal(t, e, err)
	assert.Equal(t, version, obtained)
	assert.Equal(t, expC, cursor)
}

func assertHostname(t *testing.T, h string, b []byte, cursor int, expC int, e error) {
	obtained, err := ParseHostname(b, &cursor, len(b))
	assert.Equal(t, e, err)
	assert.Equal(t, h, obtained)
	assert.Equal(t, expC, cursor)
}
