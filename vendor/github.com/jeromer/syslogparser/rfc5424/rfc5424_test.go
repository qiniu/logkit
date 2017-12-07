package rfc5424

import (
	"fmt"
	"testing"
	"time"

	"github.com/jeromer/syslogparser"
	"github.com/stretchr/testify/assert"
)

func TestParser_Valid(t *testing.T) {
	fixtures := []string{
		// no STRUCTURED-DATA
		"<34>1 2003-10-11T22:14:15.003Z mymachine.example.com su - ID47 - 'su root' failed for lonvick on /dev/pts/8",
		"<165>1 2003-08-24T05:14:15.000003-07:00 192.0.2.1 myproc 8710 - - %% It's time to make the do-nuts.",
		// with STRUCTURED-DATA
		`<165>1 2003-10-11T22:14:15.003Z mymachine.example.com evntslog - ID47 [exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"] An application event log entry...`,

		// STRUCTURED-DATA Only
		`<165>1 2003-10-11T22:14:15.003Z mymachine.example.com evntslog - ID47 [exampleSDID@32473 iut="3" eventSource= "Application" eventID="1011"][examplePriority@32473 class="high"]`,
	}

	tmpTs, err := time.Parse("-07:00", "-07:00")
	assert.NoError(t, err)

	expected := []syslogparser.LogParts{
		syslogparser.LogParts{
			"priority":        34,
			"facility":        4,
			"severity":        2,
			"version":         1,
			"timestamp":       time.Date(2003, time.October, 11, 22, 14, 15, 3*10e5, time.UTC),
			"hostname":        "mymachine.example.com",
			"app_name":        "su",
			"proc_id":         "-",
			"msg_id":          "ID47",
			"structured_data": "-",
			"message":         "'su root' failed for lonvick on /dev/pts/8",
		},
		syslogparser.LogParts{
			"priority":        165,
			"facility":        20,
			"severity":        5,
			"version":         1,
			"timestamp":       time.Date(2003, time.August, 24, 5, 14, 15, 3*10e2, tmpTs.Location()),
			"hostname":        "192.0.2.1",
			"app_name":        "myproc",
			"proc_id":         "8710",
			"msg_id":          "-",
			"structured_data": "-",
			"message":         "%% It's time to make the do-nuts.",
		},
		syslogparser.LogParts{
			"priority":        165,
			"facility":        20,
			"severity":        5,
			"version":         1,
			"timestamp":       time.Date(2003, time.October, 11, 22, 14, 15, 3*10e5, time.UTC),
			"hostname":        "mymachine.example.com",
			"app_name":        "evntslog",
			"proc_id":         "-",
			"msg_id":          "ID47",
			"structured_data": `[exampleSDID@32473 iut="3" eventSource="Application" eventID="1011"]`,
			"message":         "An application event log entry...",
		},
		syslogparser.LogParts{
			"priority":        165,
			"facility":        20,
			"severity":        5,
			"version":         1,
			"timestamp":       time.Date(2003, time.October, 11, 22, 14, 15, 3*10e5, time.UTC),
			"hostname":        "mymachine.example.com",
			"app_name":        "evntslog",
			"proc_id":         "-",
			"msg_id":          "ID47",
			"structured_data": `[exampleSDID@32473 iut="3" eventSource= "Application" eventID="1011"][examplePriority@32473 class="high"]`,
			"message":         "",
		},
	}

	assert.Equal(t, len(expected), len(fixtures))
	start := 0
	for i, buff := range fixtures {
		expectedP := &Parser{
			buff:   []byte(buff),
			cursor: start,
			l:      len(buff),
		}

		p := NewParser([]byte(buff))
		assert.Equal(t, expectedP, p)

		err := p.Parse()
		assert.NoError(t, err)

		obtained := p.Dump()
		for k, v := range obtained {
			assert.Equal(t, expected[i][k], v)
		}
	}
}

func TestParseHeader_Valid(t *testing.T) {
	ts := time.Date(2003, time.October, 11, 22, 14, 15, 3*10e5, time.UTC)
	tsString := "2003-10-11T22:14:15.003Z"
	hostname := "mymachine.example.com"
	appName := "su"
	procId := "123"
	msgId := "ID47"
	nilValue := string(NILVALUE)
	headerFmt := "<165>1 %s %s %s %s %s "

	fixtures := []string{
		// HEADER complete
		fmt.Sprintf(headerFmt, tsString, hostname, appName, procId, msgId),
		// TIMESTAMP as NILVALUE
		fmt.Sprintf(headerFmt, nilValue, hostname, appName, procId, msgId),
		// HOSTNAME as NILVALUE
		fmt.Sprintf(headerFmt, tsString, nilValue, appName, procId, msgId),
		// APP-NAME as NILVALUE
		fmt.Sprintf(headerFmt, tsString, hostname, nilValue, procId, msgId),
		// PROCID as NILVALUE
		fmt.Sprintf(headerFmt, tsString, hostname, appName, nilValue, msgId),
		// MSGID as NILVALUE
		fmt.Sprintf(headerFmt, tsString, hostname, appName, procId, nilValue),
	}

	pri := syslogparser.Priority{
		P: 165,
		F: syslogparser.Facility{Value: 20},
		S: syslogparser.Severity{Value: 5},
	}

	expected := []header{
		// HEADER complete
		header{
			priority:  pri,
			version:   1,
			timestamp: ts,
			hostname:  hostname,
			appName:   appName,
			procId:    procId,
			msgId:     msgId,
		},
		// TIMESTAMP as NILVALUE
		header{
			priority:  pri,
			version:   1,
			timestamp: *new(time.Time),
			hostname:  hostname,
			appName:   appName,
			procId:    procId,
			msgId:     msgId,
		},
		// HOSTNAME as NILVALUE
		header{
			priority:  pri,
			version:   1,
			timestamp: ts,
			hostname:  nilValue,
			appName:   appName,
			procId:    procId,
			msgId:     msgId,
		},
		// APP-NAME as NILVALUE
		header{
			priority:  pri,
			version:   1,
			timestamp: ts,
			hostname:  hostname,
			appName:   nilValue,
			procId:    procId,
			msgId:     msgId,
		},
		// PROCID as NILVALUE
		header{
			priority:  pri,
			version:   1,
			timestamp: ts,
			hostname:  hostname,
			appName:   appName,
			procId:    nilValue,
			msgId:     msgId,
		},
		// MSGID as NILVALUE
		header{
			priority:  pri,
			version:   1,
			timestamp: ts,
			hostname:  hostname,
			appName:   appName,
			procId:    procId,
			msgId:     nilValue,
		},
	}

	for i, f := range fixtures {
		p := NewParser([]byte(f))
		obtained, err := p.parseHeader()
		assert.NoError(t, err)
		assert.Equal(t, expected[i], obtained)
		assert.Equal(t, len(f), p.cursor)
	}
}

func TestParseTimestamp_UTC(t *testing.T) {
	buff := []byte("1985-04-12T23:20:50.52Z")
	ts := time.Date(1985, time.April, 12, 23, 20, 50, 52*10e6, time.UTC)

	assertTimestamp(t, ts, buff, 23, nil)
}

func TestParseTimestamp_NumericTimezone(t *testing.T) {
	tz := "-04:00"
	buff := []byte("1985-04-12T19:20:50.52" + tz)

	tmpTs, err := time.Parse("-07:00", tz)
	assert.NoError(t, err)

	ts := time.Date(1985, time.April, 12, 19, 20, 50, 52*10e6, tmpTs.Location())

	assertTimestamp(t, ts, buff, len(buff), nil)
}

func TestParseTimestamp_MilliSeconds(t *testing.T) {
	buff := []byte("2003-10-11T22:14:15.003Z")

	ts := time.Date(2003, time.October, 11, 22, 14, 15, 3*10e5, time.UTC)

	assertTimestamp(t, ts, buff, len(buff), nil)
}

func TestParseTimestamp_MicroSeconds(t *testing.T) {
	tz := "-07:00"
	buff := []byte("2003-08-24T05:14:15.000003" + tz)

	tmpTs, err := time.Parse("-07:00", tz)
	assert.NoError(t, err)

	ts := time.Date(2003, time.August, 24, 5, 14, 15, 3*10e2, tmpTs.Location())

	assertTimestamp(t, ts, buff, len(buff), nil)
}

func TestParseTimestamp_NanoSeconds(t *testing.T) {
	buff := []byte("2003-08-24T05:14:15.000000003-07:00")
	ts := new(time.Time)

	assertTimestamp(t, *ts, buff, 26, syslogparser.ErrTimestampUnknownFormat)
}

func TestParseTimestamp_NilValue(t *testing.T) {
	buff := []byte("-")
	ts := new(time.Time)

	assertTimestamp(t, *ts, buff, 1, nil)
}

func TestFindNextSpace_NoSpace(t *testing.T) {
	buff := []byte("aaaaaa")

	assertFindNextSpace(t, 0, buff, syslogparser.ErrNoSpace)
}

func TestFindNextSpace_SpaceFound(t *testing.T) {
	buff := []byte("foo bar baz")

	assertFindNextSpace(t, 4, buff, nil)
}

func TestParseYear_Invalid(t *testing.T) {
	buff := []byte("1a2b")
	expected := 0

	assertParseYear(t, expected, buff, 4, ErrYearInvalid)
}

func TestParseYear_TooShort(t *testing.T) {
	buff := []byte("123")
	expected := 0

	assertParseYear(t, expected, buff, 0, syslogparser.ErrEOL)
}

func TestParseYear_Valid(t *testing.T) {
	buff := []byte("2013")
	expected := 2013

	assertParseYear(t, expected, buff, 4, nil)
}

func TestParseMonth_InvalidString(t *testing.T) {
	buff := []byte("ab")
	expected := 0

	assertParseMonth(t, expected, buff, 2, ErrMonthInvalid)
}

func TestParseMonth_InvalidRange(t *testing.T) {
	buff := []byte("00")
	expected := 0

	assertParseMonth(t, expected, buff, 2, ErrMonthInvalid)

	// ----

	buff = []byte("13")

	assertParseMonth(t, expected, buff, 2, ErrMonthInvalid)
}

func TestParseMonth_TooShort(t *testing.T) {
	buff := []byte("1")
	expected := 0

	assertParseMonth(t, expected, buff, 0, syslogparser.ErrEOL)
}

func TestParseMonth_Valid(t *testing.T) {
	buff := []byte("02")
	expected := 2

	assertParseMonth(t, expected, buff, 2, nil)
}

func TestParseDay_InvalidString(t *testing.T) {
	buff := []byte("ab")
	expected := 0

	assertParseDay(t, expected, buff, 2, ErrDayInvalid)
}

func TestParseDay_TooShort(t *testing.T) {
	buff := []byte("1")
	expected := 0

	assertParseDay(t, expected, buff, 0, syslogparser.ErrEOL)
}

func TestParseDay_InvalidRange(t *testing.T) {
	buff := []byte("00")
	expected := 0

	assertParseDay(t, expected, buff, 2, ErrDayInvalid)

	// ----

	buff = []byte("32")

	assertParseDay(t, expected, buff, 2, ErrDayInvalid)
}

func TestParseDay_Valid(t *testing.T) {
	buff := []byte("02")
	expected := 2

	assertParseDay(t, expected, buff, 2, nil)
}

func TestParseFullDate_Invalid(t *testing.T) {
	buff := []byte("2013+10-28")
	fd := fullDate{}

	assertParseFullDate(t, fd, buff, 4, syslogparser.ErrTimestampUnknownFormat)

	// ---

	buff = []byte("2013-10+28")
	assertParseFullDate(t, fd, buff, 7, syslogparser.ErrTimestampUnknownFormat)
}

func TestParseFullDate_Valid(t *testing.T) {
	buff := []byte("2013-10-28")
	fd := fullDate{
		year:  2013,
		month: 10,
		day:   28,
	}

	assertParseFullDate(t, fd, buff, len(buff), nil)
}

func TestParseHour_InvalidString(t *testing.T) {
	buff := []byte("azer")
	expected := 0

	assertParseHour(t, expected, buff, 2, ErrHourInvalid)
}

func TestParseHour_TooShort(t *testing.T) {
	buff := []byte("1")
	expected := 0

	assertParseHour(t, expected, buff, 0, syslogparser.ErrEOL)
}

func TestParseHour_InvalidRange(t *testing.T) {
	buff := []byte("-1")
	expected := 0

	assertParseHour(t, expected, buff, 2, ErrHourInvalid)

	// ----

	buff = []byte("24")

	assertParseHour(t, expected, buff, 2, ErrHourInvalid)
}

func TestParseHour_Valid(t *testing.T) {
	buff := []byte("12")
	expected := 12

	assertParseHour(t, expected, buff, 2, nil)
}

func TestParseMinute_InvalidString(t *testing.T) {
	buff := []byte("azer")
	expected := 0

	assertParseMinute(t, expected, buff, 2, ErrMinuteInvalid)
}

func TestParseMinute_TooShort(t *testing.T) {
	buff := []byte("1")
	expected := 0

	assertParseMinute(t, expected, buff, 0, syslogparser.ErrEOL)
}

func TestParseMinute_InvalidRange(t *testing.T) {
	buff := []byte("-1")
	expected := 0

	assertParseMinute(t, expected, buff, 2, ErrMinuteInvalid)

	// ----

	buff = []byte("60")

	assertParseMinute(t, expected, buff, 2, ErrMinuteInvalid)
}

func TestParseMinute_Valid(t *testing.T) {
	buff := []byte("12")
	expected := 12

	assertParseMinute(t, expected, buff, 2, nil)
}

func TestParseSecond_InvalidString(t *testing.T) {
	buff := []byte("azer")
	expected := 0

	assertParseSecond(t, expected, buff, 2, ErrSecondInvalid)
}

func TestParseSecond_TooShort(t *testing.T) {
	buff := []byte("1")
	expected := 0

	assertParseSecond(t, expected, buff, 0, syslogparser.ErrEOL)
}

func TestParseSecond_InvalidRange(t *testing.T) {
	buff := []byte("-1")
	expected := 0

	assertParseSecond(t, expected, buff, 2, ErrSecondInvalid)

	// ----

	buff = []byte("60")

	assertParseSecond(t, expected, buff, 2, ErrSecondInvalid)
}

func TestParseSecond_Valid(t *testing.T) {
	buff := []byte("12")
	expected := 12

	assertParseSecond(t, expected, buff, 2, nil)
}

func TestParseSecFrac_InvalidString(t *testing.T) {
	buff := []byte("azerty")
	expected := 0.0

	assertParseSecFrac(t, expected, buff, 0, ErrSecFracInvalid)
}

func TestParseSecFrac_NanoSeconds(t *testing.T) {
	buff := []byte("123456789")
	expected := 0.123456

	assertParseSecFrac(t, expected, buff, 6, nil)
}

func TestParseSecFrac_Valid(t *testing.T) {
	buff := []byte("0")

	expected := 0.0
	assertParseSecFrac(t, expected, buff, 1, nil)

	buff = []byte("52")
	expected = 0.52
	assertParseSecFrac(t, expected, buff, 2, nil)

	buff = []byte("003")
	expected = 0.003
	assertParseSecFrac(t, expected, buff, 3, nil)

	buff = []byte("000003")
	expected = 0.000003
	assertParseSecFrac(t, expected, buff, 6, nil)
}

func TestParseNumericalTimeOffset_Valid(t *testing.T) {
	buff := []byte("+02:00")
	cursor := 0
	l := len(buff)
	tmpTs, err := time.Parse("-07:00", string(buff))
	assert.NoError(t, err)

	obtained, err := parseNumericalTimeOffset(buff, &cursor, l)
	assert.NoError(t, err)

	expected := tmpTs.Location()
	assert.Equal(t, expected, obtained)
	assert.Equal(t, 6, cursor)
}

func TestParseTimeOffset_Valid(t *testing.T) {
	buff := []byte("Z")
	cursor := 0
	l := len(buff)

	obtained, err := parseTimeOffset(buff, &cursor, l)
	assert.NoError(t, err)
	assert.Equal(t, time.UTC, obtained)
	assert.Equal(t, 1, cursor)
}

func TestGetHourMin_Valid(t *testing.T) {
	buff := []byte("12:34")
	cursor := 0
	l := len(buff)

	expectedHour := 12
	expectedMinute := 34

	obtainedHour, obtainedMinute, err := getHourMinute(buff, &cursor, l)
	assert.NoError(t, err)
	assert.Equal(t, expectedHour, obtainedHour)
	assert.Equal(t, expectedMinute, obtainedMinute)
	assert.Equal(t, l, cursor)
}

func TestParsePartialTime_Valid(t *testing.T) {
	buff := []byte("05:14:15.000003")
	cursor := 0
	l := len(buff)

	obtained, err := parsePartialTime(buff, &cursor, l)
	expected := partialTime{
		hour:    5,
		minute:  14,
		seconds: 15,
		secFrac: 0.000003,
	}

	assert.NoError(t, err)
	assert.Equal(t, expected, obtained)
	assert.Equal(t, l, cursor)
}

func TestParseFullTime_Valid(t *testing.T) {
	tz := "-02:00"
	buff := []byte("05:14:15.000003" + tz)
	cursor := 0
	l := len(buff)

	tmpTs, err := time.Parse("-07:00", string(tz))
	assert.NoError(t, err)

	obtainedFt, err := parseFullTime(buff, &cursor, l)
	expectedFt := fullTime{
		pt: partialTime{
			hour:    5,
			minute:  14,
			seconds: 15,
			secFrac: 0.000003,
		},
		loc: tmpTs.Location(),
	}

	assert.NoError(t, err)
	assert.Equal(t, expectedFt, obtainedFt)
	assert.Equal(t, 21, cursor)
}

func TestToNSec(t *testing.T) {
	fixtures := []float64{
		0.52,
		0.003,
		0.000003,
	}

	expected := []int{
		520000000,
		3000000,
		3000,
	}

	assert.Equal(t, len(expected), len(fixtures))
	for i, f := range fixtures {
		obtained, err := toNSec(f)
		assert.NoError(t, err)
		assert.Equal(t, expected[i], obtained)
	}
}

func TestParseAppName_Valid(t *testing.T) {
	buff := []byte("su ")
	appName := "su"

	assertParseAppName(t, appName, buff, 2, nil)
}

func TestParseAppName_TooLong(t *testing.T) {
	// > 48chars
	buff := []byte("suuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuuu ")
	appName := ""

	assertParseAppName(t, appName, buff, 48, ErrInvalidAppName)
}

func TestParseProcId_Valid(t *testing.T) {
	buff := []byte("123foo ")
	procId := "123foo"

	assertParseProcId(t, procId, buff, 6, nil)
}

func TestParseProcId_TooLong(t *testing.T) {
	// > 128chars
	buff := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab ")
	procId := ""

	assertParseProcId(t, procId, buff, 128, ErrInvalidProcId)
}

func TestParseMsgId_Valid(t *testing.T) {
	buff := []byte("123foo ")
	procId := "123foo"

	assertParseMsgId(t, procId, buff, 6, nil)
}

func TestParseMsgId_TooLong(t *testing.T) {
	// > 32chars
	buff := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa ")
	procId := ""

	assertParseMsgId(t, procId, buff, 32, ErrInvalidMsgId)
}

func TestParseStructuredData_NilValue(t *testing.T) {
	// > 32chars
	buff := []byte("-")
	sdData := "-"

	assertParseSdName(t, sdData, buff, 1, nil)
}

func TestParseStructuredData_SingleStructuredData(t *testing.T) {
	sdData := `[exampleSDID@32473 iut="3" eventSource="Application"eventID="1011"]`
	buff := []byte(sdData)

	assertParseSdName(t, sdData, buff, len(buff), nil)
}

func TestParseStructuredData_MultipleStructuredData(t *testing.T) {
	sdData := `[exampleSDID@32473 iut="3" eventSource="Application"eventID="1011"][examplePriority@32473 class="high"]`
	buff := []byte(sdData)

	assertParseSdName(t, sdData, buff, len(buff), nil)
}

func TestParseStructuredData_MultipleStructuredDataInvalid(t *testing.T) {
	a := `[exampleSDID@32473 iut="3" eventSource="Application"eventID="1011"]`
	sdData := a + ` [examplePriority@32473 class="high"]`
	buff := []byte(sdData)

	assertParseSdName(t, a, buff, len(a), nil)
}

// -------------

func BenchmarkParseTimestamp(b *testing.B) {
	buff := []byte("2003-08-24T05:14:15.000003-07:00")

	p := NewParser(buff)

	for i := 0; i < b.N; i++ {
		_, err := p.parseTimestamp()
		if err != nil {
			panic(err)
		}

		p.cursor = 0
	}
}

func BenchmarkParseHeader(b *testing.B) {
	buff := []byte("<165>1 2003-10-11T22:14:15.003Z mymachine.example.com su 123 ID47")

	p := NewParser(buff)

	for i := 0; i < b.N; i++ {
		_, err := p.parseHeader()
		if err != nil {
			panic(err)
		}

		p.cursor = 0
	}
}

// -------------

func assertTimestamp(t *testing.T, ts time.Time, b []byte, expC int, e error) {
	p := NewParser(b)
	obtained, err := p.parseTimestamp()
	assert.Equal(t, e, err)

	tFmt := time.RFC3339Nano
	assert.Equal(t, ts.Format(tFmt), obtained.Format(tFmt))

	assert.Equal(t, expC, p.cursor)
}

func assertFindNextSpace(t *testing.T, nextSpace int, b []byte, e error) {
	obtained, err := syslogparser.FindNextSpace(b, 0, len(b))
	assert.Equal(t, e, err)
	assert.Equal(t, nextSpace, obtained)
}

func assertParseYear(t *testing.T, year int, b []byte, expC int, e error) {
	cursor := 0
	obtained, err := parseYear(b, &cursor, len(b))
	assert.Equal(t, e, err)
	assert.Equal(t, year, obtained)
	assert.Equal(t, expC, cursor)
}

func assertParseMonth(t *testing.T, month int, b []byte, expC int, e error) {
	cursor := 0
	obtained, err := parseMonth(b, &cursor, len(b))
	assert.Equal(t, e, err)
	assert.Equal(t, month, obtained)
	assert.Equal(t, expC, cursor)
}

func assertParseDay(t *testing.T, day int, b []byte, expC int, e error) {
	cursor := 0
	obtained, err := parseDay(b, &cursor, len(b))
	assert.Equal(t, e, err)
	assert.Equal(t, day, obtained)
	assert.Equal(t, expC, cursor)
}

func assertParseFullDate(t *testing.T, fd fullDate, b []byte, expC int, e error) {
	cursor := 0
	obtained, err := parseFullDate(b, &cursor, len(b))
	assert.Equal(t, e, err)
	assert.Equal(t, fd, obtained)
	assert.Equal(t, expC, cursor)
}

func assertParseHour(t *testing.T, hour int, b []byte, expC int, e error) {
	cursor := 0
	obtained, err := parseHour(b, &cursor, len(b))
	assert.Equal(t, e, err)
	assert.Equal(t, hour, obtained)
	assert.Equal(t, expC, cursor)
}

func assertParseMinute(t *testing.T, minute int, b []byte, expC int, e error) {
	cursor := 0
	obtained, err := parseMinute(b, &cursor, len(b))
	assert.Equal(t, e, err)
	assert.Equal(t, minute, obtained)
	assert.Equal(t, expC, cursor)
}

func assertParseSecond(t *testing.T, second int, b []byte, expC int, e error) {
	cursor := 0
	obtained, err := parseSecond(b, &cursor, len(b))
	assert.Equal(t, e, err)
	assert.Equal(t, second, obtained)
	assert.Equal(t, expC, cursor)
}

func assertParseSecFrac(t *testing.T, secFrac float64, b []byte, expC int, e error) {
	cursor := 0
	obtained, err := parseSecFrac(b, &cursor, len(b))
	assert.Equal(t, e, err)
	assert.Equal(t, secFrac, obtained)
	assert.Equal(t, expC, cursor)
}

func assertParseAppName(t *testing.T, appName string, b []byte, expC int, e error) {
	p := NewParser(b)
	obtained, err := p.parseAppName()
	assert.Equal(t, e, err)
	assert.Equal(t, appName, obtained)
	assert.Equal(t, expC, p.cursor)
}

func assertParseProcId(t *testing.T, procId string, b []byte, expC int, e error) {
	p := NewParser(b)
	obtained, err := p.parseProcId()

	assert.Equal(t, e, err)
	assert.Equal(t, procId, obtained)
	assert.Equal(t, expC, p.cursor)
}

func assertParseMsgId(t *testing.T, msgId string, b []byte, expC int, e error) {
	p := NewParser(b)
	obtained, err := p.parseMsgId()

	assert.Equal(t, e, err)
	assert.Equal(t, msgId, obtained)
	assert.Equal(t, expC, p.cursor)
}

func assertParseSdName(t *testing.T, sdData string, b []byte, expC int, e error) {
	cursor := 0
	obtained, err := parseStructuredData(b, &cursor, len(b))

	assert.Equal(t, e, err)
	assert.Equal(t, sdData, obtained)
	assert.Equal(t, expC, cursor)
}
