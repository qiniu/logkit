package parser

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/conf"
	. "github.com/qiniu/logkit/parser/config"
	. "github.com/qiniu/logkit/utils/models"
)

func TestParseResultSlice(t *testing.T) {
	parseResultSlice := ParseResultSlice{}
	assert.EqualValues(t, 0, parseResultSlice.Len())

	parseResultSlice = ParseResultSlice{
		{
			Line:  "aaa",
			Index: 0,
		},
		{
			Line:  "aaa",
			Index: 1,
		},
		{
			Line:  "aaa",
			Index: 2,
		},
		{
			Line:  "aaa",
			Index: 1,
		},
	}
	assert.EqualValues(t, true, parseResultSlice.Less(0, 1))
	assert.EqualValues(t, false, parseResultSlice.Less(1, 3))
	parseResultSlice.Swap(1, 2)
	assert.EqualValues(t, ParseResult{
		Line:  "aaa",
		Index: 2,
	}, parseResultSlice[1])
}

func TestRegisterConstructor(t *testing.T) {
	RegisterConstructor("test", func(conf conf.MapConf) (Parser, error) { return nil, nil })
	assert.EqualValues(t, 1, len(registeredConstructors))
}

func TestRegistry(t *testing.T) {
	c := func(conf conf.MapConf) (Parser, error) { return nil, nil }
	RegisterConstructor("test", c)
	ret := NewRegistry()
	assert.NotNil(t, ret)

	assert.NotNil(t, ret.RegisterParser("test", c))
	assert.Nil(t, ret.RegisterParser("mytest", c))

	_, err := ret.NewLogParser(nil)
	assert.NotNil(t, err)
	_, err = ret.NewLogParser(conf.MapConf{KeyParserType: "test"})
	assert.Nil(t, err)

	_, err = ret.NewLogParser(conf.MapConf{KeyParserType: "test_test"})
	assert.NotNil(t, err)
}

func TestParseLine(t *testing.T) {
	var (
		sendChan   = make(chan ParseInfo)
		resultChan = make(chan ParseResult)
		wg         = new(sync.WaitGroup)
	)
	wg.Add(1)
	go ParseLine(sendChan, resultChan, wg, true, func(line string) (Data, error) { return nil, nil })
	go func() {
		wg.Wait()
		close(resultChan)
	}()
	sendChan <- ParseInfo{Line: "  ", Index: 1}
	close(sendChan)
	for result := range resultChan {
		assert.EqualValues(t, ParseResult{Line: "", Index: 1}, result)
	}

	sendChan = make(chan ParseInfo)
	resultChan = make(chan ParseResult)
	wg.Add(1)
	go ParseLine(sendChan, resultChan, wg, true, func(line string) (Data, error) { return nil, nil })
	go func() {
		wg.Wait()
		close(resultChan)
	}()
	sendChan <- ParseInfo{Line: `{"a":"b"}`, Index: 2}
	close(sendChan)
	for result := range resultChan {
		assert.EqualValues(t, ParseResult{Line: `{"a":"b"}`, Index: 2}, result)
	}
}

func TestParseLineDataSlice(t *testing.T) {
	var (
		sendChan   = make(chan ParseInfo)
		resultChan = make(chan ParseResult)
		wg         = new(sync.WaitGroup)
	)
	wg.Add(1)
	go ParseLineDataSlice(sendChan, resultChan, wg, true, func(line string) ([]Data, error) { return nil, nil })
	go func() {
		wg.Wait()
		close(resultChan)
	}()
	sendChan <- ParseInfo{Line: "  ", Index: 1}
	close(sendChan)
	for result := range resultChan {
		assert.EqualValues(t, ParseResult{Line: "", Index: 1}, result)
	}

	sendChan = make(chan ParseInfo)
	resultChan = make(chan ParseResult)
	wg.Add(1)
	go ParseLineDataSlice(sendChan, resultChan, wg, true, func(line string) ([]Data, error) { return nil, nil })
	go func() {
		wg.Wait()
		close(resultChan)
	}()
	sendChan <- ParseInfo{Line: `{"a":"b"}`, Index: 2}
	close(sendChan)
	for result := range resultChan {
		assert.EqualValues(t, ParseResult{Line: `{"a":"b"}`, Index: 2}, result)
	}
}
