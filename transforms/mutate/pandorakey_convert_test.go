package mutate

import (
	"testing"

	. "github.com/qiniu/logkit/utils/models"
	"github.com/stretchr/testify/assert"
)

func TestDeepconvertkey(t *testing.T) {

	pandoraConvert := &PandoraKeyConvert{}

	data := []Data{{"ts。ts2": "stamp1"}, {"ts-tes2/1.2": "stamp2"}}
	got, _ := pandoraConvert.Transform(data)
	exp := []Data{{"ts_ts2": "stamp1"}, {"ts_tes2_1_2": "stamp2"}}
	assert.Equal(t, exp, got)

	data = []Data{{"ts。ts2": map[string]interface{}{"_xs1_2s.xs.1": 1, "a.xs.1": 2}}, {"ts- ": "stamp2"}}
	got, _ = pandoraConvert.Transform(data)
	exp = []Data{{"ts_ts2": map[string]interface{}{"xs1_2s_xs_1": 1, "a_xs_1": 2}}, {"ts__": "stamp2"}}
	assert.Equal(t, exp, got)

	data = []Data{{"ts。ts2": map[string]interface{}{"200": 1, "a.xs.1": 2}}}
	got, _ = pandoraConvert.Transform(data)
	exp = []Data{{"ts_ts2": map[string]interface{}{"K200": 1, "a_xs_1": 2}}}
	assert.Equal(t, exp, got)
}

var got []Data

//old(没有cache):500000	      2846 ns/op	    2536 B/op	      33 allocs/op
//new(cache): 500000	      2249 ns/op	    2392 B/op	      17 allocs/op
func BenchmarkCache(b *testing.B) {
	pandoraConvert := &PandoraKeyConvert{cache: make(map[string]KeyInfo)}
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		data := []Data{{"ts。ts2": "stamp1"}, {"ts-tes2/1.2": "stamp2"}}
		got, _ = pandoraConvert.Transform(data)

		data = []Data{{"ts。ts2": map[string]interface{}{"_xs1_2s.xs.1": 1, "a.xs.1": 2}}, {"ts- ": "stamp2"}}
		got, _ = pandoraConvert.Transform(data)

		data = []Data{{"ts。ts2": map[string]interface{}{"200": 1, "a.xs.1": 2}}}
		got, _ = pandoraConvert.Transform(data)
	}
}
