package json

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/json-iterator/go"
	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/parser"
	"github.com/qiniu/logkit/utils"
	. "github.com/qiniu/logkit/utils/models"
)

var (
	bench    []Data
	testData = utils.GetParseTestData(`{"a":1,"b":[1.0,2.0,3.0],"c":{"d":"123","g":1.2},"e":"x","mm":1.23,"jjj":1493797500346428926}`, DefaultMaxBatchSize)
)

// old: 20	  86082792 ns/op	routine = 1  (2MB)
// now: 10	 135893207 ns/op	routine = 1  (2MB)
// now: 20	  88439912 ns/op	routine = 2  (2MB)
func Benchmark_JsonParse(b *testing.B) {
	c := conf.MapConf{}
	c[parser.KeyParserName] = "testjsonparser"
	c[parser.KeyParserType] = "json"
	c[parser.KeyLabels] = "mm abc"
	c[parser.KeyDisableRecordErrData] = "true"
	p, _ := NewParser(c)

	var m []Data
	for n := 0; n < b.N; n++ {
		m, _ = p.Parse(testData)
	}
	bench = m
}

func TestJsonParser(t *testing.T) {
	c := conf.MapConf{}
	c[parser.KeyParserName] = "testjsonparser"
	c[parser.KeyParserType] = "json"
	c[parser.KeyLabels] = "mm abc"
	c[parser.KeyDisableRecordErrData] = "true"
	p, _ := NewParser(c)
	tests := []struct {
		in  []string
		exp []Data
	}{
		{
			in: []string{`{
							"a":1,"b":[1.0,2.0,3.0],
							"c":{"d":"123","g":1.2},
							"e":"x","f":1.23
						}`, ""},
			exp: []Data{{
				"a": json.Number("1"),
				"b": []interface{}{json.Number("1.0"), json.Number("2.0"), json.Number("3.0")},
				"c": map[string]interface{}{
					"d": "123",
					"g": json.Number("1.2"),
				},
				"e":  "x",
				"f":  json.Number("1.23"),
				"mm": "abc",
			}},
		},
		{
			in: []string{`{"a":1,"b":[1.0,2.0,3.0],"c":{"d":"123","g":1.2},"e":"x","mm":1.23,"jjj":1493797500346428926}`},
			exp: []Data{Data{
				"a": json.Number("1"),
				"b": []interface{}{json.Number("1.0"), json.Number("2.0"), json.Number("3.0")},
				"c": map[string]interface{}{
					"d": "123",
					"g": json.Number("1.2"),
				},
				"e":   "x",
				"mm":  json.Number("1.23"),
				"jjj": json.Number("1493797500346428926"),
			}},
		},
	}

	m, err := p.Parse(tests[0].in)
	if err != nil {
		errx, _ := err.(*StatsError)
		assert.Equal(t, int64(0), errx.StatsInfo.Errors)
	}
	if len(m) != 1 {
		t.Fatalf("parse lines error, expect 1 line but got %v lines", len(m))
	}
	assert.EqualValues(t, tests[0].exp, m)

	m, err = p.Parse(tests[1].in)
	if err != nil {
		errx, _ := err.(*StatsError)
		if errx.ErrorDetail != nil {
			t.Error(errx.ErrorDetail)
		}
	}
	assert.EqualValues(t, tests[1].exp, m)

	assert.EqualValues(t, "testjsonparser", p.Name())
}

func TestJsonParserForErrData(t *testing.T) {
	c := conf.MapConf{}
	c[parser.KeyParserName] = "testjsonparser"
	c[parser.KeyParserType] = "json"
	c[parser.KeyLabels] = "mm abc"
	c[parser.KeyDisableRecordErrData] = "false"
	p, _ := NewParser(c)
	testIn := []string{`{"a":1,"b":[1.0,2.0,3.0],"c":{"d":"123","g":1.2},"e":"x","f":1.23}`, ""}
	testExp := Data{
		"a": json.Number("1"),
		"b": []interface{}{json.Number("1.0"), json.Number("2.0"), json.Number("3.0")},
		"c": map[string]interface{}{
			"d": "123",
			"g": json.Number("1.2"),
		},
		"e":  "x",
		"f":  json.Number("1.23"),
		"mm": "abc",
	}

	m, err := p.Parse(testIn)
	if err != nil {
		errx, _ := err.(*StatsError)
		assert.Equal(t, int64(0), errx.StatsInfo.Errors)
	}
	if len(m) != 1 {
		t.Fatalf("parse lines error, expect 1 lines but got %v lines", len(m))
	}
	assert.EqualValues(t, testExp, m[0])

	assert.EqualValues(t, "testjsonparser", p.Name())
}

var testjsonline = `{"a":1,"b":[1.0,2.0,3.0],"c":{"d":"123","g":1.2},"e":"x","mm":1.23,"jjj":1493797500346428926}`
var testmiddleline = `{
  "person": {
    "id": "d50887ca-a6ce-4e59-b89f-14f0b5d03b03",
    "name": {
      "fullName": "Leonid Bugaev",
      "givenName": "Leonid",
      "familyName": "Bugaev"
    },
    "email": "leonsbox@gmail.com",
    "gender": "male",
    "location": "Saint Petersburg, Saint Petersburg, RU",
    "geo": {
      "city": "Saint Petersburg",
      "state": "Saint Petersburg",
      "country": "Russia",
      "lat": 59.9342802,
      "lng": 30.3350986
    },
    "bio": "Senior engineer at Granify.com",
    "site": "http://flickfaver.com",
    "avatar": "https://d1ts43dypk8bqh.cloudfront.net/v1/avatars/d50887ca-a6ce-4e59-b89f-14f0b5d03b03",
    "employment": {
      "name": "www.latera.ru",
      "title": "Software Engineer",
      "domain": "gmail.com"
    },
    "facebook": {
      "handle": "leonid.bugaev"
    },
    "github": {
      "handle": "buger",
      "id": 14009,
      "avatar": "https://avatars.githubusercontent.com/u/14009?v=3",
      "company": "Granify",
      "blog": "http://leonsbox.com",
      "followers": 95,
      "following": 10
    },
    "twitter": {
      "handle": "flickfaver",
      "id": 77004410,
      "bio": null,
      "followers": 2,
      "following": 1,
      "statuses": 5,
      "favorites": 0,
      "location": "",
      "site": "http://flickfaver.com",
      "avatar": null
    },
    "linkedin": {
      "handle": "in/leonidbugaev"
    },
    "googleplus": {
      "handle": null
    },
    "angellist": {
      "handle": "leonid-bugaev",
      "id": 61541,
      "bio": "Senior engineer at Granify.com",
      "blog": "http://buger.github.com",
      "site": "http://buger.github.com",
      "followers": 41,
      "avatar": "https://d1qb2nb5cznatu.cloudfront.net/users/61541-medium_jpg?1405474390"
    },
    "klout": {
      "handle": null,
      "score": null
    },
    "foursquare": {
      "handle": null
    },
    "aboutme": {
      "handle": "leonid.bugaev",
      "bio": null,
      "avatar": null
    },
    "gravatar": {
      "handle": "buger",
      "urls": [
      ],
      "avatar": "http://1.gravatar.com/avatar/f7c8edd577d13b8930d5522f28123510",
      "avatars": [
        {
          "url": "http://1.gravatar.com/avatar/f7c8edd577d13b8930d5522f28123510",
          "type": "thumbnail"
        }
      ]
    },
    "fuzzy": false
  },
  "company": null
}`

//BenchmarkJsoninterParser-4                       	  300000	      5144 ns/op
func BenchmarkJsoninterParser(b *testing.B) {
	jsonnumber := jsoniter.Config{
		EscapeHTML: true,
		UseNumber:  true,
	}.Froze()
	for i := 0; i < b.N; i++ {
		data := Data{}
		if err := jsonnumber.Unmarshal([]byte(testjsonline), &data); err != nil {
			b.Error(err)
		}
	}
}

//BenchmarkJsonParser-4                    	  200000	      7767 ns/op
func BenchmarkJsonParser(b *testing.B) {
	for i := 0; i < b.N; i++ {
		data := Data{}
		decoder := json.NewDecoder(bytes.NewReader([]byte(testjsonline)))
		decoder.UseNumber()
		if err := decoder.Decode(&data); err != nil {
			b.Error(err)
		}
	}
}

//BenchmarkJsonMiddlelineParser-4                  	   30000	     58441 ns/op
func BenchmarkJsonMiddlelineParser(b *testing.B) {
	for i := 0; i < b.N; i++ {
		data := Data{}
		decoder := json.NewDecoder(bytes.NewReader([]byte(testmiddleline)))
		decoder.UseNumber()
		if err := decoder.Decode(&data); err != nil {
			b.Error(err)
		}
	}
}

//BenchmarkJsoniterMiddlelineWithDecoderParser-4   	   30000	     41496 ns/op
func BenchmarkJsoniterMiddlelineWithDecoderParser(b *testing.B) {
	for i := 0; i < b.N; i++ {
		data := Data{}
		decoder := jsoniter.NewDecoder(bytes.NewReader([]byte(testmiddleline)))
		decoder.UseNumber()
		if err := decoder.Decode(&data); err != nil {
			b.Error(err)
		}
	}
}

//BenchmarkMiddlelineWithConfigParser-4            	   50000	     35298 ns/op
func BenchmarkMiddlelineWithConfigParser(b *testing.B) {
	jsonnumber := jsoniter.Config{
		EscapeHTML: true,
		UseNumber:  true,
	}.Froze()
	for i := 0; i < b.N; i++ {
		data := Data{}
		if err := jsonnumber.Unmarshal([]byte(testmiddleline), &data); err != nil {
			b.Error(err)
		}
	}
}

func TestParseMutiLineJson(t *testing.T) {
	c := conf.MapConf{}
	c[parser.KeyParserName] = "TestParseMutiLineJson"
	c[parser.KeyParserType] = "json"
	c[parser.KeyDisableRecordErrData] = "false"
	p, _ := NewParser(c)
	data := `[{"name":"ethancai", "fansCount": 9223372036854775807}]`
	res, err := p.Parse([]string{data})
	errx, _ := err.(*StatsError)
	err = errx.ErrorDetail
	assert.NoError(t, err)

	exp := []Data{{"name": "ethancai", "fansCount": json.Number("9223372036854775807")}}
	assert.Equal(t, exp, res)
}

func TestParseSpaceJson(t *testing.T) {
	c := conf.MapConf{}
	c[parser.KeyParserName] = "TestParseSpaceJson"
	c[parser.KeyParserType] = "json"
	c[parser.KeyDisableRecordErrData] = "false"
	p, _ := NewParser(c)
	data := "\n"
	res, err := p.Parse([]string{data})
	errx, _ := err.(*StatsError)
	err = errx.ErrorDetail
	assert.NoError(t, err)

	exp := []Data{}
	assert.Equal(t, exp, res)
}
