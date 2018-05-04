package mutate

import (
	. "github.com/qiniu/logkit/utils/models"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestScriptTransformer(t *testing.T) {
	g1 := &Script{
		//OldKey: "key",
		//NewKey: "key",
		Script: `
		var i = 0;
		while (true) {
        	// Loop forever
    	}`,
	}
	g1.Init()
	datas1, err1 := g1.Transform(getTestData())
	assert.Error(t, err1)
	assert.Equal(t, getTestData(), datas1)

	g2 := &Script{
		OldKey: "logLevel",
		NewKey: "logLevel",
		Script: `
			switch (logLevel) {
			case "i" :
				logLevel = "info";
				break;
			case "d" :
				logLevel = "debug";
				break;
			case "w" :
				logLevel = "warn";
				break;
			case "e" :
				logLevel = "error";
				break;
    		}
		`,
	}
	g2.Init()
	datas2, err2 := g2.Transform(getTestData())
	assert.NoError(t, err2)
	assert.Equal(t, []Data{
		map[string]interface{}{
			"logLevel": "warn",
			"method":   "POST",
		},
		map[string]interface{}{
			"logLevel": "info",
			"method":   "GET",
		}}, datas2)

	g3 := &Script{
		OldKey: "logLevel:l",
		NewKey: "logLevel:l",
		Script: `
			switch (l) {
			case "i" :
				l = "info";
				break;
			case "d" :
				l = "debug";
				break;
			case "w" :
				l = "warn";
				break;
			case "e" :
				l = "error";
				break;
    		}
		`,
	}
	g3.Init()
	datas3, err3 := g3.Transform(getTestData())
	assert.NoError(t, err3)
	assert.Equal(t, []Data{
		map[string]interface{}{
			"logLevel": "warn",
			"method":   "POST",
		},
		map[string]interface{}{
			"logLevel": "info",
			"method":   "GET",
		}}, datas3)

	g4 := &Script{
		OldKey: "logLevel:l",
		NewKey: "logLevel:l, method:m",
		Script: `
			switch (l) {
			case "i" :
				l = "info";
				break;
			case "d" :
				l = "debug";
				break;
			case "w" :
				l = "warn";
				break;
			case "e" :
				l = "error";
				break;
    		}
		m = "delete";
		`,
	}
	g4.Init()
	datas4, err4 := g4.Transform(getTestData())
	assert.NoError(t, err4)
	assert.Equal(t, []Data{
		map[string]interface{}{
			"logLevel": "warn",
			"method":   "delete",
		},
		map[string]interface{}{
			"logLevel": "info",
			"method":   "delete",
		}}, datas4)

	g5 := &Script{
		OldKey:    "logLevel",
		NewKey:    "",
		Script:    ``,
		DeleteOld: true,
	}
	g5.Init()
	datas5, err5 := g5.Transform(getTestData())
	assert.NoError(t, err5)
	assert.Equal(t, datas5, []Data{
		map[string]interface{}{
			"method": "POST",
		},
		map[string]interface{}{
			"method": "GET",
		},
	})

	g6 := &Script{
		OldKey:    "logLevel",
		NewKey:    "logLevel",
		Script:    ``,
		DeleteOld: true,
	}
	g6.Init()
	datas6, err6 := g6.Transform(getTestData())
	assert.NoError(t, err6)
	assert.Equal(t, datas6, getTestData())

}

func getTestData() []Data {
	testData := []Data{
		map[string]interface{}{
			"logLevel": "w",
			"method":   "POST",
		},
		map[string]interface{}{
			"logLevel": "i",
			"method":   "GET",
		},
	}
	return testData
}
