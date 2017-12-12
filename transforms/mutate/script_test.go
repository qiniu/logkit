package mutate

import (
	"github.com/qiniu/logkit/sender"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestScriptTransformer(t *testing.T) {
	g1 := &Script{
		OldKey: "key",
		NewKey: "key",
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
	assert.Equal(t, []sender.Data{
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
				l = "error"
				break;
    		}
		`,
	}
	g3.Init()
	datas3, err3 := g3.Transform(getTestData())
	assert.NoError(t, err3)
	assert.Equal(t, []sender.Data{
		map[string]interface{}{
			"logLevel": "warn",
			"method":   "POST",
		},
		map[string]interface{}{
			"logLevel": "info",
			"method":   "GET",
		}}, datas3)
}

func getTestData() []sender.Data {
	testData := []sender.Data{
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
