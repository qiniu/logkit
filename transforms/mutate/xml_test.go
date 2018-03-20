package mutate

import (
	"testing"

	. "github.com/qiniu/logkit/utils/models"
	"github.com/stretchr/testify/assert"
)

func TestXmlTransformer(t *testing.T) {

	xtr := Xml{
		Key: "xml",
		New: "xml",
	}
	data := []Data{{"key1": "value1", "xml": `<?xml version="1.0" encoding="UTF-8"?>
<note>
  <to>Tove</to>
  <from>Jani</from>
  <heading>Reminder</heading>
  <body>Don't forget me this weekend!</body>
</note>`}}

	newdata, err := xtr.Transform(data)
	assert.NoError(t, err)
	expdata := []Data{
		{
			"key1": "value1",
			"xml": map[string]interface{}{
				"note": map[string]interface{}{
					"heading": "Reminder",
					"body":    "Don't forget me this weekend!",
					"to":      "Tove",
					"from":    "Jani",
				},
			},
		},
	}
	assert.Equal(t, expdata, newdata)
}
