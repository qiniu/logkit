package date

import (
	"fmt"
	"testing"
	"time"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"

	"github.com/stretchr/testify/assert"
)

func TestDateTransformer(t *testing.T) {
	var k string = "k"
	nowstr := time.Now().Format("2006/01/02")
	tm := time.Unix(1506049632, 0)
	str3 := tm.Format("2006/01/02/03/04/05")
	tests := []struct {
		Key          string
		Offset       int
		LayoutBefore string
		LayoutAfter  string
		data         Data
		exp          Data
	}{
		{
			Offset:       0,
			LayoutBefore: "",
			LayoutAfter:  "",
			Key:          k,
			data:         Data{k: "2017/03/28 15:41:53"},
			exp:          Data{k: "2017-03-28T15:41:53Z"},
		},
		{
			Offset:       0,
			LayoutBefore: "",
			Key:          k,
			LayoutAfter:  "2006/01/02",
			data:         Data{k: nowstr},
			exp:          Data{k: nowstr},
		},
		{
			Offset:       0,
			LayoutBefore: "",
			LayoutAfter:  "",
			Key:          k,
			data:         Data{k: 1506049632},
			exp:          Data{k: time.Unix(1506049632, 0).Format(time.RFC3339Nano)},
		},
		{
			Offset:       0,
			LayoutBefore: "",
			LayoutAfter:  "2006/01/02/03/04/05",
			Key:          k,
			data:         Data{k: 1506049632},
			exp:          Data{k: str3},
		},
		{
			Offset:       0,
			LayoutBefore: "2006/01/02/03/04/05",
			LayoutAfter:  "",
			Key:          k,
			data:         Data{k: "2017/09/22/11/07/12"},
			exp:          Data{k: "2017-09-22T11:07:12Z"},
		},
		{
			Offset:       0,
			LayoutBefore: "【2006/01/02/03/04/05】",
			LayoutAfter:  "",
			Key:          k,
			data:         Data{k: "【2017/09/22/11/07/12】"},
			exp:          Data{k: "2017-09-22T11:07:12Z"},
		},
		{
			Offset:       0,
			LayoutBefore: "",
			LayoutAfter:  "",
			Key:          "a.b",
			data:         Data{"a": map[string]interface{}{"b": "2017/03/28 15:41:53"}},
			exp:          Data{"a": map[string]interface{}{"b": "2017-03-28T15:41:53Z"}},
		},
	}
	for idx, ti := range tests {
		tis := &DateTrans{
			Key:          ti.Key,
			Offset:       ti.Offset,
			LayoutBefore: ti.LayoutBefore,
			LayoutAfter:  ti.LayoutAfter,
		}
		data, err := tis.Transform([]Data{ti.data})
		assert.NoError(t, err)
		exp := []Data{ti.exp}
		assert.Equal(t, exp, data, fmt.Sprintf("idx %v", idx))
		assert.Equal(t, transforms.StageAfterParser, tis.Stage())
	}

}
