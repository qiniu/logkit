package ip

import (
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/transforms"
	. "github.com/qiniu/logkit/utils/models"
)

func TestTransformer(t *testing.T) {
	ipt := &Transformer{
		Key:      "ip",
		DataPath: "./test_data/17monipdb.dat",
	}
	data, err := ipt.Transform([]Data{{"ip": "111.2.3.4"}, {"ip": "x.x.x.x"}})
	assert.Error(t, err)
	exp := []Data{{
		"ip":      "111.2.3.4",
		"Region":  "浙江",
		"City":    "宁波",
		"Country": "中国",
		"Isp":     "N/A"},
		{
			"ip": "x.x.x.x",
		}}
	assert.Equal(t, exp, data)
	expe := StatsInfo{
		Errors:    1,
		Success:   1,
		LastError: "find total 1 erorrs in transform IP, last error info is invalid IP format",
	}
	assert.Equal(t, expe, ipt.stats)
	assert.Equal(t, ipt.Stage(), transforms.StageAfterParser)

	// 并发查询测试
	{
		var wg sync.WaitGroup
		for i := 1; i <= 100; i++ {
			wg.Add(1)
			go func() {
				info, err := ipt.loc.Find("111.2.3.4")
				assert.Nil(t, err)
				exp := &LocationInfo{
					Country:      "中国",
					Region:       "浙江",
					City:         "宁波",
					Isp:          "N/A",
					CountryCode:  "",
					Latitude:     "",
					Longitude:    "",
					DistrictCode: "",
				}
				assert.Equal(t, exp, info)
				wg.Done()
			}()
		}
		wg.Wait()
	}

	ipt2 := &Transformer{
		Key:      "multi.ip",
		DataPath: "./test_data/17monipdb.dat",
	}
	data2, err2 := ipt2.Transform([]Data{{"multi": map[string]interface{}{"ip": "111.2.3.4"}}, {"multi": map[string]interface{}{"ip": "x.x.x.x"}}})
	assert.Error(t, err2)
	exp2 := []Data{{
		"multi": map[string]interface{}{
			"ip":      "111.2.3.4",
			"Region":  "浙江",
			"City":    "宁波",
			"Country": "中国",
			"Isp":     "N/A"},
	},
		{"multi": map[string]interface{}{
			"ip": "x.x.x.x",
		},
		},
	}
	assert.Equal(t, exp2, data2)
	expe2 := StatsInfo{
		Errors:    1,
		Success:   1,
		LastError: "find total 1 erorrs in transform IP, last error info is invalid IP format",
	}
	assert.Equal(t, expe2, ipt.stats)
	assert.Equal(t, ipt2.Stage(), transforms.StageAfterParser)

	ipt3 := &Transformer{
		Key:      "multi.ip",
		DataPath: "./test_data/17monipdb.datx",
	}
	data3, err3 := ipt3.Transform([]Data{{"multi": map[string]interface{}{"ip": "111.2.3.4"}}, {"multi": map[string]interface{}{"ip": "x.x.x.x"}}})
	assert.Error(t, err3)
	exp3 := []Data{{
		"multi": map[string]interface{}{
			"ip":          "111.2.3.4",
			"Region":      "浙江",
			"City":        "宁波",
			"Country":     "中国",
			"Isp":         "N/A",
			"CountryCode": "N/A"},
	},
		{"multi": map[string]interface{}{
			"ip": "x.x.x.x",
		},
		},
	}
	assert.Equal(t, exp3, data3)
	expe3 := StatsInfo{
		Errors:    1,
		Success:   1,
		LastError: "find total 1 erorrs in transform IP, last error info is invalid IP format",
	}
	assert.Equal(t, expe3, ipt.stats)

	ipt4 := &Transformer{
		Key:      "multi.ip2",
		DataPath: "./test_data/17monipdb.dat",
	}

	multi_ip := []Data{{
		"multi": map[string]interface{}{
			"ip":      "111.2.3.4",
			"Region":  "浙江",
			"City":    "宁波",
			"Country": "中国",
			"Isp":     "N/A",
			"ip2":     "183.251.28.250",
		},
	},
		{"multi": map[string]interface{}{
			"ip": "x.x.x.x",
		},
		},
	}
	data4, err4 := ipt4.Transform(multi_ip)
	exp4 := []Data{
		{
			"multi": map[string]interface{}{
				"ip":          "111.2.3.4",
				"Region":      "浙江",
				"City":        "宁波",
				"Country":     "中国",
				"Isp":         "N/A",
				"ip2":         "183.251.28.250",
				"ip2_City":    "厦门",
				"ip2_Isp":     "N/A",
				"ip2_Region":  "福建",
				"ip2_Country": "中国",
			},
		},
		{
			"multi": map[string]interface{}{
				"ip": "x.x.x.x",
			},
		},
	}
	assert.Error(t, err4)
	assert.Equal(t, exp4, data4)
	expe4 := StatsInfo{
		Errors:    1,
		Success:   1,
		LastError: "find total 1 erorrs in transform IP, last error info is invalid IP format",
	}
	assert.Equal(t, expe4, ipt.stats)
	assert.Equal(t, ipt4.Stage(), transforms.StageAfterParser)

	// 确保多个 transformer 只有两个 Locator 产生
	assert.Len(t, locatorStore.locators, 2)
}

func Test_badData(t *testing.T) {
	ipt := &Transformer{
		Key:      "ip",
		DataPath: "./test_data/bad.dat",
	}
	_, err := ipt.Transform([]Data{{"ip": "111.2.3.4"}, {"ip": "x.x.x.x"}})
	assert.Error(t, err)
	ierr, ok := err.(ErrInvalidFile)
	assert.True(t, ok)
	assert.Equal(t, "dat", ierr.Format)

	ipt = &Transformer{
		Key:      "ip",
		DataPath: "./test_data/bad.datx",
	}
	_, err = ipt.Transform([]Data{{"ip": "111.2.3.4"}, {"ip": "x.x.x.x"}})
	assert.Error(t, err)
	ierr, ok = err.(ErrInvalidFile)
	assert.True(t, ok)
	assert.Equal(t, "datx", ierr.Format)

	ipt = &Transformer{
		Key:      "ip",
		DataPath: "./test_data/bad.datn",
	}
	_, err = ipt.Transform([]Data{{"ip": "111.2.3.4"}, {"ip": "x.x.x.x"}})
	assert.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "unrecognized data file format"))
}
