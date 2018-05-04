package mgr

import (
	"fmt"
	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/plugin"
	"github.com/qiniu/logkit/sender"
	"sync"
	"testing"
)

func TestNewPluginRunner(t *testing.T) {
	var wg sync.WaitGroup
	wg.Add(1)
	plugin.Conf = &plugin.Config{
		Enabled: true,
		Dir:     `E:\Go\GOPATH\src\github.com\qiniu\logkit\plugins`,
		RemoteSource:     "",
	}
	err := plugin.SyncPlugins()
	if err != nil {
		fmt.Println(err)
	}
	sr := &sender.SenderRegistry{}

	rc1 := RunnerConfig{
		PluginConfig: &PluginConfig{
			PluginType: "hello1",
			Config: map[string]interface{}{
				"name": "admin",
				"pass": "123",
			},
		},
		RunnerInfo: RunnerInfo{
			RunnerName:      "hello1",
			CollectInterval: 1,
			MaxBatchLen:     10,
		},
		Transforms:   []map[string]interface{}{},
		SenderConfig: []conf.MapConf{},
	}

	pRunner1, err1 := NewPluginRunner(rc1, sr)
	if err1 != nil {
		fmt.Println(err1)
	}
	go pRunner1.Run()

	rc2 := RunnerConfig{
		PluginConfig: &PluginConfig{
			PluginType: "hello2",
			Config: map[string]interface{}{
				"name": "admin",
				"pass": "123",
			},
		},
		RunnerInfo: RunnerInfo{
			RunnerName:      "hello2",
			CollectInterval: 1,
			MaxBatchLen:     10,
		},
		Transforms:   []map[string]interface{}{},
		SenderConfig: []conf.MapConf{},
	}
	pRunner2, err2 := NewPluginRunner(rc2, sr)
	if err2 != nil {
		fmt.Println(err2)
	}
	go pRunner2.Run()

	rc3 := RunnerConfig{
		PluginConfig: &PluginConfig{
			PluginType: "hello3",
			Config: map[string]interface{}{
				"name": "admin",
				"pass": "123",
			},
		},
		RunnerInfo: RunnerInfo{
			RunnerName:      "hello3",
			CollectInterval: 1,
			MaxBatchLen:     10,
		},
		Transforms:   []map[string]interface{}{},
		SenderConfig: []conf.MapConf{},
	}
	pRunner3, err3 := NewPluginRunner(rc3, sr)
	if err3 != nil {
		fmt.Println(err3)
	}
	go pRunner3.Run()

	rc4 := RunnerConfig{
		PluginConfig: &PluginConfig{
			PluginType: "hello4",
			Config: map[string]interface{}{
				"name": "admin",
				"pass": "123",
			},
		},
		RunnerInfo: RunnerInfo{
			RunnerName:      "hello4",
			CollectInterval: 1,
			MaxBatchLen:     10,
		},
		Transforms:   []map[string]interface{}{},
		SenderConfig: []conf.MapConf{},
	}
	pRunner4, err4 := NewPluginRunner(rc4, sr)
	if err4 != nil {
		fmt.Println(err4)
	}
	go pRunner4.Run()

	rc5 := RunnerConfig{
		PluginConfig: &PluginConfig{
			PluginType: "hello5",
			Config: map[string]interface{}{
				"name": "admin",
				"pass": "123",
			},
		},
		RunnerInfo: RunnerInfo{
			RunnerName:      "hello5",
			CollectInterval: 1,
			MaxBatchLen:     10,
		},
		Transforms:   []map[string]interface{}{},
		SenderConfig: []conf.MapConf{},
	}
	pRunner5, err5 := NewPluginRunner(rc5, sr)
	if err5 != nil {
		fmt.Println(err5)
	}
	go pRunner5.Run()

	rc6 := RunnerConfig{
		PluginConfig: &PluginConfig{
			PluginType: "hello6",
			Config: map[string]interface{}{
				"name": "admin",
				"pass": "123",
			},
		},
		RunnerInfo: RunnerInfo{
			RunnerName:      "hello6",
			CollectInterval: 1,
			MaxBatchLen:     10,
		},
		Transforms:   []map[string]interface{}{},
		SenderConfig: []conf.MapConf{},
	}
	pRunner6, err6 := NewPluginRunner(rc6, sr)
	if err6 != nil {
		fmt.Println(err6)
	}
	go pRunner6.Run()
	wg.Wait()

}
