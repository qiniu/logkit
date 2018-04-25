package main

import (
	"os"
	"runtime"

	config "github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/mgr"
	"github.com/qiniu/logkit/parser"
	"github.com/qiniu/logkit/samples"
	"github.com/qiniu/logkit/sender"
	utilsos "github.com/qiniu/logkit/utils/os"

	_ "net/http/pprof"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/reader"
)

type Config struct {
	MaxProcs   int      `json:"max_procs"`
	DebugLevel int      `json:"debug_level"`
	ConfsPath  []string `json:"confs_path"`
	mgr.ManagerConfig
}

var conf Config

func main() {
	config.Init("f", "qbox", "qboxlogexporter.conf")
	if err := config.Load(&conf); err != nil {
		log.Fatal("config.Load failed:", err)
	}
	log.Printf("Config: %#v", conf)

	if conf.MaxProcs == 0 {
		conf.MaxProcs = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(conf.MaxProcs)
	log.SetOutputLevel(conf.DebugLevel)

	pregistry := parser.NewParserRegistry()
	// 注册你自定义的parser
	pregistry.RegisterParser("myparser", samples.NewMyParser)

	sregistry := sender.NewSenderRegistry()
	sregistry.RegisterSender("mysender", samples.NewMySender)

	rr := reader.NewReaderRegistry()

	m, err := mgr.NewCustomManager(conf.ManagerConfig, rr, pregistry, sregistry)
	if err != nil {
		log.Fatalf("NewManager: %v", err)
	}
	var paths []string
	for _, v := range conf.ConfsPath {
		_, err = os.Stat(v)
		if os.IsNotExist(err) {
			err = os.MkdirAll(v, os.ModePerm)
		}
		if err != nil {
			log.Fatalf("Cannot read or create ConfsPath %s: %v", conf.ConfsPath, err)
		}
		paths = append(paths, v)
	}

	if err = m.Watch(paths); err != nil {
		log.Fatalf("watch path error %v", err)
	}
	utilsos.WaitForInterrupt(func() { m.Stop() })
}
