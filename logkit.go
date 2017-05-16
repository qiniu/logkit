package main

import (
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"

	config "github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/mgr"
	"github.com/qiniu/logkit/utils"

	_ "net/http/pprof"

	"github.com/qiniu/log"
)

//Config of logkit
type Config struct {
	MaxProcs         int      `json:"max_procs"`
	DebugLevel       int      `json:"debug_level"`
	ConfsPath        []string `json:"confs_path"`
	CleanSelfLog     bool     `json:"clean_self_log"`
	CleanSelfDir     string   `json:"clean_self_dir"`
	CleanSelfPattern string   `json:"clean_self_pattern"`
	CleanSelfLogCnt  int      `json:"clean_self_cnt"`
	mgr.ManagerConfig
}

var conf Config

const defaultReserveCnt = 5
const defaultLogDir = "./run"
const defaultLogPattern = "*.log-*"

func getValidPath(confPaths []string) (paths []string) {
	paths = make([]string, 0)
	exits := make(map[string]bool)
	for _, v := range confPaths {
		rp, err := filepath.Abs(v)
		if err != nil {
			log.Errorf("Get real path of ConfsPath %v error %v, ignore it", v, rp)
			continue
		}
		if _, ok := exits[rp]; ok {
			log.Errorf("ConfsPath %v duplicated, ignore", rp)
			continue
		}
		exits[rp] = true
		paths = append(paths, rp)
	}
	return
}

func cleanLogkitLog(dir, pattern string, reserveCnt int) {
	var err error
	path := filepath.Join(dir, pattern)
	matches, err := filepath.Glob(path)
	if err != nil {
		log.Errorf("filepath.Glob path %v error %v", path, err)
		return
	}
	if len(matches) <= reserveCnt {
		return
	}
	sort.Strings(matches)
	for _, f := range matches[0 : len(matches)-reserveCnt] {
		err := os.Remove(f)
		if err != nil {
			log.Errorf("Remove %s failed , error: %v", f, err)
			continue
		}
	}
	return
}

func loopCleanLogkitLog(dir, pattern string, reserveCnt int, exitchan chan struct{}) {
	if len(dir) <= 0 {
		dir = defaultLogDir
	}
	if len(pattern) <= 0 {
		pattern = defaultLogPattern
	}
	if reserveCnt <= 0 {
		reserveCnt = defaultReserveCnt
	}
	ticker := time.NewTicker(time.Minute * 10)
	for {
		select {
		case <-exitchan:
			return
		case <-ticker.C:
			cleanLogkitLog(dir, pattern, reserveCnt)
		}
	}
}

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

	m, err := mgr.NewManager(conf.ManagerConfig)
	if err != nil {
		log.Fatalf("NewManager: %v", err)
	}
	paths := getValidPath(conf.ConfsPath)
	if len(paths) <= 0 {
		log.Fatalf("Cannot read or create any ConfsPath %v", conf.ConfsPath)
	}
	if err = m.Watch(paths); err != nil {
		log.Fatalf("watch path error %v", err)
	}
	stopClean := make(chan struct{}, 0)
	if conf.CleanSelfLog {
		go loopCleanLogkitLog(conf.CleanSelfDir, conf.CleanSelfPattern, conf.CleanSelfLogCnt, stopClean)
	}

	// start rest service
	rs := mgr.NewRestService(m)

	utils.WaitForInterrupt(func() {
		rs.Stop()
		stopClean <- struct{}{}
		m.Stop()
	})
}
