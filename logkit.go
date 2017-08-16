package main

import (
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"

	config "github.com/qiniu/logkit/conf"
	_ "github.com/qiniu/logkit/metric/all"
	"github.com/qiniu/logkit/mgr"
	"github.com/qiniu/logkit/times"
	"github.com/qiniu/logkit/utils"

	"net/http"
	_ "net/http/pprof"

	"github.com/labstack/echo"
	"github.com/qiniu/log"
)

//Config of logkit
type Config struct {
	MaxProcs         int      `json:"max_procs"`
	DebugLevel       int      `json:"debug_level"`
	ProfileHost      string   `json:"profile_host"`
	ConfsPath        []string `json:"confs_path"`
	CleanSelfLog     bool     `json:"clean_self_log"`
	CleanSelfDir     string   `json:"clean_self_dir"`
	CleanSelfPattern string   `json:"clean_self_pattern"`
	TimeLayouts      []string `json:"timeformat_layouts"`
	CleanSelfLogCnt  int      `json:"clean_self_cnt"`
	StaticRootPath   string   `json:"static_root_path"`
	mgr.ManagerConfig
}

var conf Config

const (
	Version           = "v1.2.3"
	defaultReserveCnt = 5
	defaultLogDir     = "./run"
	defaultLogPattern = "*.log-*"
)

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
	log.Printf("Welcome to use Logkit, Version: %v \n\nConfig: %#v", Version, conf)
	if conf.TimeLayouts != nil {
		times.AddLayout(conf.TimeLayouts)
	}
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
		log.Warnf("Cannot read or create any ConfsPath %v", conf.ConfsPath)
	}
	if err = m.Watch(paths); err != nil {
		log.Fatalf("watch path error %v", err)
	}
	m.RestoreWebDir()

	stopClean := make(chan struct{}, 0)
	defer close(stopClean)
	if conf.CleanSelfLog {
		go loopCleanLogkitLog(conf.CleanSelfDir, conf.CleanSelfPattern, conf.CleanSelfLogCnt, stopClean)
	}
	if len(conf.BindHost) > 0 {
		m.BindHost = conf.BindHost
	}
	e := echo.New()
	e.Static("/", conf.StaticRootPath)

	// start rest service
	rs := mgr.NewRestService(m, e)
	if conf.ProfileHost != "" {
		log.Printf("profile_host was open at %v", conf.ProfileHost)
		go func() {
			log.Println(http.ListenAndServe(conf.ProfileHost, nil))
		}()
	}
	utils.WaitForInterrupt(func() {
		rs.Stop()
		if conf.CleanSelfLog {
			stopClean <- struct{}{}
		}
		m.Stop()
	})
}
