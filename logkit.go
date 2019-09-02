package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"time"

	"github.com/labstack/echo"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/cli"
	config "github.com/qiniu/logkit/conf"
	_ "github.com/qiniu/logkit/metric/builtin"
	"github.com/qiniu/logkit/mgr"
	"github.com/qiniu/logkit/times"
	_ "github.com/qiniu/logkit/transforms/builtin"
	. "github.com/qiniu/logkit/utils/models"
	utilsos "github.com/qiniu/logkit/utils/os"
)

//Config of logkit
type Config struct {
	MaxProcs          int      `json:"max_procs"`
	DebugLevel        int      `json:"debug_level"`
	ProfileHost       string   `json:"profile_host"`
	ConfsPath         []string `json:"confs_path"`
	LogPath           string   `json:"log"`
	CleanSelfLog      bool     `json:"clean_self_log"`
	CleanSelfDir      string   `json:"clean_self_dir"`
	CleanSelfPattern  string   `json:"clean_self_pattern"`
	CleanSelfDuration string   `json:"clean_self_duration"`
	CleanSelfLogCnt   int      `json:"clean_self_cnt"`
	TimeLayouts       []string `json:"timeformat_layouts"`
	StaticRootPath    string   `json:"static_root_path"`
	mgr.ManagerConfig
}

var conf Config

const (
	NextVersion        = "v1.5.5"
	defaultReserveCnt  = 5
	defaultLogDir      = "./"
	defaultLogPattern  = "*.log-*"
	defaultLogDuration = 10 * time.Minute
	defaultRotateSize  = 100 * 1024 * 1024
)

const usage = `logkit, Very easy-to-use server agent for collecting & sending logs & metrics.

Usage:

  logkit [commands|flags]

The commands & flags are:

  -v                 print the version to stdout.
  -h                 print logkit usage info to stdout.
  -upgrade           check and upgrade version.

  -f <file>          configuration file to load

Examples:

  # start logkit
  logkit -f logkit.conf

  # check version
  logkit -v

  # checking and upgrade version
  logkit -upgrade
`

var (
	fversion = flag.Bool("v", false, "print the version to stdout")
	upgrade  = flag.Bool("upgrade", false, "check and upgrade version")
	confName = flag.String("f", "logkit.conf", "configuration file to load")
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

type MatchFile struct {
	Name    string
	ModTime time.Time
}

type MatchFiles []MatchFile

func (f MatchFiles) Len() int           { return len(f) }
func (f MatchFiles) Swap(i, j int)      { f[i], f[j] = f[j], f[i] }
func (f MatchFiles) Less(i, j int) bool { return f[i].ModTime.Before(f[j].ModTime) }

func cleanLogkitLog(dir, pattern string, reserveCnt int) {
	var err error
	path := filepath.Join(dir, pattern)
	matches, err := filepath.Glob(path)
	if err != nil {
		log.Errorf("filepath.Glob path %v error %v", path, err)
		return
	}
	var files MatchFiles
	for _, name := range matches {
		info, err := os.Stat(name)
		if err != nil {
			log.Errorf("os.Stat name %v error %v", name, err)
			continue
		}
		files = append(files, MatchFile{
			Name:    name,
			ModTime: info.ModTime(),
		})
	}
	if len(files) <= reserveCnt {
		return
	}
	sort.Sort(files)
	for _, f := range files[0 : len(files)-reserveCnt] {
		err := os.Remove(f.Name)
		if err != nil {
			log.Errorf("Remove %s failed , error: %v", f, err)
			continue
		}
	}
	return
}

func loopCleanLogkitLog(dir, pattern string, reserveCnt int, duration string, exitchan chan struct{}) {
	if len(dir) <= 0 {
		dir = defaultLogDir
	}
	if len(pattern) <= 0 {
		pattern = defaultLogPattern
	}
	if reserveCnt <= 0 {
		reserveCnt = defaultReserveCnt
	}
	var (
		dur time.Duration
		err error
	)
	if duration != "" {
		dur, err = time.ParseDuration(duration)
		if err != nil {
			log.Warnf("clean self duration parse failed: %v, Valid time units are 'ns', 'us' (or 'µs'), 'ms', 's', 'm', 'h'. Use default duration: 10m", err)
			dur = defaultLogDuration
		}
	} else {
		dur = defaultLogDuration
	}
	ticker := time.NewTicker(dur)
	defer ticker.Stop()
	for {
		select {
		case <-exitchan:
			return
		case <-ticker.C:
			cleanLogkitLog(dir, pattern, reserveCnt)
		}
	}
}

func rotateLog(path string) (file *os.File, err error) {
	newfile := path + "-" + time.Now().Format("0102030405")
	file, err = os.OpenFile(newfile, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0666)
	if err != nil {
		err = fmt.Errorf("rotateLog open newfile %v err %v", newfile, err)
		return
	}
	log.SetOutput(file)
	return
}

func loopRotateLogs(path string, rotateSize int64, dur time.Duration, exitchan chan struct{}) {
	file, err := rotateLog(path)
	if err != nil {
		log.Fatal(err)
	}
	ticker := time.NewTicker(dur)
	defer ticker.Stop()
	for {
		select {
		case <-exitchan:
			return
		case <-ticker.C:
			info, err := file.Stat()
			if err != nil {
				log.Warnf("stat log error %v", err)
			} else {
				if info.Size() >= rotateSize {
					newfile, err := rotateLog(path)
					if err != nil {
						log.Errorf("rotate log %v error %v, use old log to write logkit log", path, err)
					} else {
						file.Close()
						file = newfile
					}
				}
			}

		}
	}
}

func usageExit(rc int) {
	fmt.Println(usage)
	os.Exit(rc)
}

//！！！注意： 自动生成 grok pattern代码，下述注释请勿删除！！！
//go:generate go run tools/generators/grok_pattern_generator.go
func main() {
	flag.Usage = func() { usageExit(0) }
	flag.Parse()
	switch {
	case *fversion:
		fmt.Println("logkit version: ", NextVersion)
		osInfo := utilsos.GetOSInfo()
		fmt.Println("Hostname: ", osInfo.Hostname)
		fmt.Println("Core: ", osInfo.Core)
		fmt.Println("OS: ", osInfo.OS)
		fmt.Println("Platform: ", osInfo.Platform)
		return
	case *upgrade:
		cli.CheckAndUpgrade(NextVersion)
		return
	}

	if err := config.LoadEx(&conf, *confName); err != nil {
		log.Fatal("config.Load failed:", err)
	}
	if conf.TimeLayouts != nil {
		times.AddLayout(conf.TimeLayouts)
	}
	if conf.MaxProcs <= 0 {
		conf.MaxProcs = NumCPU
	}
	MaxProcs = conf.MaxProcs
	runtime.GOMAXPROCS(conf.MaxProcs)
	log.SetOutputLevel(conf.DebugLevel)

	var (
		stopRotate         = make(chan struct{}, 0)
		logdir, logpattern string
		err                error
	)
	defer close(stopRotate)
	if conf.LogPath != "" {
		logdir, logpattern, err = LogDirAndPattern(conf.LogPath)
		if err != nil {
			log.Fatal(err)
		}
		go loopRotateLogs(filepath.Join(logdir, logpattern), defaultRotateSize, 10*time.Second, stopRotate)
		conf.CleanSelfPattern = logpattern + "-*"
		conf.CleanSelfDir = logdir
		conf.ManagerConfig.CollectLogPath = filepath.Join(logdir, logpattern+"-*")
	}

	log.Infof("Welcome to use Logkit, Version: %v \n\nConfig: %#v", NextVersion, conf)
	m, err := mgr.NewManager(conf.ManagerConfig)
	if err != nil {
		log.Fatalf("NewManager: %v", err)
	}
	m.Version = NextVersion

	if m.CollectLogRunner != nil {
		go m.CollectLogRunner.Run()
		time.Sleep(time.Second) // 等待1秒让收集器启动
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
		if conf.CleanSelfDir == "" && logdir != "" {
			conf.CleanSelfDir = logdir
		}
		if conf.CleanSelfPattern == "" && logpattern != "" {
			conf.CleanSelfPattern = logpattern + "-*"
		}
		go loopCleanLogkitLog(conf.CleanSelfDir, conf.CleanSelfPattern, conf.CleanSelfLogCnt, conf.CleanSelfDuration, stopClean)
	}
	if len(conf.BindHost) > 0 {
		m.BindHost = conf.BindHost
	}
	e := echo.New()
	e.Static("/", conf.StaticRootPath)

	// start rest service
	rs := mgr.NewRestService(m, e)
	if conf.ProfileHost != "" {
		log.Infof("go profile_host was open at %v", conf.ProfileHost)
		go func() {
			log.Fatal(http.ListenAndServe(conf.ProfileHost, nil))
		}()
	}
	if err = rs.Register(); err != nil {
		log.Fatalf("register master error %v", err)
	}
	utilsos.WaitForInterrupt(func() {
		rs.Stop()
		if conf.CleanSelfLog {
			stopClean <- struct{}{}
		}
		m.Stop()
	})
}
