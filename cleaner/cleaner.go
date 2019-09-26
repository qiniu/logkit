package cleaner

import (
	"path/filepath"
	"time"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	"github.com/qiniu/logkit/reader/config"
	. "github.com/qiniu/logkit/utils/models"
)

type Cleaner struct {
	cleanTicker   <-chan time.Time
	reserveNumber int64 //个
	reserveSize   int64 //byte
	meta          *reader.Meta
	exitChan      chan struct{}
	cleanChan     chan<- CleanSignal
	name          string
	logdir        string
}

type CleanSignal struct {
	Logdir   string
	Filename string
	Cleaner  string
	ReadMode string
}

const (
	KeyCleanEnable       = "delete_enable"
	KeyCleanInterval     = "delete_interval"
	KeyReserveFileNumber = "reserve_file_number"
	KeyReserveFileSize   = "reserve_file_size"
	cleanerName          = "cleaner_name"

	defaultDeleteInterval    = 300  //5分钟
	defaultReserveFileNumber = 10   //默认保存是个文件
	defaultReserveFileSize   = 2048 //单位MB，默认删除保存2G
	// 如果两项任意一项达到要求，就执行删除；如果两项容易一项有值设置，但是另一项为0，就认为另一项不做限制
)

// 删除文件时遍历全部
// 删除时生成filedeleted文件
func NewCleaner(conf conf.MapConf, meta *reader.Meta, cleanChan chan<- CleanSignal, logdir string) (*Cleaner, error) {
	enable, _ := conf.GetBoolOr(KeyCleanEnable, false)
	if !enable {
		return nil, nil
	}
	mode := meta.GetMode()
	if mode != config.ModeDir &&
		mode != config.ModeFile &&
		mode != config.ModeCloudTrail &&
		mode != config.ModeCloudTrailV2 &&
		mode != config.ModeTailx &&
		mode != config.ModeDirx {
		log.Errorf("Cleaner only supports reader mode dir|file|cloudtrail|tailx|dirx, current mode is %v, cleaner disabled", meta.GetMode())
		return nil, nil
	}
	interval, _ := conf.GetIntOr(KeyCleanInterval, 0) //单位，秒
	if interval <= 0 {
		interval = defaultDeleteInterval
	}
	name, _ := conf.GetStringOr(cleanerName, "unknown")
	reserveNumber, _ := conf.GetInt64Or(KeyReserveFileNumber, 0)
	reserveSize, _ := conf.GetInt64Or(KeyReserveFileSize, 0)
	if reserveNumber <= 0 && reserveSize <= 0 {
		reserveNumber = defaultReserveFileNumber
		reserveSize = defaultReserveFileSize
	}
	reserveSize = reserveSize * MB
	if mode != config.ModeTailx && mode != config.ModeDirx {
		var err error
		logdir, _, err = GetRealPath(logdir)
		if err != nil {
			log.Errorf("Failed to get real path of %q: %v", logdir, err)
			return nil, err
		}
	}
	return &Cleaner{
		cleanTicker:   time.NewTicker(time.Duration(interval) * time.Second).C,
		reserveNumber: reserveNumber,
		reserveSize:   reserveSize,
		meta:          meta,
		exitChan:      make(chan struct{}),
		cleanChan:     cleanChan,
		name:          name,
		logdir:        logdir,
	}, nil
}

func (c *Cleaner) Run() {
	for {
		select {
		case <-c.exitChan:
			log.Warnf("%v receive exit signal, cleaner exiting...", c.name)
			return
		case <-c.cleanTicker:
		}
		err := c.Clean()
		if err != nil {
			log.Error(err)
		}
	}
}

func (c *Cleaner) Close() {
	c.exitChan <- struct{}{}
}

func (c *Cleaner) Name() string {
	return c.name
}

func (c *Cleaner) shouldClean(size, count int64) bool {
	if c.reserveNumber > 0 && count > c.reserveNumber {
		return true
	}
	if c.reserveSize > 0 && size > c.reserveSize {
		return true
	}
	return false
}

func (c *Cleaner) checkBelong(path string) bool {
	dir := filepath.Dir(path)
	dir, _, err := GetRealPath(dir)
	if err != nil {
		log.Errorf("GetRealPath for %v error %v", path, err)
		return false
	}

	switch c.meta.GetMode() {
	case config.ModeTailx:
		matched, err := filepath.Match(filepath.Dir(c.logdir), filepath.Dir(path))
		if err != nil {
			log.Errorf("Failed to check if %q belongs to %q: %v", path, c.logdir, err)
			return false
		}
		return matched

	case config.ModeDirx:
		matched, err := filepath.Match(c.logdir, filepath.Dir(path))
		if err != nil {
			log.Errorf("Failed to check if %q belongs to %q: %v", path, c.logdir, err)
			return false
		}
		return matched
	}

	if dir != c.logdir {
		return false
	}
	return true
}

func (c *Cleaner) Clean() (err error) {
	var size int64 = 0
	var count int64 = 0
	beginClean := false
	doneFiles, err := c.meta.GetDoneFiles()
	if err != nil {
		return err
	}
	checked := make(map[string]struct{})
	for _, f := range doneFiles {
		logFiles := GetLogFiles(f.Path)
		allremoved := true
		for _, logf := range logFiles {
			if !c.checkBelong(logf.Path) {
				continue
			}
			if _, ok := checked[logf.Path]; ok {
				continue
			}
			checked[logf.Path] = struct{}{}
			size += logf.Info.Size()
			count++
			// 一旦符合条件，更老的文件必然都要删除
			if beginClean || c.shouldClean(size, count) {
				beginClean = true
				sig := CleanSignal{
					Logdir:   filepath.Dir(logf.Path),
					Filename: logf.Info.Name(),
					Cleaner:  c.name,
					ReadMode: c.meta.GetMode(),
				}
				log.Infof("send clean signal %v", sig)
				c.cleanChan <- sig
				if err = c.meta.AppendDeleteFile(logf.Path); err != nil {
					log.Error(err)
				}
			} else {
				allremoved = false
			}
		}
		if allremoved {
			if err = c.meta.DeleteDoneFile(f.Path); err != nil {
				log.Error(err)
			}
		}
	}
	return nil
}

func (c *Cleaner) LogDir() string {
	return c.logdir
}
