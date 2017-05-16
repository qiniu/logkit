package cleaner

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/qiniu/log"

	"github.com/qiniu/logkit/conf"
	"github.com/qiniu/logkit/reader"
	"github.com/qiniu/logkit/utils"
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
}

const (
	clean_enable      = "delete_enable"
	clean_interval    = "delete_interval"
	clean_name        = "cleaner_name"
	reservefileNumber = "reserve_file_number"
	reservefileSize   = "reserve_file_size"

	default_delete_interval     = 300  //5分钟
	default_reserve_file_number = 10   //默认保存是个文件
	default_reserve_file_size   = 2048 //单位MB，默认删除保存2G
	MB                          = 1024 * 1024
	// 如果两项任意一项达到要求，就执行删除；如果两项容易一项有值设置，但是另一项为0，就认为另一项不做限制
)

// 删除文件时遍历全部
// 删除时生成filedeleted文件
func NewCleaner(conf conf.MapConf, meta *reader.Meta, cleanChan chan<- CleanSignal, logdir string) (c *Cleaner, err error) {
	enable, _ := conf.GetBoolOr(clean_enable, false)
	if !enable {
		return
	}
	interval, _ := conf.GetIntOr(clean_interval, 0) //单位，秒
	if interval <= 0 {
		interval = default_delete_interval
	}
	name, _ := conf.GetStringOr(clean_name, "unknow")
	reserveNumber, _ := conf.GetInt64Or(reservefileNumber, 0)
	reserveSize, _ := conf.GetInt64Or(reservefileSize, 0)
	if reserveNumber <= 0 && reserveSize <= 0 {
		reserveNumber = default_reserve_file_number
		reserveSize = default_reserve_file_size
	}
	reserveSize = reserveSize * MB
	logdir, _, err = utils.GetRealPath(logdir)
	if err != nil {
		err = fmt.Errorf("GetRealPath for %v error %v", logdir, err)
		return
	}
	c = &Cleaner{
		cleanTicker:   time.NewTicker(time.Duration(interval) * time.Second).C,
		reserveNumber: reserveNumber,
		reserveSize:   reserveSize,
		meta:          meta,
		exitChan:      make(chan struct{}),
		cleanChan:     cleanChan,
		name:          name,
		logdir:        logdir,
	}
	return
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

func (c *Cleaner) shoudClean(size, count int64) bool {
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
	dir, _, err := utils.GetRealPath(dir)
	if err != nil {
		log.Errorf("GetRealPath for %v error %v", path, err)
		return false
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
		logFiles := utils.GetLogFiles(f.Path)
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
			if beginClean || c.shoudClean(size, count) {
				beginClean = true
				c.cleanChan <- CleanSignal{
					Logdir:   filepath.Dir(logf.Path),
					Filename: logf.Info.Name(),
					Cleaner:  c.name,
				}
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
