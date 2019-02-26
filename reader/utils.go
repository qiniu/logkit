package reader

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/json-iterator/go"

	"github.com/qiniu/log"
	"github.com/qiniu/logkit/reader/config"
	"github.com/qiniu/logkit/utils/models"
)

var ErrStopped = errors.New("runner stopped")
var ErrNoFileChosen = errors.New("no files found")
var ErrMetaFileRead = errors.New("cannot read meta file")
var ErrFileNotRegular = errors.New("file is not regular")
var ErrFileNotDir = errors.New("file is not directory")

var WaitNoSuchFile = 100 * time.Millisecond

// getLatestFile 获得当前文件夹下最新的文件
func getLatestFile(logdir string) (os.FileInfo, error) {
	return GetMaxFile(logdir, NoCondition, models.ModTimeLater)
}

// getOldestFile 获得当前文件夹下最旧的文件
func getOldestFile(logdir string) (os.FileInfo, error) {
	return GetMinFile(logdir, NoCondition, models.ModTimeLater)
}

// GetMaxFile 在指定的限制条件condition下，根据比较函数gte 选择最大的os.FileInfo
// condition 文件必须满足的条件
// gte f1 >= f2 则返回true
func GetMaxFile(logdir string, condition func(os.FileInfo) bool, gte func(f1, f2 os.FileInfo) bool) (chosen os.FileInfo, err error) {
	files, err := ioutil.ReadDir(logdir)
	if err != nil {
		return nil, err
	}
	for _, fi := range files {
		if fi.IsDir() {
			continue
		}
		if condition == nil || !condition(fi) {
			continue
		}
		if chosen == nil || gte(fi, chosen) {
			chosen = fi
		}
	}
	if chosen == nil {
		return nil, os.ErrNotExist
	}
	return
}

// GetMinFile 于getMaxFile 相反，返回最小的文件
func GetMinFile(logdir string, condition func(os.FileInfo) bool, gte func(f1, f2 os.FileInfo) bool) (os.FileInfo, error) {
	return GetMaxFile(logdir, condition, func(f1, f2 os.FileInfo) bool {
		return !gte(f1, f2)
	})
}

// NoCondition 无限制条件
func NoCondition(f os.FileInfo) bool {
	return true
}

func AndCondition(f1, f2 func(os.FileInfo) bool) func(os.FileInfo) bool {
	return func(fi os.FileInfo) bool {
		return f1(fi) && f2(fi)
	}
}

func OrCondition(f1, f2 func(os.FileInfo) bool) func(os.FileInfo) bool {
	return func(fi os.FileInfo) bool {
		return f1(fi) || f2(fi)
	}
}

func NotCondition(f1 func(os.FileInfo) bool) func(os.FileInfo) bool {
	return func(fi os.FileInfo) bool {
		return !f1(fi)
	}
}

func HeadPatternMode(mode string, v interface{}) (reg *regexp.Regexp, err error) {
	switch mode {
	case config.ReadModeHeadPatternString:
		pattern, ok := v.(string)
		if !ok {
			err = fmt.Errorf(" %v is not pattern string", v)
			return
		}
		reg, err = regexp.Compile(pattern)
		if err != nil {
			err = fmt.Errorf("pattern %v compile error %v ", v, err)
			return
		}
		return
	case config.ReadModeHeadPatternRegexp:
		reg1, ok := v.(*regexp.Regexp)
		if !ok {
			err = fmt.Errorf(" %v is not *regexp.Regexp type value", v)
		}
		reg = reg1
		return
	default:
		err = fmt.Errorf("unknown HeadPatternMode %v", mode)
		return
	}
}

func ParseLoopDuration(cronSched string) (dur time.Duration, err error) {
	cronSched = strings.TrimSpace(strings.TrimPrefix(cronSched, config.Loop))
	dur, err = time.ParseDuration(cronSched)
	if err != nil {
		dur = time.Duration(0)
		err = fmt.Errorf("parse Cron loop duration %v error %v, make duration as 1 second", cronSched, err)
	}
	return
}

func getTags(tagFile string) (tags map[string]interface{}, err error) {
	tags = make(map[string]interface{})
	if tagFile == "" {
		return
	}
	tagsData, err := ioutil.ReadFile(tagFile)
	if tagsData == nil || err != nil {
		return
	}
	if jerr := jsoniter.Unmarshal(tagsData, &tags); jerr != nil {
		return
	}
	return
}

func CompressedFile(path string) bool {
	if strings.HasSuffix(path, ".gz") || strings.HasSuffix(path, ".tar") || strings.HasSuffix(path, ".zip") {
		return true
	}
	return false
}

// IgnoreFileSuffixes return true if file has suffix of one of the suffixes
func IgnoreFileSuffixes(file string, suffixes []string) bool {
	for _, s := range suffixes {
		if strings.HasSuffix(file, s) {
			return true
		}
	}
	return false
}

// ValidFileRegex return true if file matches with validFilePattern
func ValidFileRegex(file, validFilePattern string) bool {
	if validFilePattern == "" {
		return true
	}
	match, err := filepath.Match(validFilePattern, file)
	if err != nil {
		log.Debugf("Pattern %s is invalid to match file %s", validFilePattern, file)
		return false
	}
	return match
}

// IgnoreHidden return ture if file has dot(.) which presents ignore files in *nix system
func IgnoreHidden(file string, ignoreHidden bool) bool {
	if !ignoreHidden {
		return false
	}
	if strings.HasPrefix(filepath.Base(file), ".") {
		return true
	}
	return false
}
