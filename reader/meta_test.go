package reader

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/qiniu/logkit/conf"
	. "github.com/qiniu/logkit/reader/test"
	"github.com/qiniu/logkit/utils"
	. "github.com/qiniu/logkit/utils/models"
)

func createFile(interval int) {
	CreateDir()
	CreateFiles(interval)
}

func TestMeta(t *testing.T) {
	createFile(50)
	defer DestroyDir()
	logkitConf := conf.MapConf{
		KeyMetaPath: MetaDir,
		KeyFileDone: MetaDir,
		KeyLogPath:  Dir,
		KeyMode:     ModeDir,
	}
	meta, err := NewMetaWithConf(logkitConf)
	if err != nil {
		t.Error(err)
	}
	os.RemoveAll(MetaDir)
	// no metaDir conf except work
	confNoMetaPath := conf.MapConf{
		KeyLogPath:    Dir,
		GlobalKeyName: "mock_runner_name",
		KeyMode:       ModeDir,
	}
	meta, err = NewMetaWithConf(confNoMetaPath)
	if err != nil {
		t.Error(err)
	}
	f, err := os.Stat("meta/" + "mock_runner_name_" + Hash(Dir))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(f.Name(), Hash(Dir)) {
		t.Fatal("not excepted dir")
	}
	dirToRm := "meta"
	os.RemoveAll(dirToRm)

	meta, err = NewMeta(MetaDir, MetaDir, ModeDir, "logpath", "", 7)
	if err != nil {
		t.Error(err)
	}
	defer os.RemoveAll(MetaDir)

	file, offset, err := meta.ReadOffset()
	if err == nil {
		t.Error("offset must be nil")
	}
	err = meta.WriteOffset(filepath.Join(Dir, "f1"), 2)
	if err != nil {
		t.Error(err)
	}
	file, offset, err = meta.ReadOffset()
	if file != filepath.Join(Dir, "f1") {
		t.Error("file should be " + filepath.Join(Dir, "f1"))
	}
	if offset != 2 {
		t.Error("file offset should be 2")
	}
	err = meta.AppendDoneFile("f1")
	if err != nil {
		t.Error(err)
	}
	if meta.IsDoneFile("file.done.12212") != true {
		t.Error("test is done file error,expect file.done.12212 is done file")
	}
	if meta.IsDoneFile("filedone.11111") != false {
		t.Error("test is done file error,expect filedone.11111 is done file")
	}
	donefile := filepath.Join(MetaDir, DoneFileName+".2015-11-11")
	_, err = os.Create(donefile)
	if err != nil {
		t.Error(err)
	}
	if err = meta.DeleteDoneFile(donefile); err != nil {
		t.Error(err)
	}
	_, err = os.Stat(filepath.Join(MetaDir, deletedFileName+".2015-11-11"))
	if err == nil || !os.IsNotExist(err) {
		t.Error("not deleted", err)
	}
	y, m, d := time.Now().Date()
	donefile = filepath.Join(MetaDir, DoneFileName+fmt.Sprintf(".%d-%d-%d", y, m, d))
	if err = meta.DeleteDoneFile(donefile); err != nil {
		t.Error(err)
	}
	_, err = os.Stat(donefile)
	if err != nil {
		t.Errorf("%v shoud not deleted %v", donefile, err)
	}
	stat := &Statistic{
		ReaderCnt: 6,
		ParserCnt: [2]int64{6, 8},
		SenderCnt: map[string][2]int64{
			"aaa": {1, 2},
			"bbb": {5, 6},
		},
		TransformErrors: make(map[string]ErrorQueue),
		SendErrors:      make(map[string]ErrorQueue),
	}
	err = meta.WriteStatistic(stat)
	assert.NoError(t, err)
	newStat, err := meta.ReadStatistic()
	assert.NoError(t, err)
	assert.Equal(t, *stat, newStat)
}

func Test_getdonefiles(t *testing.T) {
	donefiles := "Test_getdonefiles"
	err := os.Mkdir(donefiles, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(donefiles)
	_, err = os.Create(filepath.Join(donefiles, "file.done.2016-10-01"))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	_, err = os.Create(filepath.Join(donefiles, "file.done.2016-10-02"))
	if err != nil {
		t.Fatal(err)
	}
	_, err = os.Create(filepath.Join(donefiles, "file.deleted.2016-10-02"))
	if err != nil {
		t.Fatal(err)
	}
	err = os.Mkdir(filepath.Join(donefiles, "file.done.2016-10-03"), os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	c := conf.MapConf{}
	c[KeyMetaPath] = donefiles
	c[KeyLogPath] = "logpath"
	c[KeyMode] = ModeDir
	c[KeyDataSourceTag] = "tag1path"
	meta, err := NewMetaWithConf(c)
	if err != nil {
		t.Error(err)
	}

	files, err := meta.GetDoneFiles()
	if err != nil {
		t.Error(err)
	}
	exps := []string{"file.done.2016-10-02", "file.done.2016-10-01"}
	var gots []string
	for _, fi := range files {
		gots = append(gots, fi.Info.Name())
	}
	if !reflect.DeepEqual(exps, gots) {
		t.Errorf("Test_getdonefiles error got %v exp %v", gots, exps)
	}
	if meta.GetDataSourceTag() != "tag1path" {
		t.Error("GetDataSourceTag error")
	}
	if meta.GetMode() != ModeDir {
		t.Error("get mode error")
	}
}

func TestExtraInfo(t *testing.T) {
	meta, err := NewMetaWithConf(conf.MapConf{
		ExtraInfo: "true",
		KeyMode:   ModeMySQL,
	})
	assert.NoError(t, err)
	got := meta.ExtraInfo()
	assert.Equal(t, len(got), 4)
	meta, err = NewMetaWithConf(conf.MapConf{
		KeyMode: ModeMySQL,
	})
	assert.NoError(t, err)
	got = meta.ExtraInfo()
	assert.NotNil(t, got)
	assert.Equal(t, len(got), 0)
}

func Test_GetLogFiles2(t *testing.T) {
	logfiles := "Test_GetLogFiles2"
	os.RemoveAll(logfiles)
	err := os.Mkdir(logfiles, os.ModePerm)
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(logfiles)

	log1 := logfiles + "/log1"
	log2 := logfiles + "/log2"
	log3 := logfiles + "/log3"
	_, err = os.Create(log1)
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
	_, err = os.Create(log2)
	if err != nil {
		t.Fatal(err)
	}

	logkitConf := conf.MapConf{
		"log_path":  logfiles,
		"meta_path": logfiles,
		KeyFileDone: filepath.Join(logfiles, "meta"),
		"mode":      "dir",
	}
	meta, err := NewMetaWithConf(logkitConf)
	if err != nil {
		t.Error(err)
	}
	meta.AppendDoneFileInode(log1, 123)
	meta.AppendDoneFileInode(log2, 234)
	meta.AppendDoneFileInode(log3, 456)

	files := GetLogFiles(meta.DoneFile())
	exps := []string{"log2", "log1"}
	var gots []string
	for _, f := range files {
		gots = append(gots, f.Info.Name())
	}
	if !reflect.DeepEqual(exps, gots) {
		t.Errorf("Test_getLogFiles error exp %v but got %v", exps, gots)
	}
	err = os.RemoveAll(logfiles)
	if err != nil {
		t.Error(err)
	}
}

func TestMeta_CleanExpiredSubMetas(t *testing.T) {
	metaDir := "TestMeta_CleanExpiredSubMetas"
	assert.NoError(t, os.Mkdir(metaDir, os.ModePerm))
	defer os.RemoveAll(metaDir)

	subMeta1Dir := filepath.Join(metaDir, "submeta1")
	assert.NoError(t, os.Mkdir(subMeta1Dir, os.ModePerm))
	subMeta1File := filepath.Join(subMeta1Dir, metaFileName)
	assert.NoError(t, ioutil.WriteFile(subMeta1File, []byte("submeta1"), 0644))

	subMeta2Dir := filepath.Join(metaDir, "submeta2")
	assert.NoError(t, os.Mkdir(subMeta2Dir, os.ModePerm))
	subMeta2File := filepath.Join(subMeta2Dir, metaFileName)
	assert.NoError(t, ioutil.WriteFile(subMeta2File, []byte("submeta2"), 0644))

	expired := time.Now().Add(-25 * time.Hour)
	assert.NoError(t, os.Chtimes(subMeta2File, expired, expired))

	m := &Meta{
		Dir:            metaDir,
		subMetaExpired: make(map[string]bool),
	}
	m.CheckExpiredSubMetas(time.Nanosecond)
	m.CleanExpiredSubMetas(time.Nanosecond)

	assert.True(t, utils.IsExist(subMeta1File))
	assert.False(t, utils.IsExist(subMeta2File))

	// 测试 submeta 清理缓存，subMeta1File 虽然过期但此刻不会被清理
	assert.NoError(t, os.Chtimes(subMeta1File, expired, expired))
	m.CleanExpiredSubMetas(time.Nanosecond)
	assert.True(t, utils.IsExist(subMeta1File))
}
